package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/makasim/cestimator/app/cestorage/protoparser"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

// Estimator tracks cardinality for a single stream configuration.
type estimator struct {
	groupBy     []string          // label names to group by; empty means no grouping
	extraLabels map[string]string // extra labels added to output metrics
	interval    time.Duration

	metricPrefix string

	mu         sync.Mutex
	sketch     *hyperloglog.Sketch            // current-interval sketch; used when group == ""
	prevSketch *hyperloglog.Sketch            // previous-interval sketch for smooth rotation
	groups     map[string]*hyperloglog.Sketch // current-interval per-group sketches; used when group != ""
	prevGroups map[string]*hyperloglog.Sketch // previous-interval per-group sketches for smooth rotation

	stopCh chan struct{} // closed by Stop to terminate the rotation goroutine
}

func (e *estimator) String() string {
	return fmt.Sprintf(
		"interval: %s; group_by: %v; extra_labels: %v", e.interval, e.groupBy, e.extraLabels)
}

func newEstimator(cfg EstimatorConfig) (*estimator, error) {
	if cfg.Interval == 0 {
		cfg.Interval = time.Minute * 5
	}

	e := &estimator{
		groupBy:      cfg.Group,
		extraLabels:  cfg.Labels,
		interval:     cfg.Interval,
		metricPrefix: fmt.Sprintf("cardinality_estimate{interval=%q", cfg.Interval),
		stopCh:       make(chan struct{}),
	}

	if len(cfg.Group) == 0 {
		sk, err := hyperloglog.NewSketch(14, true)
		if err != nil {
			return nil, fmt.Errorf("cannot create HLL sketch for stream %s: %w", e, err)
		}
		e.sketch = sk
	} else {
		e.groups = make(map[string]*hyperloglog.Sketch)
	}

	go e.runRotation()

	return e, nil
}

// Stop stops the background rotation goroutine, if any.
func (e *estimator) Stop() {
	close(e.stopCh)
}

// runRotation resets the sketches on every tick until stopCh is closed.
func (e *estimator) runRotation() {
	t := time.NewTicker(e.interval / 2)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			e.rotate()
		case <-e.stopCh:
			return
		}
	}
}

// rotate promotes current sketches to previous and starts fresh current sketches.
// Estimates are computed as the union of previous and current (see estimateSketch /
// estimateGroup), so cardinality does not drop to zero immediately after rotation.
func (e *estimator) rotate() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.groupBy) == 0 {
		sk, err := hyperloglog.NewSketch(14, true)
		if err != nil {
			return
		}
		e.prevSketch = e.sketch
		e.sketch = sk
		return
	}
	e.prevGroups = e.groups
	e.groups = make(map[string]*hyperloglog.Sketch)
}

// insert adds a time series to the estimator if it matches the configured filter.
func (e *estimator) insert(ts protoparser.TimeSerie) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.groupBy) == 0 {
		e.sketch.InsertHash(ts.Fingerprint)
		return
	}

	var key []byte
	first := true
	var hasNonEmptyLabel bool
	for _, labelName := range e.groupBy {
		if !first {
			key = append(key, ',')
		}

		for _, l := range ts.GroupLabels {
			if l.Name == labelName {
				if l.Value != "" {
					hasNonEmptyLabel = true
				}

				key = append(key, l.Value...)
				break
			}
		}

		first = false
	}
	// time series does not contribute to this group
	if !hasNonEmptyLabel {
		return
	}

	groupKey := string(key)
	sk := e.groups[groupKey]
	if sk == nil {
		var err error
		sk, err = hyperloglog.NewSketch(14, true)
		if err != nil {
			panic(fmt.Sprintf("FATAL: cannot create HLL sketch for stream %s: %s", e, err))
		}
		e.groups[groupKey] = sk
	}
	sk.InsertHash(ts.Fingerprint)
}

// writeMetrics writes cardinality_estimate metrics to w in Prometheus text format.
func (e *estimator) writeMetrics(w io.Writer) {
	e.mu.Lock()
	defer e.mu.Unlock()

	metricPrefix := fmt.Sprintf("cardinality_estimate{interval=%q", e.interval)

	if len(e.extraLabels) > 0 {
		keys := make([]string, 0, len(e.extraLabels))
		for k := range e.extraLabels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			metricPrefix += fmt.Sprintf(",%s=%q", k, e.extraLabels[k])
		}
	}

	if len(e.groupBy) == 0 {
		card := e.estimateSketch(e.sketch, e.prevSketch)
		fmt.Fprintf(w, "%s,group_by_keys=\"__no_value__\",group_by_values=\"__no_value__\"} %d\n", metricPrefix, card)
		return
	}

	groupByKey := strings.Join(e.groupBy, ",")

	// Collect all group keys from both current and previous intervals.
	keys := make(map[string]struct{}, len(e.groups)+len(e.prevGroups))
	for k := range e.groups {
		keys[k] = struct{}{}
	}
	for k := range e.prevGroups {
		keys[k] = struct{}{}
	}

	fmt.Fprintf(w, "%s,group_by_keys=\"__no_value__\",group_by_values=%q} %d\n", metricPrefix, groupByKey, len(keys))
	for groupByVal := range keys {
		card := e.estimateSketch(e.groups[groupByVal], e.prevGroups[groupByVal])
		fmt.Fprintf(w, "%s,group_by_keys=%q,group_by_values=%q} %d\n", metricPrefix, groupByKey, groupByVal, card)
	}
}

// estimateSketch returns the cardinality estimate for the union of cur and prev.
// If prev is nil (no rotation has happened yet, or no previous interval data),
// only cur is used.  This prevents an abrupt drop to zero right after rotation.
func (e *estimator) estimateSketch(cur, prev *hyperloglog.Sketch) uint64 {
	if cur == nil && prev == nil {
		return 0
	}
	if prev == nil {
		return cur.Estimate()
	}
	if cur == nil {
		return prev.Estimate()
	}
	// Merge into a temporary copy so the originals are not mutated.
	merged := cur.Clone()
	if err := merged.Merge(prev); err != nil {
		// Merge only fails on precision mismatch; both sketches use precision 14.
		return cur.Estimate()
	}
	return merged.Estimate()
}

// fingerprintLabels returns a byte slice that uniquely identifies a label set.
// The Prometheus remote write protocol guarantees labels are sorted by name,
// so no additional sorting is needed.
func fingerprintLabels(labels []prompb.Label) []byte {
	var b []byte
	for _, l := range labels {
		b = append(b, l.Name...)
		b = append(b, 0x00)
		b = append(b, l.Value...)
		b = append(b, 0x00)
	}
	return b
}
