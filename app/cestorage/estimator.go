package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/alphadose/haxmap"
	"github.com/axiomhq/hyperloglog"

	"github.com/makasim/cestimator/app/cestorage/protoparser"
)

type groupsMap = haxmap.Map[string, *sketch]

func newGroupsMap() *groupsMap {
	return haxmap.New[string, *sketch]()
}

// Estimator tracks cardinality for a single stream configuration.
type estimator struct {
	groupBy      []string          // label names to group by; empty means no grouping
	extraLabels  map[string]string // extra labels added to output metrics
	interval     time.Duration
	metricPrefix string
	bucketsNum   int
	precision    uint8
	sparse       bool

	sketch     atomic.Pointer[sketch]
	prevSketch atomic.Pointer[sketch]
	groups     atomic.Pointer[groupsMap]
	prevGroups atomic.Pointer[groupsMap]

	stopCh chan struct{}
}

func (e *estimator) String() string {
	return fmt.Sprintf(
		"interval: %s; group_by: %v; extra_labels: %v", e.interval, e.groupBy, e.extraLabels)
}

func newEstimator(cfg EstimatorConfig) (*estimator, error) {
	if cfg.Interval == 0 {
		cfg.Interval = time.Minute * 5
	}

	bucketsNum := 1
	if len(cfg.Group) == 0 {
		bucketsNum = 10
	}

	metricPrefix := fmt.Sprintf("cardinality_estimate{interval=%q", cfg.Interval)
	if len(cfg.Labels) > 0 {
		keys := make([]string, 0, len(cfg.Labels))
		for k := range cfg.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			metricPrefix += fmt.Sprintf(",%s=%q", k, cfg.Labels[k])
		}
	}

	e := &estimator{
		groupBy:      cfg.Group,
		extraLabels:  cfg.Labels,
		interval:     cfg.Interval,
		bucketsNum:   bucketsNum,
		metricPrefix: metricPrefix,
		precision:    14,
		sparse:       true,

		stopCh: make(chan struct{}),
	}

	if len(cfg.Group) == 0 {
		sk, err := e.newSketch()
		if err != nil {
			return nil, fmt.Errorf("cannot create HLL sketch for stream %s: %w", e, err)
		}
		e.sketch.Store(sk)
	} else {
		e.groups.Store(newGroupsMap())
		e.prevGroups.Store(newGroupsMap())
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
	if len(e.groupBy) == 0 {
		sk, err := e.newSketch()
		if err != nil {
			return
		}
		currSK := e.sketch.Load()
		e.prevSketch.Store(currSK)
		e.sketch.Store(sk)
		return
	}
	currGroup := e.groups.Load()
	e.prevGroups.Store(currGroup)
	e.groups.Store(newGroupsMap())
}

// insert adds a time series to the estimator if it matches the configured filter.
func (e *estimator) insert(ts protoparser.TimeSerie) {
	if len(e.groupBy) == 0 {
		sk := e.sketch.Load()
		sk.insert(ts.Fingerprint)
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

	groups := e.groups.Load()

	groupKey := string(key)
	sk, _ := groups.Get(groupKey)
	if sk == nil {
		var err error
		sk, err = e.newSketch()
		if err != nil {
			panic(fmt.Sprintf("FATAL: cannot create HLL sketch for stream %s: %s", e, err))
		}
		groups.Set(groupKey, sk)
	}
	sk.insert(ts.Fingerprint)
}

// writeMetrics writes cardinality_estimate metrics to w in Prometheus text format.
func (e *estimator) writeMetrics(w io.Writer) {
	if len(e.groupBy) == 0 {
		card := e.estimateSketch(e.sketch.Load(), e.prevSketch.Load())
		fmt.Fprintf(w, "%s,group_by_keys=\"__no_value__\",group_by_values=\"__no_value__\"} %d\n", e.metricPrefix, card)
		return
	}

	groupByKey := strings.Join(e.groupBy, ",")

	groups := e.groups.Load()
	prevGroups := e.prevGroups.Load()

	// Collect all group keys from both current and previous intervals.
	keys := make(map[string]struct{}, groups.Len()+prevGroups.Len())
	for k := range groups.Keys() {
		keys[k] = struct{}{}
	}
	for k := range prevGroups.Keys() {
		keys[k] = struct{}{}
	}

	fmt.Fprintf(w, "%s,group_by_keys=\"__no_value__\",group_by_values=%q} %d\n", e.metricPrefix, groupByKey, len(keys))
	for groupByVal := range keys {
		sk, _ := groups.Get(groupByVal)
		prevSK, _ := prevGroups.Get(groupByVal)

		card := e.estimateSketch(sk, prevSK)
		fmt.Fprintf(w, "%s,group_by_keys=%q,group_by_values=%q} %d\n", e.metricPrefix, groupByKey, groupByVal, card)
	}
}

// estimateSketch returns the cardinality estimate for the union of cur and prev.
// If prev is nil (no rotation has happened yet, or no previous interval data),
// only cur is used.  This prevents an abrupt drop to zero right after rotation.
func (e *estimator) estimateSketch(cur, prev *sketch) uint64 {
	if cur == nil && prev == nil {
		return 0
	}
	if prev == nil {
		return cur.estimate()
	}
	if cur == nil {
		return prev.estimate()
	}
	// Merge into a temporary copy so the originals are not mutated.
	merged := cur.cloneHLL()
	if err := merged.Merge(prev.cloneHLL()); err != nil {
		// Merge only fails on precision mismatch; both sketches use precision 14.
		return cur.estimate()
	}
	return merged.Estimate()
}

func (e *estimator) newSketch() (*sketch, error) {
	return newSketch(e.bucketsNum, e.precision, e.sparse)
}

type sketch struct {
	bucketsNum uint64
	buckets    []sketchBucket
}

type sketchBucket struct {
	mu        sync.Mutex
	hllSketch hyperloglog.Sketch
}

func (skb *sketchBucket) insert(fp uint64) {
	skb.mu.Lock()
	defer skb.mu.Unlock()
	skb.hllSketch.InsertHash(fp)
}

func (skb *sketchBucket) estimate() uint64 {
	skb.mu.Lock()
	defer skb.mu.Unlock()
	return skb.hllSketch.Estimate()
}

func (skb *sketchBucket) cloneHLL() *hyperloglog.Sketch {
	skb.mu.Lock()
	defer skb.mu.Unlock()
	return skb.hllSketch.Clone()
}

func newSketch(bucketsNum int, precision uint8, sparse bool) (*sketch, error) {
	buckets := make([]sketchBucket, bucketsNum)

	for i := range buckets {
		hllSK, err := hyperloglog.NewSketch(precision, sparse)
		if err != nil {
			return nil, err
		}

		buckets[i] = sketchBucket{
			hllSketch: *hllSK,
		}
	}

	return &sketch{
		buckets:    buckets,
		bucketsNum: uint64(bucketsNum),
	}, nil
}

func (sk *sketch) insert(fp uint64) {
	skb := &sk.buckets[fp%sk.bucketsNum]
	skb.insert(fp)
}

func (sk *sketch) estimate() uint64 {
	if sk.bucketsNum == 1 {
		skb := &sk.buckets[0]
		return skb.estimate()
	}

	return sk.cloneHLL().Estimate()
}

func (sk *sketch) cloneHLL() *hyperloglog.Sketch {
	skb := &sk.buckets[0]
	resHLL := skb.cloneHLL()
	if sk.bucketsNum == 1 {
		return resHLL
	}

	for i := 1; i < len(sk.buckets); i++ {
		skbI := &sk.buckets[i]
		if err := resHLL.Merge(skbI.cloneHLL()); err != nil {
			panic(err)
		}
	}

	return resHLL
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
