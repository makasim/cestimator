package cardinalitylimiter

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

type Config struct {
	Query    string
	Endpoint string
	Interval time.Duration
}

type Limiter struct {
	cfg    Config
	client *http.Client

	rules  atomic.Pointer[[][]prompb.Label]
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func New(cfg Config) *Limiter {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Minute
	}

	l := &Limiter{
		cfg:    cfg,
		client: &http.Client{Timeout: 30 * time.Second},
		stopCh: make(chan struct{}),
	}

	l.wg.Go(l.run)

	return l
}

func (l *Limiter) Stop() {
	close(l.stopCh)
	l.wg.Wait()
}

func (l *Limiter) Limit(tss, res []prompb.TimeSeries) []prompb.TimeSeries {
	rules := l.rules.Load()

	res = res[:0]
	if rules == nil || len(*rules) == 0 {
		return append(res, tss...)
	}

	for _, ts := range tss {
		if !matchesAnyRule(ts.Labels, *rules) {
			res = append(res, ts)
		}
	}
	return res
}

func (l *Limiter) run() {
	t := time.NewTicker(l.cfg.Interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if err := l.refresh(); err != nil {
				logger.Errorf("cardinalitylimiter: query refresh failed: %v; keeping previous rules", err)
			}
		case <-l.stopCh:
			return
		}
	}
}

func (l *Limiter) refresh() error {
	rules, err := queryRules(l.client, l.cfg.Endpoint, l.cfg.Query)
	if err != nil {
		return err
	}

	l.rules.Store(&rules)
	return nil
}

func matchesAnyRule(labels []prompb.Label, rules [][]prompb.Label) bool {
	for _, rule := range rules {
		if matchesRule(labels, rule) {
			return true
		}
	}
	return false
}

func matchesRule(labels, rule []prompb.Label) bool {
	for _, rl := range rule {
		found := false
		for _, l := range labels {
			if l.Name == rl.Name && l.Value == rl.Value {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

type promResponse struct {
	Status string   `json:"status"`
	Error  string   `json:"error,omitempty"`
	Data   promData `json:"data"`
}

type promData struct {
	ResultType string       `json:"resultType"`
	Result     []promResult `json:"result"`
}

type promResult struct {
	Metric map[string]string `json:"metric"`
}

func queryRules(client *http.Client, endpoint, query string) ([][]prompb.Label, error) {
	u := endpoint + "/api/v1/query?query=" + url.QueryEscape(query)

	resp, err := client.Get(u)
	if err != nil {
		return nil, fmt.Errorf("cannot execute query %q: %w", query, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read query response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("unexpected status %d from %s: %s", resp.StatusCode, endpoint, body)
	}

	var pr promResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, fmt.Errorf("cannot parse query response: %w", err)
	}
	if pr.Status != "success" {
		return nil, fmt.Errorf("query returned non-success status %q: %s", pr.Status, pr.Error)
	}
	if len(pr.Data.Result) == 0 {
		return nil, nil
	}

	var rules [][]prompb.Label
	for _, res := range pr.Data.Result {
		keys, hasKeys := res.Metric["group_by_keys"]
		vals, hasVals := res.Metric["group_by_values"]

		if !hasKeys && !hasVals {
			return nil, fmt.Errorf("query result metric %v has neither group_by_keys nor group_by_values", res.Metric)
		}

		if keys == "__global__" || keys == "__group__" {
			continue
		}

		labels, err := parseLabels(keys, vals)
		if err != nil {
			return nil, fmt.Errorf("cannot parse labels for metric %v: %w", res.Metric, err)
		}
		if len(labels) > 0 {
			rules = append(rules, labels)
		}
	}
	return rules, nil
}

func parseLabels(groupByKeys, groupByValues string) ([]prompb.Label, error) {
	if groupByKeys == "" || groupByValues == "" {
		return nil, fmt.Errorf("group_by_keys and group_by_values must both be non-empty, got %q and %q", groupByKeys, groupByValues)
	}

	keys := splitEscaped(groupByKeys)
	vals := splitEscaped(groupByValues)

	if len(keys) != len(vals) {
		return nil, fmt.Errorf("group_by_keys has %d elements but group_by_values has %d elements", len(keys), len(vals))
	}

	labels := make([]prompb.Label, len(keys))
	for i := range keys {
		labels[i] = prompb.Label{Name: keys[i], Value: vals[i]}
	}
	return labels, nil
}

func splitEscaped(s string) []string {
	if s == "" {
		return nil
	}

	var result []string
	var cur strings.Builder

	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\\' && i+1 < len(s) {
			next := s[i+1]
			if next == ',' || next == '\\' {
				cur.WriteByte(next)
				i++
				continue
			}
		}
		if c == ',' {
			result = append(result, cur.String())
			cur.Reset()
			continue
		}
		cur.WriteByte(c)
	}
	result = append(result, cur.String())
	return result
}
