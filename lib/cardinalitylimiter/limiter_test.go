package cardinalitylimiter

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

func TestLimit_EmptyRules(t *testing.T) {
	l := &Limiter{}
	tss := []prompb.TimeSeries{
		{Labels: []prompb.Label{{Name: "job", Value: "foo"}}},
		{Labels: []prompb.Label{{Name: "job", Value: "bar"}}},
	}
	got := l.Limit(tss, nil)
	if len(got) != 2 {
		t.Fatalf("expected 2 series, got %d", len(got))
	}
}

func TestLimit_ExactMatch(t *testing.T) {
	l := &Limiter{}
	rules := [][]prompb.Label{
		{{Name: "job", Value: "foo"}},
	}
	l.rules.Store(&rules)

	tss := []prompb.TimeSeries{
		{Labels: []prompb.Label{{Name: "job", Value: "foo"}}},
		{Labels: []prompb.Label{{Name: "job", Value: "bar"}}},
	}
	got := l.Limit(tss, nil)
	if len(got) != 1 {
		t.Fatalf("expected 1 series, got %d", len(got))
	}
	if got[0].Labels[0].Value != "bar" {
		t.Fatalf("unexpected surviving series: %v", got[0].Labels)
	}
}

func TestLimit_SupersetMatch(t *testing.T) {
	l := &Limiter{}
	rules := [][]prompb.Label{
		{{Name: "job", Value: "foo"}},
	}
	l.rules.Store(&rules)

	tss := []prompb.TimeSeries{
		{Labels: []prompb.Label{
			{Name: "instance", Value: "host1"},
			{Name: "job", Value: "foo"},
		}},
	}
	got := l.Limit(tss, nil)
	if len(got) != 0 {
		t.Fatalf("expected 0 series, got %d", len(got))
	}
}

func TestLimit_PartialMatch(t *testing.T) {
	l := &Limiter{}
	rules := [][]prompb.Label{
		{
			{Name: "job", Value: "foo"},
			{Name: "env", Value: "prod"},
		},
	}
	l.rules.Store(&rules)

	tss := []prompb.TimeSeries{
		{Labels: []prompb.Label{{Name: "job", Value: "foo"}}},
	}
	got := l.Limit(tss, nil)
	if len(got) != 1 {
		t.Fatalf("expected 1 series, got %d", len(got))
	}
}

func TestLimit_MultipleRules(t *testing.T) {
	l := &Limiter{}
	rules := [][]prompb.Label{
		{{Name: "job", Value: "foo"}},
		{{Name: "job", Value: "bar"}},
	}
	l.rules.Store(&rules)

	tss := []prompb.TimeSeries{
		{Labels: []prompb.Label{{Name: "job", Value: "bar"}}},
		{Labels: []prompb.Label{{Name: "job", Value: "baz"}}},
	}
	got := l.Limit(tss, nil)
	if len(got) != 1 {
		t.Fatalf("expected 1 series, got %d", len(got))
	}
	if got[0].Labels[0].Value != "baz" {
		t.Fatalf("unexpected surviving series: %v", got[0].Labels)
	}
}

func TestLimit_ResReuse(t *testing.T) {
	l := &Limiter{}
	rules := [][]prompb.Label{
		{{Name: "job", Value: "drop"}},
	}
	l.rules.Store(&rules)

	tss := []prompb.TimeSeries{
		{Labels: []prompb.Label{{Name: "job", Value: "keep"}}},
	}
	res := make([]prompb.TimeSeries, 0, 10)
	got := l.Limit(tss, res)
	if len(got) != 1 {
		t.Fatalf("expected 1, got %d", len(got))
	}
	if cap(got) < 10 {
		t.Fatal("res buffer capacity was not preserved")
	}
}

func TestParseLabels_Simple(t *testing.T) {
	labels, err := parseLabels("instance,job", "host1,prom")
	if err != nil {
		t.Fatal(err)
	}
	if len(labels) != 2 {
		t.Fatalf("expected 2 labels, got %d", len(labels))
	}
	if labels[0].Name != "instance" || labels[0].Value != "host1" {
		t.Fatalf("unexpected label[0]: %v", labels[0])
	}
	if labels[1].Name != "job" || labels[1].Value != "prom" {
		t.Fatalf("unexpected label[1]: %v", labels[1])
	}
}

func TestParseLabels_EscapedComma(t *testing.T) {
	labels, err := parseLabels("k", `v1\,v2`)
	if err != nil {
		t.Fatal(err)
	}
	if len(labels) != 1 {
		t.Fatalf("expected 1 label, got %d", len(labels))
	}
	if labels[0].Value != "v1,v2" {
		t.Fatalf("unexpected value: %q", labels[0].Value)
	}
}

func TestParseLabels_EscapedBackslash(t *testing.T) {
	labels, err := parseLabels("k", `v1\\v2`)
	if err != nil {
		t.Fatal(err)
	}
	if labels[0].Value != `v1\v2` {
		t.Fatalf("unexpected value: %q", labels[0].Value)
	}
}

func TestParseLabels_EitherEmpty(t *testing.T) {
	if _, err := parseLabels("", "foo"); err == nil {
		t.Fatal("expected error when group_by_keys is empty")
	}
	if _, err := parseLabels("foo", ""); err == nil {
		t.Fatal("expected error when group_by_values is empty")
	}
}

func TestParseLabels_Mismatch(t *testing.T) {
	_, err := parseLabels("a,b", "x")
	if err == nil {
		t.Fatal("expected error for mismatched key/value count")
	}
}

func TestSplitEscaped_Empty(t *testing.T) {
	if splitEscaped("") != nil {
		t.Fatal("expected nil for empty string")
	}
}

func TestSplitEscaped_Single(t *testing.T) {
	got := splitEscaped("hello")
	if len(got) != 1 || got[0] != "hello" {
		t.Fatalf("unexpected: %v", got)
	}
}

func TestQueryRules_EmptyResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(promResponse{
			Status: "success",
			Data:   promData{ResultType: "vector", Result: nil},
		})
	}))
	defer srv.Close()

	rules, err := queryRules(srv.Client(), srv.URL, "cardinality_estimate")
	if err != nil {
		t.Fatal(err)
	}
	if rules != nil {
		t.Fatalf("expected nil rules for empty result, got %v", rules)
	}
}

func TestQueryRules_SkipsSummaryRows(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(promResponse{
			Status: "success",
			Data: promData{
				ResultType: "vector",
				Result: []promResult{
					{Metric: map[string]string{"group_by_keys": "__global__"}},
					{Metric: map[string]string{"group_by_keys": "__group__", "group_by_values": "job"}},
					{Metric: map[string]string{"group_by_keys": "job", "group_by_values": "foo"}},
				},
			},
		})
	}))
	defer srv.Close()

	rules, err := queryRules(srv.Client(), srv.URL, "cardinality_estimate")
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0][0].Name != "job" || rules[0][0].Value != "foo" {
		t.Fatalf("unexpected rule: %v", rules[0])
	}
}

func TestQueryRules_MissingGroupByLabels(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(promResponse{
			Status: "success",
			Data: promData{
				ResultType: "vector",
				Result: []promResult{
					{Metric: map[string]string{"__name__": "some_metric"}},
				},
			},
		})
	}))
	defer srv.Close()

	_, err := queryRules(srv.Client(), srv.URL, "some_metric")
	if err == nil {
		t.Fatal("expected error for missing group_by_keys and group_by_values")
	}
}

func TestQueryRules_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := queryRules(srv.Client(), srv.URL, "cardinality_estimate")
	if err == nil {
		t.Fatal("expected error for HTTP 500")
	}
}
