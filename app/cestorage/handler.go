package main

import (
	"bytes"
	"net/http"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/promremotewrite/stream"
	"github.com/VictoriaMetrics/metrics"
)

var prometheusWriteRequests = metrics.NewCounter(`cestorage_http_requests_total{path="/api/v1/write", protocol="promremotewrite"}`)
var rowsInserted = metrics.NewCounter(`cestorage_rows_inserted_total{type="promremotewrite"}`)

var (
	cardinalityMetricsRequests      = metrics.NewCounter(`cestorage_http_requests_total{path="/cardinality/metrics"}`)
	cardinalityMetricsDurationTotal = metrics.NewFloatCounter(`cestorage_http_request_duration_seconds_total{path="/cardinality/metrics"}`)
	cardinalityMetricsBytesTotal    = metrics.NewCounter(`cestorage_http_response_size_bytes_total{path="/cardinality/metrics"}`)
)

var (
	cardinalityCacheMu  sync.Mutex
	cardinalityCacheAt  time.Time
	cardinalityCache    []byte
	cardinalityCacheTTL = time.Minute
)

func requestHandler(estimators []*estimator) httpserver.RequestHandler {
	return func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/api/v1/write":
			prometheusWriteRequests.Inc()
			handleRemoteWrite(w, r, estimators)
			return true
		case "/cardinality/metrics":
			handleCardinalityMetrics(w, r, estimators)
			return true
		}
		return false
	}
}

func handleCardinalityMetrics(w http.ResponseWriter, _ *http.Request, estimators []*estimator) {
	startTime := time.Now()

	cardinalityCacheMu.Lock()
	if time.Since(cardinalityCacheAt) >= cardinalityCacheTTL {
		plain := bytes.NewBuffer(cardinalityCache[:0])
		for _, e := range estimators {
			e.writeMetrics(plain)
		}
		cardinalityCache = plain.Bytes()
		cardinalityCacheAt = time.Now()
	}
	responseData := cardinalityCache
	cardinalityCacheMu.Unlock()

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(responseData)

	cardinalityMetricsRequests.Inc()
	cardinalityMetricsDurationTotal.Add(time.Since(startTime).Seconds())
	cardinalityMetricsBytesTotal.Add(len(responseData))
}

func handleRemoteWrite(w http.ResponseWriter, r *http.Request, estimators []*estimator) {
	isVMRemoteWrite := r.Header.Get("Content-Encoding") == "zstd"
	err := stream.Parse(r.Body, isVMRemoteWrite, func(tss []prompb.TimeSeries, _ []prompb.MetricMetadata) error {
		for i := range tss {
			for _, e := range estimators {
				e.insert(tss[i].Labels)
			}
		}
		rowsInserted.Add(len(tss))
		return nil
	})
	if err != nil {
		httpserver.Errorf(w, r, "error parsing remote write request: %s", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
