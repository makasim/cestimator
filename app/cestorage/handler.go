package main

import (
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/promremotewrite/stream"
	"github.com/VictoriaMetrics/metrics"
)

var prometheusWriteRequests = metrics.NewCounter(`cestorage_http_requests_total{path="/api/v1/write", protocol="promremotewrite"}`)
var rowsInserted = metrics.NewCounter(`cestorage_rows_inserted_total{type="promremotewrite"}`)

func requestHandler(estimators []*Estimator) httpserver.RequestHandler {
	return func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/api/v1/write" {
			prometheusWriteRequests.Inc()
			handleRemoteWrite(w, r, estimators)
			return true
		}
		return false
	}
}

func handleRemoteWrite(w http.ResponseWriter, r *http.Request, estimators []*Estimator) {
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
