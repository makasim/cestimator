package main

import (
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/metrics"
	"github.com/makasim/cestimator/app/cestorage/protoparser"
)

var prometheusWriteRequests = metrics.NewCounter(`cestorage_http_requests_total{path="/api/v1/write", protocol="promremotewrite"}`)
var rowsInserted = metrics.NewCounter(`cestorage_rows_inserted_total{type="promremotewrite"}`)

func requestHandler(estimators []*estimator) httpserver.RequestHandler {
	groupLabelsMap := make(map[string]struct{})
	for _, e := range estimators {
		for _, l := range e.groupBy {
			groupLabelsMap[l] = struct{}{}
		}
	}

	groupLabels := make([]string, 0, len(groupLabelsMap))
	for k := range groupLabelsMap {
		groupLabels = append(groupLabels, k)
	}

	return func(w http.ResponseWriter, r *http.Request) bool {
		cmPath := *cardinalityMetricsExposeAt

		if cmPath != "/metrics" && cmPath != "" && r.URL.Path == cmPath {
			w.WriteHeader(http.StatusOK)
			writeCardinalityMetrics(w, estimators)
			return true
		}

		switch r.URL.Path {
		case "/api/v1/write":
			prometheusWriteRequests.Inc()
			handleRemoteWrite(w, r, groupLabels, estimators)
			return true
		case "/cardinality/reset":
			handleCardinalityReset(w, r, estimators)
			return true
		}
		return false
	}
}

func handleCardinalityReset(w http.ResponseWriter, _ *http.Request, estimators []*estimator) {
	for _, e := range estimators {
		e.reset()
	}
	w.WriteHeader(http.StatusOK)
}

func handleRemoteWrite(w http.ResponseWriter, r *http.Request, groupLabels []string, estimators []*estimator) {
	err := protoparser.Parse(r.Body, groupLabels, func(tss []protoparser.TimeSerie) {
		//var wg sync.WaitGroup
		for _, e := range estimators {
			//wg.Go(func() {
			e.insertMany(tss)
			//})
		}
		//wg.Wait()
		rowsInserted.Add(len(tss))
	})
	if err != nil {
		httpserver.Errorf(w, r, "error parsing remote write request: %s", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
