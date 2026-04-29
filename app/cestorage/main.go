package main

import (
	"bytes"
	"flag"
	"io"
	"os"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/pushmetrics"
	"github.com/VictoriaMetrics/metrics"
)

var (
	httpListenAddrs = flagutil.NewArrayString("httpListenAddr", "TCP address to listen for incoming HTTP requests")
	configPath      = flag.String("config", "config.yaml", "Path to YAML configuration file")
)

func main() {
	flag.CommandLine.SetOutput(os.Stdout)
	envflag.Parse()
	buildinfo.Init()
	logger.Init()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		logger.Fatalf("cannot load config: %v", err)
	}

	estimators := make([]*estimator, 0, len(cfg.Streams))
	for _, ec := range cfg.Streams {
		e, err := newEstimator(ec)
		if err != nil {
			logger.Fatalf("cannot create estimator: %v", err)
		}
		estimators = append(estimators, e)
	}

	if *cardinalityMetricsExposeAt == `/metrics` {
		metrics.RegisterMetricsWriter(func(w io.Writer) {
			writeCardinalityMetrics(w, estimators)
		})
	}

	listenAddrs := *httpListenAddrs
	if len(listenAddrs) == 0 {
		listenAddrs = []string{":8490"}
	}

	logger.Infof("starting cestorage at %q", listenAddrs)
	startTime := time.Now()

	go httpserver.Serve(listenAddrs, requestHandler(estimators), httpserver.ServeOptions{})

	logger.Infof("started cestorage in %.3f seconds", time.Since(startTime).Seconds())

	pushmetrics.Init()
	sig := procutil.WaitForSigterm()
	logger.Infof("received signal %s", sig)
	pushmetrics.Stop()

	logger.Infof("gracefully shutting down webservice at %q", listenAddrs)
	if err := httpserver.Stop(listenAddrs); err != nil {
		logger.Errorf("cannot stop http server: %s", err)
	}
	for _, e := range estimators {
		e.stop()
	}
	logger.Infof("shutting down cestorage")
}

var (
	cardinalityMetricsWrites        = metrics.NewCounter(`cestorage_write_cardinality_metrics_total`)
	cardinalityMetricsWriteDuration = metrics.NewFloatCounter(`cestorage_write_cardinality_metrics_duration_seconds_total`)
	cardinalityMetricsWriteBytes    = metrics.NewCounter(`cestorage_write_cardinality_metrics_size_bytes_total`)

	cardinalityCacheMu         sync.Mutex
	cardinalityMetricsCacheAt  time.Time
	cardinalityMetricsCache    []byte
	cardinalityMetricsCacheTTL = flag.Duration("cardinalityMetrics.cacheTTL", time.Minute, "Duration for caching cardinality metrics response")
	cardinalityMetricsExposeAt = flag.String(`cardinalityMetrics.exposeAt`, `/metrics`, "HTTP path for exposing cardinality metrics. "+
		"If set to the default /metrics, cardinality metrics are merged with regular metrics and exposed together. "+
		"If set to a different path, only cardinality metrics are exposed at that endpoint. "+
		"If set to an empty value, cardinality metrics are not exposed via HTTP at all.")
)

func writeCardinalityMetrics(w io.Writer, es []*estimator) {
	startTime := time.Now()

	cardinalityCacheMu.Lock()
	if time.Since(cardinalityMetricsCacheAt) >= *cardinalityMetricsCacheTTL || *cardinalityMetricsCacheTTL == 0 {
		plain := bytes.NewBuffer(cardinalityMetricsCache[:0])
		for _, e := range es {
			e.writeMetrics(plain)
		}
		cardinalityMetricsCache = plain.Bytes()
		cardinalityMetricsCacheAt = time.Now()
	}
	cm := cardinalityMetricsCache
	cardinalityCacheMu.Unlock()

	_, _ = w.Write(cm)

	cardinalityMetricsWrites.Inc()
	cardinalityMetricsWriteDuration.Add(time.Since(startTime).Seconds())
	cardinalityMetricsWriteBytes.Add(len(cm))
}
