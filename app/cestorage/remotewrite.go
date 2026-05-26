package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/metrics"
	"github.com/golang/snappy"
)

func init() {
	flagutil.RegisterSecretFlag("remoteWrite.url")
}

var (
	remoteWriteURLs = flagutil.NewArrayString(`remoteWrite.url`,
		"Optional URL(s) to push cardinality metrics to via Prometheus remote_write protocol. "+
			"Can be specified multiple times to push to multiple endpoints. "+
			"By default, cardinality metrics are not pushed anywhere and only exposed via -cardinalityMetrics.exposeAt.")

	remoteWriteInterval = flag.Duration(`remoteWrite.interval`, time.Minute,
		"Interval between successive remote_write pushes of the metrics")

	remoteWriteExtraLabels = flagutil.NewArrayString(`remoteWrite.extraLabel`,
		"Optional label=value to attach to every series pushed via -remoteWrite.url. "+
			"Can be specified multiple times. Example: -remoteWrite.extraLabel=env=production")

	remoteWriteHeaders = flagutil.NewArrayString(`remoteWrite.header`,
		"Optional HTTP header to send with every -remoteWrite.url request. "+
			"Can be specified multiple times. Example: -remoteWrite.header='Authorization: Basic foobar'")

	remoteWriteTimeout = flag.Duration(`remoteWrite.timeout`, 30*time.Second,
		"Per-request HTTP timeout for -remoteWrite.url pushes")

	remoteWriteRequests        = metrics.NewCounter(`cestorage_remote_write_requests_total`)
	remoteWriteErrors          = metrics.NewCounter(`cestorage_remote_write_errors_total`)
	remoteWriteSeriesPushed    = metrics.NewCounter(`cestorage_remote_write_series_pushed_total`)
	remoteWriteBytesSent       = metrics.NewCounter(`cestorage_remote_write_bytes_sent_total`)
	remoteWriteDurationSeconds = metrics.NewFloatCounter(`cestorage_remote_write_duration_seconds_total`)
)

type remoteWriter struct {
	urls        []string
	interval    time.Duration
	timeout     time.Duration
	extraLabels []prompb.Label
	headers     http.Header
	httpClient  *http.Client
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

func newRemoteWriter() (*remoteWriter, error) {
	extraLabels := make([]prompb.Label, 0, len(*remoteWriteExtraLabels))
	for _, kv := range *remoteWriteExtraLabels {
		idx := strings.IndexByte(kv, '=')
		if idx <= 0 {
			return nil, fmt.Errorf("invalid -remoteWrite.extraLabel=%q: must be key=value", kv)
		}
		extraLabels = append(extraLabels, prompb.Label{
			Name:  kv[:idx],
			Value: kv[idx+1:],
		})
	}

	headers := http.Header{}
	for _, h := range *remoteWriteHeaders {
		idx := strings.IndexByte(h, ':')
		if idx <= 0 {
			return nil, fmt.Errorf("invalid -remoteWrite.header=%q: must be 'Name: Value'", h)
		}
		headers.Add(strings.TrimSpace(h[:idx]), strings.TrimSpace(h[idx+1:]))
	}

	return &remoteWriter{
		urls:        []string(*remoteWriteURLs),
		interval:    *remoteWriteInterval,
		timeout:     *remoteWriteTimeout,
		extraLabels: extraLabels,
		headers:     headers,
		httpClient:  &http.Client{},
		stopCh:      make(chan struct{}),
	}, nil
}

// pushOnce snapshots cardinality estimates from all estimators and sends them to every configured URL as a single Prometheus remote_write request.
func (rw *remoteWriter) pushOnce(es []*estimator) {
	startTime := time.Now()
	nowMs := startTime.UnixMilli()

	var tss []prompb.TimeSeries
	for _, e := range es {
		tss = e.appendTimeSeries(tss, nowMs)
	}

	if len(tss) == 0 {
		return
	}

	// Attach extra labels
	if len(rw.extraLabels) > 0 {
		for i := range tss {
			tss[i].Labels = append(tss[i].Labels, rw.extraLabels...)
		}
	}

	wr := &prompb.WriteRequest{Timeseries: tss}
	pbData := wr.MarshalProtobuf(nil)
	compressed := snappy.Encode(nil, pbData)

	for _, url := range rw.urls {
		rw.sendTo(url, compressed, len(tss))
	}

	remoteWriteDurationSeconds.Add(time.Since(startTime).Seconds())
}

func (rw *remoteWriter) sendTo(url string, compressed []byte, seriesCount int) {
	remoteWriteRequests.Inc()

	ctx, cancel := context.WithTimeout(context.Background(), rw.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(compressed))
	if err != nil {
		remoteWriteErrors.Inc()
		logger.Errorf("remotewrite: cannot build request to %s: %s", url, err)
		return
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	for name, values := range rw.headers {
		for _, v := range values {
			req.Header.Add(name, v)
		}
	}

	resp, err := rw.httpClient.Do(req)
	if err != nil {
		remoteWriteErrors.Inc()
		logger.Errorf("remotewrite: request to %s failed: %s", url, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		remoteWriteErrors.Inc()
		logger.Errorf("remotewrite: unexpected status %d from %s", resp.StatusCode, url)
		return
	}

	remoteWriteSeriesPushed.Add(seriesCount)
	remoteWriteBytesSent.Add(len(compressed))
}

// run starts the periodic push loop.
func (rw *remoteWriter) run(es []*estimator) {
	defer rw.wg.Done()
	t := time.NewTicker(rw.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			rw.pushOnce(es)
		case <-rw.stopCh:
			return
		}
	}
}

// stopAndFlush stops the periodic push loop, waits for any in-flight push to
// finish, then performs one final push so the last window's data is not lost.
func (rw *remoteWriter) stopAndFlush(es []*estimator) {
	close(rw.stopCh)
	rw.wg.Wait()
	logger.Infof("remotewrite: performing final push on shutdown")
	rw.pushOnce(es)
}
