package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/promremotewrite/stream"
)

func BenchmarkStreamParse_EstimatorGlobal(b *testing.B) {
	data := buildSnappyEncodedWriteRequest(5_000, 3, 5, 1)
	e, err := newEstimator(EstimatorConfig{Interval: Duration(time.Hour)})
	if err != nil {
		b.Fatalf("newEstimator: %v", err)
	}
	defer e.Stop()
	estimators := []*estimator{e}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		err := stream.Parse(bytes.NewReader(data), false, func(tss []prompb.TimeSeries, _ []prompb.MetricMetadata) error {
			for i := range tss {
				for _, est := range estimators {
					est.insert(tss[i].Labels)
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("stream.Parse: %v", err)
		}
	}
}

func BenchmarkStreamParse_EstimatorGroup(b *testing.B) {
	data := buildSnappyEncodedWriteRequest(5_000, 3, 5, 100)
	e, err := newEstimator(EstimatorConfig{
		Group:    []string{"groupLabel"},
		Interval: Duration(time.Hour),
	})
	if err != nil {
		b.Fatalf("newEstimator: %v", err)
	}
	defer e.Stop()
	estimators := []*estimator{e}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		err := stream.Parse(bytes.NewReader(data), false, func(tss []prompb.TimeSeries, _ []prompb.MetricMetadata) error {
			for i := range tss {
				for _, est := range estimators {
					est.insert(tss[i].Labels)
				}
			}
			return nil
		})
		if err != nil {
			b.Fatalf("stream.Parse: %v", err)
		}
	}
}

// buildSnappyEncodedWriteRequest builds a snappy-encoded protobuf WriteRequest
// with numSeries time series, each having numLabels labels of labelSize bytes each.
func buildSnappyEncodedWriteRequest(numSeries, numLabels, labelSize, groupsNum int) []byte {
	labelValue := strings.Repeat("x", labelSize)

	tss := make([]prompb.TimeSeries, numSeries)
	for i := range tss {
		labels := make([]prompb.Label, numLabels)
		for j := range labels {
			labels[j] = prompb.Label{
				Name:  fmt.Sprintf("label%02d", j),
				Value: fmt.Sprintf("val%05d_%s", i, labelValue),
			}
		}
		labels = append(labels, prompb.Label{
			Name:  "groupLabel",
			Value: fmt.Sprintf("%d", i%groupsNum),
		})

		tss[i] = prompb.TimeSeries{
			Labels:  labels,
			Samples: []prompb.Sample{{Value: 1, Timestamp: 1000}},
		}
	}

	wr := &prompb.WriteRequest{Timeseries: tss}
	pbData := wr.MarshalProtobuf(nil)
	return snappy.Encode(nil, pbData)
}
