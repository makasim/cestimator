package main

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/makasim/cestimator/app/cestorage/protoparser"
)

func BenchmarkWriteMetrics_NoGroup(b *testing.B) {
	b.Run("NoPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{Interval: time.Hour})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.Stop()
		insertSeriesIntoEstimator(e, 5_000, 0)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.writeMetrics(io.Discard)
		}
	})

	b.Run("WithPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{Interval: time.Hour})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.Stop()
		insertSeriesIntoEstimator(e, 5_000, 0)
		e.rotate()
		insertSeriesIntoEstimator(e, 5_000, 0)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.writeMetrics(io.Discard)
		}
	})
}

func BenchmarkWriteMetrics_Group100(b *testing.B) {
	b.Run("NoPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			Group:    []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.Stop()
		insertSeriesIntoEstimator(e, 5_000, 100)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.writeMetrics(io.Discard)
		}
	})

	b.Run("WithPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			Group:    []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.Stop()
		insertSeriesIntoEstimator(e, 5_000, 100)
		e.rotate()
		insertSeriesIntoEstimator(e, 5_000, 100)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.writeMetrics(io.Discard)
		}
	})
}

func BenchmarkWriteMetrics_Group10k(b *testing.B) {
	b.Run("NoPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			Group:    []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.Stop()
		insertSeriesIntoEstimator(e, 50_000, 10_000)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.writeMetrics(io.Discard)
		}
	})

	b.Run("WithPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{
			Group:    []string{"groupLabel"},
			Interval: time.Hour,
		})
		if err != nil {
			b.Fatalf("newEstimator: %v", err)
		}
		defer e.Stop()
		insertSeriesIntoEstimator(e, 50_000, 10_000)
		e.rotate()
		insertSeriesIntoEstimator(e, 50_000, 10_000)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			e.writeMetrics(io.Discard)
		}
	})
}

// insertSeriesIntoEstimator inserts numSeries time series into e.
// When groupsNum > 0 each series gets a "groupLabel" cycling through groupsNum values.
func insertSeriesIntoEstimator(e *estimator, numSeries, groupsNum int) {
	for i := 0; i < numSeries; i++ {
		var labels []protoparser.Label
		if groupsNum > 0 {
			labels = append(labels, protoparser.Label{
				Name:  "groupLabel",
				Value: fmt.Sprintf("%d", i%groupsNum),
			})
		}
		e.insert(protoparser.TimeSerie{
			GroupLabels: labels,
			Fingerprint: uint64(i),
		})
	}
}
