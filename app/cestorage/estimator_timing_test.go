package main

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

func BenchmarkWriteMetrics_NoGroup(b *testing.B) {
	b.Run("NoPrev", func(b *testing.B) {
		e, err := newEstimator(EstimatorConfig{Interval: Duration(time.Hour)})
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
		e, err := newEstimator(EstimatorConfig{Interval: Duration(time.Hour)})
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
			Interval: Duration(time.Hour),
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
			Interval: Duration(time.Hour),
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
			Interval: Duration(time.Hour),
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
			Interval: Duration(time.Hour),
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
		labels := []prompb.Label{
			{Name: "label00", Value: fmt.Sprintf("val%07d", i)},
			{Name: "label01", Value: fmt.Sprintf("val%07d", i)},
			{Name: "label02", Value: fmt.Sprintf("val%07d", i)},
		}
		if groupsNum > 0 {
			labels = append(labels, prompb.Label{
				Name:  "groupLabel",
				Value: fmt.Sprintf("%d", i%groupsNum),
			})
		}
		e.insert(labels)
	}
}
