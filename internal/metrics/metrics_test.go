package metrics

import (
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestMetricsRecorded wires an OTEL SDK manual reader, records each instrument, and
// asserts the collected values — proving the metrics flow end to end.
func TestMetricsRecorded(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)

	ctx := t.Context()
	Request(ctx, "lock", "granted")
	Request(ctx, "lock", "granted")
	Request(ctx, "lock", "queued")
	Forward(ctx, "remote")
	ProposeDuration(ctx, 0.0012)
	LeasesExpired(ctx, 3)
	LeasesExpired(ctx, 0) // ignored

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("TestMetricsRecorded: Collect: %s", err)
	}

	checks := []struct {
		metric string
		want   int64
	}{
		{metric: "zuul.requests", want: 3},         // 2 granted + 1 queued
		{metric: "zuul.forward.proposes", want: 1}, // 1 remote
		{metric: "zuul.lease.expired", want: 3},    // 3 + ignored 0
	}
	for _, c := range checks {
		if got := counterSum(t, &rm, c.metric); got != c.want {
			t.Errorf("TestMetricsRecorded: %s sum = %d, want %d", c.metric, got, c.want)
		}
	}

	if got := histogramCount(t, &rm, "zuul.propose.duration"); got != 1 {
		t.Errorf("TestMetricsRecorded: zuul.propose.duration count = %d, want 1", got)
	}
}

// counterSum returns the total across all data points of an int64 sum metric.
func counterSum(t *testing.T, rm *metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	m := findMetric(t, rm, name)
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("counterSum(%s): data is %T, want Sum[int64]", name, m.Data)
	}
	var total int64
	for _, dp := range sum.DataPoints {
		total += dp.Value
	}
	return total
}

// histogramCount returns the total count across all data points of a histogram.
func histogramCount(t *testing.T, rm *metricdata.ResourceMetrics, name string) uint64 {
	t.Helper()
	m := findMetric(t, rm, name)
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("histogramCount(%s): data is %T, want Histogram[float64]", name, m.Data)
	}
	var total uint64
	for _, dp := range h.DataPoints {
		total += dp.Count
	}
	return total
}

// findMetric locates a metric by name across all scopes.
func findMetric(t *testing.T, rm *metricdata.ResourceMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("findMetric: metric %q not recorded", name)
	return metricdata.Metrics{}
}
