// Package otel groups zuul's OpenTelemetry wiring. It has no code of its own; its
// subpackages hold the service's telemetry:
//
//   - metrics: OTel metric instruments (request counts, durations, lease/session gauges).
//   - tracing: OTel trace/span setup.
//
// They are reached through the context package's Meter/MeterProvider and NewSpan/Span
// accessors during request handling; this grouping is purely organizational.
package otel
