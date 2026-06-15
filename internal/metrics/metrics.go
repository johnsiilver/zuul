// Package metrics records Zuul's OpenTelemetry metrics. Instruments are created
// lazily from the process's configured OTEL meter provider (the gostdlib/base
// telemetry setup wires one; absent that, the no-op provider makes every record a
// cheap no-op). Recording happens only at non-replicated chokepoints — the forward
// dispatcher, the gRPC handlers, and the expiry sweep — never inside the FSM, whose
// apply must stay a deterministic, side-effect-free function on every replica.
package metrics

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
)

const meterName = "github.com/johnsiilver/zuul"

var (
	once       sync.Once
	requests   metric.Int64Counter
	forwards   metric.Int64Counter
	proposeDur metric.Float64Histogram
	leaseExp   metric.Int64Counter
	sessions   metric.Int64UpDownCounter
)

// ensure creates the instruments once, from the meter provider current at that time.
func ensure() {
	once.Do(func() {
		m := otel.Meter(meterName)
		requests, _ = m.Int64Counter("zuul.requests",
			metric.WithDescription("client request outcomes, by operation"))
		forwards, _ = m.Int64Counter("zuul.forward.proposes",
			metric.WithDescription("proposes, split by whether they ran locally or were forwarded to the leader"))
		proposeDur, _ = m.Float64Histogram("zuul.propose.duration",
			metric.WithDescription("propose latency including any forward and retries"),
			metric.WithUnit("s"))
		leaseExp, _ = m.Int64Counter("zuul.lease.expired",
			metric.WithDescription("leases reclaimed by the leader-driven expiry sweep"))
		sessions, _ = m.Int64UpDownCounter("zuul.sessions.active",
			metric.WithDescription("client sessions currently open on this node"))
	})
}

// SessionsActive adjusts the open-session gauge by delta (+1 on open, -1 on close).
func SessionsActive(ctx context.Context, delta int64) {
	ensure()
	sessions.Add(ctx, delta)
}

// Request records one client-facing operation outcome (e.g. op="lock",
// outcome="granted").
func Request(ctx context.Context, op, outcome string) {
	ensure()
	requests.Add(ctx, 1, metric.WithAttributes(
		attribute.String("op", op),
		attribute.String("outcome", outcome),
	))
}

// Forward records one propose, attributed by target ("local" or "remote").
func Forward(ctx context.Context, target string) {
	ensure()
	forwards.Add(ctx, 1, metric.WithAttributes(attribute.String("target", target)))
}

// ProposeDuration records a propose's wall-clock latency in seconds.
func ProposeDuration(ctx context.Context, seconds float64) {
	ensure()
	proposeDur.Record(ctx, seconds)
}

// LeasesExpired records that n leases were reclaimed by the expiry sweep.
func LeasesExpired(ctx context.Context, n int) {
	if n == 0 {
		return
	}
	ensure()
	leaseExp.Add(ctx, int64(n))
}
