#!/usr/bin/env bash
#
# run_scaling_bench.sh — reproducible, benchstat-friendly scaling benchmarks.
#
# Runs the scaling benchmarks in scaling_bench_test.go across a set of GOMAXPROCS
# values with -count for statistical stability, strips the dragonboat/gostdlib log
# lines that interleave with (and split) the benchmark output, and emits:
#
#   $OUT/p<N>          one benchstat input file per GOMAXPROCS value
#   $OUT/comparison.txt  benchstat table, one column per GOMAXPROCS
#   $OUT/scaling.csv     the same, as CSV (benchstat -format csv)
#   $OUT/raw-p<N>.txt    untouched go-test output, for debugging
#
# Benchmark names have their trailing -<GOMAXPROCS> suffix removed so a benchmark
# lines up across the per-proc files (benchstat then shows one column per proc).
#
# Env overrides (all optional):
#   BENCH      benchmark regex (default: all scaling benchmarks)
#   PROCS      space-separated GOMAXPROCS values (default: "1 2 4 8")
#   COUNT      -count, runs per benchmark for statistics (default: 5)
#   BENCHTIME  -benchtime per run (default: 1s)
#   OUT        output directory (default: bench-out)
#   PKG        package under test (default: ./internal/integration)
#
# A full default run is LONG (all benchmarks x all sub-cases x COUNT x PROCS).
# Narrow it, e.g.:
#   BENCH=BenchmarkShardSaturation PROCS='1 2 4 8' COUNT=6 ./internal/integration/run_scaling_bench.sh
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.." # repo root

PKG="${PKG:-./internal/integration}"
BENCH="${BENCH:-BenchmarkLockCapacity|BenchmarkLeaderCapacity|BenchmarkLatencyVsConnections|BenchmarkContendedKey|BenchmarkForwardedWrite|BenchmarkShardedThroughput|BenchmarkShardSaturation}"
PROCS="${PROCS:-1 2 4 8}"
COUNT="${COUNT:-5}"
BENCHTIME="${BENCHTIME:-1s}"
OUT="${OUT:-bench-out}"
export GOPROXY="${GOPROXY:-off}" # repo is vendored; avoid the network by default

if ! command -v benchstat >/dev/null 2>&1; then
	echo "error: benchstat not on PATH. Install it with:" >&2
	echo "  go install golang.org/x/perf/cmd/benchstat@latest" >&2
	exit 1
fi

mkdir -p "$OUT"

# clean rebuilds benchstat-parseable lines from go-test output that the dragonboat and
# gostdlib loggers split mid-line (they write to stdout between the benchmark name and
# its metrics), and strips the trailing -<GOMAXPROCS> suffix so names align across files.
clean() {
	awk '
		function emit(nm, metrics) {
			sub(/^[ \t]+/, "", metrics)   # drop leading whitespace on the metrics
			sub(/-[0-9]+$/, "", nm)        # drop the -<GOMAXPROCS> name suffix
			print nm "\t" metrics
		}
		/^Benchmark/ {
			if ($0 ~ / ns\/op/) {          # a clean, un-split line
				m = $0; sub(/^[^ \t]+[ \t]+/, "", m); emit($1, m); name = ""; next
			}
			name = $1; next                # split: remember the name for the metrics line
		}
		/ ns\/op/ { if (name != "") { emit(name, $0); name = "" } }
	'
}

files=()
for p in $PROCS; do
	echo ">> GOMAXPROCS=$p  (count=$COUNT, benchtime=$BENCHTIME)" >&2
	raw="$OUT/raw-p$p.txt"
	dst="$OUT/p$p"
	if ! GOMAXPROCS="$p" go test -run '^$' -bench "$BENCH" -benchmem \
		-count="$COUNT" -benchtime="$BENCHTIME" "$PKG" >"$raw" 2>"$OUT/stderr-p$p.txt"; then
		echo "error: benchmark run failed for GOMAXPROCS=$p (see $raw / $OUT/stderr-p$p.txt)" >&2
		exit 1
	fi
	clean <"$raw" >"$dst"
	files+=("$dst")
done

echo >&2
echo ">> benchstat comparison (one column per GOMAXPROCS file):" >&2
benchstat "${files[@]}" | tee "$OUT/comparison.txt"

benchstat -format csv "${files[@]}" >"$OUT/scaling.csv" 2>/dev/null || true

echo >&2
echo ">> wrote $OUT/comparison.txt, $OUT/scaling.csv, and per-proc inputs ${files[*]}" >&2
