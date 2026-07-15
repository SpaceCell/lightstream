#!/usr/bin/env bash
#
# Local industry benchmark of parallel Arrow streaming and Arrow Flight throughput.
#
# Runs the matched-stream-count comparison over loopback - Arrow Flight against
# Lightstream TCP parallel and the Lightstream protocol parallel - across the
# standard shape and scale matrix. Both sides ship the same Arrow payload at the
# same stream counts and ordering contract.
#
# Override the matrix with LIGHTSTREAM_BENCH_MATRIX (quick, standard, full).
# Extra arguments pass through to the Criterion harness, for example a benchmark
# id filter or "--measurement-time 5".

set -euo pipefail

cd "$(dirname "$0")"

if [ "$(uname -s)" != "Linux" ]; then
    echo "WARNING: Lightstream is heavily optimised for Linux deployment."
    echo "         Results on $(uname -s) may vary widely across platforms."
    echo
fi

MATRIX="${LIGHTSTREAM_BENCH_MATRIX:-standard}"

echo "Lightstream vs Arrow Flight - matrix: ${MATRIX}"
echo

LIGHTSTREAM_BENCH_MATRIX="${MATRIX}" \
    cargo bench --bench arrow_flight_comparison \
    --features "bench_arrow_flight,tcp,protocol" \
    -- "$@"
