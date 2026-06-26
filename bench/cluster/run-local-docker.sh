#!/usr/bin/env bash
# Local Docker validation of the cross-host benchmark. Builds the image and runs the
# source and sink as two containers on a user-defined network, exercising the
# image build, the runtime binaries and the benchmark end to end.
#
# Both containers share one host, so the figures are not representative. Use the
# EKS benchmark (run.sh) for cross-node numbers. This check is for correctness.
#
# Override SHAPE, ROWS, BATCHES_PER_STREAM and STREAMS via the environment.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

IMAGE="${IMAGE:-lightstream-vpc-bench:local}"
NET="${NET:-lightstream-vpc-bench}"
SHAPE="${SHAPE:-mixed}"
ROWS="${ROWS:-100000}"
BATCHES_PER_STREAM="${BATCHES_PER_STREAM:-50}"
STREAMS="${STREAMS:-4,8}"

cleanup() {
  docker rm -f bench-vpc-source bench-vpc-sink >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[local] building image $IMAGE"
docker build -f "$HERE/Dockerfile" -t "$IMAGE" "$ROOT"

docker network create "$NET" >/dev/null 2>&1 || true

echo "[local] starting source"
docker run -d --name bench-vpc-source --network "$NET" "$IMAGE" \
  bench_vpc_source --shape "$SHAPE" --rows "$ROWS" \
  --batches-per-stream "$BATCHES_PER_STREAM" --streams "$STREAMS" \
  --flight-bind 0.0.0.0:9101 --echo-bind 0.0.0.0:9102 \
  --sink-ls-addr bench-vpc-sink:9103 >/dev/null

echo "[local] running sink"
docker run --name bench-vpc-sink --network "$NET" "$IMAGE" \
  bench_vpc_sink --shape "$SHAPE" --rows "$ROWS" \
  --batches-per-stream "$BATCHES_PER_STREAM" --streams "$STREAMS" \
  --source-flight-addr bench-vpc-source:9101 \
  --source-echo-addr bench-vpc-source:9102 --ls-bind 0.0.0.0:9103

echo "[local] source log tail:"
docker logs bench-vpc-source 2>&1 | grep -v flatbuffers | tail -5
