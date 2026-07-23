#!/usr/bin/env bash
# Local Docker validation of the cross-host benchmark. Builds the image and runs
# the source and sink as two containers on a user-defined network, exercising the
# image build, the runtime binaries and the benchmark end to end.
#
# Both containers share one host, so the figures are not representative. Use the
# ECS benchmark (run.sh) for cross-host numbers. This check is for correctness.
#
# Both data sources are exercised. The memory source measures RAM -> transport.
# The nvme source writes the workload to a docker volume mounted at /data on
# the source. Each transport then evicts the page cache and replays the files
# once cold, followed by RUNS warm replays, exercising the on-disk replay path
# and the phase-control channel. The volume is not the NVMe device, so nvme
# figures here only confirm the code path runs.
#
# Override SHAPE, DATA_SOURCES, ROWS, DATASET_GB, STREAMS, RUNS and
# USE_MMAP via the environment. USE_MMAP=1 replays the nvme files through
# the mmap reader instead of the default buffered file reader, for the
# in-RAM replay comparison.

set -euo pipefail

# The Dockerfile's cache mounts require BuildKit.
export DOCKER_BUILDKIT=1

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"    # the lightstream repo root
CONTEXT="$(cd "$ROOT/.." && pwd)"       # parent dir holding lightstream + minarrow

IMAGE="${IMAGE:-lightstream-ecs-bench:local}"
NET="${NET:-lightstream-ecs-bench}"
VOLUME="${VOLUME:-lightstream-ecs-bench-data}"
SHAPE="${SHAPE:-mixed}"
DATA_SOURCES="${DATA_SOURCES:-memory nvme}"
ROWS="${ROWS:-100000}"
DATASET_GB="${DATASET_GB:-1}"
STREAMS="${STREAMS:-1,4}"
RUNS="${RUNS:-1}"
USE_MMAP="${USE_MMAP:-0}"

FLIGHT_PORT="${FLIGHT_PORT:-9101}"
ECHO_PORT="${ECHO_PORT:-9102}"
LS_PORT="${LS_PORT:-9103}"
CTRL_PORT="${CTRL_PORT:-9104}"

STAGED_IGNORE="$CONTEXT/.dockerignore"
STAGED_IGNORE_BAK=""

cleanup() {
  docker rm -f bench-ecs-source bench-ecs-sink >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  docker volume rm "$VOLUME" >/dev/null 2>&1 || true
  rm -f "$STAGED_IGNORE" 2>/dev/null || true
  if [ -n "$STAGED_IGNORE_BAK" ]; then
    mv "$STAGED_IGNORE_BAK" "$STAGED_IGNORE" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# The minarrow path dependency lives outside the repo, so the build context is
# the parent directory holding both checkouts. Docker only reads .dockerignore
# from the context root, so stage this rig's ignore file there for the build.
# The repo directory is not always named `lightstream` (a git worktree carries
# its own name), so the allowlist entry is rewritten to the directory this
# script runs from and the name is passed to the build as `LIGHTSTREAM_DIR`.
REPO_DIR="$(basename "$ROOT")"
if [ -e "$STAGED_IGNORE" ]; then
  STAGED_IGNORE_BAK="$STAGED_IGNORE.bench-ecs.bak"
  mv "$STAGED_IGNORE" "$STAGED_IGNORE_BAK"
fi
sed "s|^!lightstream$|!$REPO_DIR|" "$HERE/.dockerignore" > "$STAGED_IGNORE"

echo "[local] building image $IMAGE"
docker build -f "$HERE/Dockerfile" --build-arg LIGHTSTREAM_DIR="$REPO_DIR" -t "$IMAGE" "$CONTEXT"

# Remove the staged ignore now the build is done; cleanup restores any original.
rm -f "$STAGED_IGNORE"
if [ -n "$STAGED_IGNORE_BAK" ]; then
  mv "$STAGED_IGNORE_BAK" "$STAGED_IGNORE"
  STAGED_IGNORE_BAK=""
fi

docker network create "$NET" >/dev/null 2>&1 || true
# A named volume stands in for the instance's NVMe device so nvme mode has a
# writable /data on the source container.
docker volume create "$VOLUME" >/dev/null 2>&1 || true

for DS in $DATA_SOURCES; do
  echo "[local] shape=$SHAPE data=$DS"
  docker rm -f bench-ecs-source bench-ecs-sink >/dev/null 2>&1 || true

  echo "[local] starting source"
  docker run -d --name bench-ecs-source --network "$NET" -v "$VOLUME:/data" "$IMAGE" \
    bench_ecs_source --shape "$SHAPE" --rows "$ROWS" \
    --dataset-gb "$DATASET_GB" --streams "$STREAMS" --runs "$RUNS" \
    --data-source "$DS" --dataset-dir /data --use-mmap "$USE_MMAP" \
    --flight-bind "0.0.0.0:${FLIGHT_PORT}" --echo-bind "0.0.0.0:${ECHO_PORT}" \
    --ctrl-bind "0.0.0.0:${CTRL_PORT}" \
    --sink-ls-addr "bench-ecs-sink:${LS_PORT}" >/dev/null

  echo "[local] running sink"
  docker run --name bench-ecs-sink --network "$NET" "$IMAGE" \
    bench_ecs_sink --shape "$SHAPE" --rows "$ROWS" \
    --dataset-gb "$DATASET_GB" --streams "$STREAMS" --runs "$RUNS" \
    --data-source "$DS" \
    --source-flight-addr "bench-ecs-source:${FLIGHT_PORT}" \
    --source-echo-addr "bench-ecs-source:${ECHO_PORT}" \
    --source-ctrl-addr "bench-ecs-source:${CTRL_PORT}" \
    --ls-bind "0.0.0.0:${LS_PORT}"

  echo "[local] source log tail:"
  docker logs bench-ecs-source 2>&1 | grep -v flatbuffers | tail -5
done
