#!/usr/bin/env bash
# Runs the cross-host throughput benchmark on a temporary Amazon ECS (EC2 launch
# type) cluster of two dedicated instances.
#
# The script provisions the infrastructure, builds and pushes the benchmark
# image, then for each data shape and data source runs the source and sink
# tasks on separate container instances. At each stream count the sink
# receives the same workload per transport and times each transfer
# independently, once over Arrow Flight and once over the Lightstream protocol,
# printing one RESULT line per transport per run plus a median line carrying
# min and max, and one round-trip latency line per shape. Each run also emits
# a RESULT metric=gaps line summarising adjacent receiver-visible arrivals and RAW
# lines carrying the arrival series, which this script splits into CSV files
# under the results directory. The infrastructure is destroyed at the end
# unless KEEP=1 is set.
#
# Two data sources are exercised per shape. The `memory` source materialises
# the table in RAM and measures pure transport throughput. The `nvme` source
# writes DATASET_GB gigabytes to the instance's local NVMe as one Arrow IPC
# file per stream. Each transport then evicts the page cache and replays the
# files once cold off the device, followed by RUNS warm replays served from
# the cache, covering both the first-scan and steady-state replay cases.
#
# Both transports run plaintext over the trusted-VPC network. TLS is assumed
# terminated at the ingress boundary and is excluded, so neither side pays
# encryption overhead.
#
# Required tools: AWS CLI v2, Terraform 1.6 or later, Docker and jq.
#
# Configuration:
#
#   REGION             AWS region. Defaults to eu-west-2.
#   AVAILABILITY_ZONE  Availability zone for the rig's subnet, e.g. eu-west-2a.
#                      Defaults to the Terraform value, which picks the
#                      alphabetically-first default-VPC subnet. Set it when
#                      the zone reports InsufficientInstanceCapacity.
#   SHAPES             Space-separated data shapes. Defaults to all four.
#   DATA_SOURCES       Space-separated data sources. Defaults to "memory nvme".
#   ROWS               Rows per table. Defaults to 1000000.
#   DATASET_GB         Workload gigabytes split across the largest stream
#                      count. Defaults to 350, sized to stay cache-resident
#                      under the container memory ceiling for the warm runs.
#   STREAMS            Comma-separated stream counts. Defaults to 1,4,8,16.
#   RUNS               Warm runs per cell. Defaults to 5.
#   MAX_BATCH_SIZE     Nvme replay batch size limit in bytes for Lightstream. 0
#                      replays whole batches. Defaults to 0.
#   USE_MMAP           Set to 1 to replay the nvme files through Lightstream's
#                      mmap reader instead of the buffered file reader. Use 0
#                      when DATASET_GB exceeds the host's RAM. Defaults to 0.
#   FLIGHT_PORT        Source Flight port. Defaults to 9101.
#   ECHO_PORT          Source latency echo port. Defaults to 9102.
#   LS_PORT            Sink Lightstream port. Defaults to 9103.
#   CTRL_PORT          Source control port. Defaults to 9104.
#   INSTANCE_TYPE      Container-instance type. Defaults to the Terraform value.
#   TASK_MEMORY        Container memory limit in MiB. Defaults to the Terraform
#                      value, sized for the default instance type. Lower it
#                      together with INSTANCE_TYPE and DATASET_GB for smaller
#                      hosts.
#   KEEP               Set to 1 to retain the infrastructure after the run.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"        # the lightstream repo root
CONTEXT="$(cd "$ROOT/.." && pwd)"           # parent dir holding lightstream + minarrow
TF="$HERE/terraform"

REGION="${REGION:-eu-west-2}"
SHAPES="${SHAPES:-mixed narrow_numeric string_heavy wide}"
DATA_SOURCES="${DATA_SOURCES:-memory nvme}"
ROWS="${ROWS:-1000000}"
DATASET_GB="${DATASET_GB:-350}"
STREAMS="${STREAMS:-1,4,8,16}"
RUNS="${RUNS:-5}"
MAX_BATCH_SIZE="${MAX_BATCH_SIZE:-8388608}"
USE_MMAP="${USE_MMAP:-0}"
FLIGHT_PORT="${FLIGHT_PORT:-9101}"
ECHO_PORT="${ECHO_PORT:-9102}"
LS_PORT="${LS_PORT:-9103}"
CTRL_PORT="${CTRL_PORT:-9104}"
KEEP="${KEEP:-0}"

TF_VARS=(-var "region=${REGION}" \
         -var "flight_port=${FLIGHT_PORT}" \
         -var "echo_port=${ECHO_PORT}" \
         -var "ls_port=${LS_PORT}" \
         -var "ctrl_port=${CTRL_PORT}")
if [ -n "${INSTANCE_TYPE:-}" ]; then
  TF_VARS+=(-var "instance_type=${INSTANCE_TYPE}")
fi
if [ -n "${AVAILABILITY_ZONE:-}" ]; then
  TF_VARS+=(-var "availability_zone=${AVAILABILITY_ZONE}")
fi
if [ -n "${TASK_MEMORY:-}" ]; then
  TF_VARS+=(-var "task_memory=${TASK_MEMORY}")
fi

teardown() {
  if [ "$KEEP" = "1" ]; then
    echo "[run] KEEP=1 set, leaving the infrastructure up. Destroy later with:"
    echo "      terraform -chdir=$TF destroy -auto-approve -var region=$REGION"
    return
  fi
  echo "[run] destroying infrastructure"
  if ! terraform -chdir="$TF" destroy -auto-approve -input=false "${TF_VARS[@]}"; then
    echo "[run] WARNING: terraform destroy failed. The infrastructure may still be"
    echo "      running and billing. Destroy it manually with:"
    echo "      terraform -chdir=$TF destroy -auto-approve -var region=$REGION"
  fi
}
# The Dockerfile's cache mounts require BuildKit.
export DOCKER_BUILDKIT=1

# The image is built before any infrastructure exists, so a failed build costs
# nothing and the instances only start billing once there is an image to run.
TAG="$(git -C "$ROOT" rev-parse --short HEAD)"
LOCAL_IMAGE="lightstream-ecs-bench:${TAG}"

# The workspace depends on minarrow through a `path = "../minarrow"` sibling
# checkout, which lives outside the repo. The Dockerfile therefore expects the
# build context to be the parent directory that holds both `lightstream` and
# `minarrow`. Docker only reads `.dockerignore` from the context root, so stage
# this rig's ignore file there for the build and remove it afterwards.`
REPO_DIR="$(basename "$ROOT")"
STAGED_IGNORE="$CONTEXT/.dockerignore"
STAGED_IGNORE_BAK=""
if [ -e "$STAGED_IGNORE" ]; then
  STAGED_IGNORE_BAK="$STAGED_IGNORE.bench-ecs.bak"
  mv "$STAGED_IGNORE" "$STAGED_IGNORE_BAK"
fi
sed "s|^!lightstream$|!$REPO_DIR|" "$HERE/.dockerignore" > "$STAGED_IGNORE"
restore_ignore() {
  rm -f "$STAGED_IGNORE"
  if [ -n "$STAGED_IGNORE_BAK" ]; then
    mv "$STAGED_IGNORE_BAK" "$STAGED_IGNORE"
    STAGED_IGNORE_BAK=""
  fi
}
trap restore_ignore EXIT

echo "[run] building image $LOCAL_IMAGE"
docker build -f "$HERE/Dockerfile" --build-arg LIGHTSTREAM_DIR="$REPO_DIR" -t "$LOCAL_IMAGE" "$CONTEXT"
restore_ignore
trap teardown EXIT

# Confirm the built binaries accept the arguments this script passes before
# any infrastructure exists, so a stale build cannot reach the cluster.
if ! docker run --rm "$LOCAL_IMAGE" bench_ecs_source --help | grep -q -- '--max-batch-size'; then
  echo "[run] built image does not support --max-batch-size: stale binaries" >&2
  exit 1
fi
if ! docker run --rm "$LOCAL_IMAGE" bench_ecs_source --help | grep -q -- '--use-mmap'; then
  echo "[run] built image does not support --use-mmap: stale binaries" >&2
  exit 1
fi

echo "[run] provisioning ECS infrastructure (region=$REGION)"
terraform -chdir="$TF" init -input=false
terraform -chdir="$TF" apply -auto-approve -input=false "${TF_VARS[@]}"

CLUSTER="$(terraform -chdir="$TF" output -raw cluster_name)"
ECR="$(terraform -chdir="$TF" output -raw ecr_repository_url)"
REGISTRY="${ECR%/*}"
SOURCE_IP="$(terraform -chdir="$TF" output -raw source_private_ip)"
SINK_IP="$(terraform -chdir="$TF" output -raw sink_private_ip)"
SOURCE_FAMILY="$(terraform -chdir="$TF" output -raw source_task_family)"
SINK_FAMILY="$(terraform -chdir="$TF" output -raw sink_task_family)"
SINK_LOG_GROUP="$(terraform -chdir="$TF" output -raw sink_log_group)"
SOURCE_LOG_GROUP="$(terraform -chdir="$TF" output -raw source_log_group)"

echo "[run] pushing image"
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"
IMAGE="$ECR:$TAG"
docker tag "$LOCAL_IMAGE" "$IMAGE"
# Registry pushes can drop mid-transfer on transient network errors. A retry
# resumes from the layers already uploaded, so failing the run on the first
# error would forfeit a full provision and teardown cycle for nothing.
for attempt in 1 2 3; do
  if docker push "$IMAGE"; then
    break
  fi
  if [ "$attempt" -eq 3 ]; then
    echo "[run] docker push failed after $attempt attempts" >&2
    exit 1
  fi
  echo "[run] docker push attempt $attempt failed - retrying"
  sleep 5
done

# Re-apply so the task definitions carry the freshly pushed image reference.
terraform -chdir="$TF" apply -auto-approve -input=false "${TF_VARS[@]}" -var "image_ref=${IMAGE}"

# Wait for both container instances to register with the cluster before running
# any tasks. Two instances are expected: one source, one sink.
echo "[run] waiting for container instances to register with $CLUSTER"
for _ in $(seq 1 60); do
  count="$(aws ecs list-container-instances --cluster "$CLUSTER" --region "$REGION" \
    --query 'length(containerInstanceArns)' --output text 2>/dev/null || echo 0)"
  [ "$count" = "2" ] && break
  sleep 5
done
if [ "${count:-0}" != "2" ]; then
  echo "[run] ERROR: expected 2 registered container instances, found ${count:-0}" >&2
  exit 1
fi

RESULTS_DIR="${RESULTS_DIR:-$HERE/results/$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$RESULTS_DIR"
echo "[run] saving per-shape per-data-source logs under $RESULTS_DIR"

# Emit a container command-override overrides file for aws ecs run-task.
# $1 container name, $2.. the command argv.
run_task() {
  local family="$1"; shift
  local container="$1"; shift
  local role="$1"; shift
  local cmd_json
  cmd_json="$(printf '%s\n' "$@" | jq -R . | jq -s .)"
  aws ecs run-task \
    --cluster "$CLUSTER" \
    --region "$REGION" \
    --launch-type EC2 \
    --task-definition "$family" \
    --count 1 \
    --placement-constraints "type=memberOf,expression=attribute:bench_role == ${role}" \
    --overrides "{\"containerOverrides\":[{\"name\":\"${container}\",\"command\":${cmd_json}}]}" \
    --query 'tasks[0].taskArn' --output text
}

# Split a sink log's RAW lines into one CSV of arrival offsets per pass under
# $2, named <shape>-<data>-<protocol>-s<streams>[-<cache>][-run<run>].csv.
extract_series() {
  local log="$1" outdir="$2"
  mkdir -p "$outdir"
  awk -v outdir="$outdir" '/^RAW /{
    proto = ""; shape = ""; ds = ""; streams = ""; cache = ""; run = ""; vals = "";
    for (i = 2; i <= NF; i++) {
      split($i, kv, "=");
      if (kv[1] == "protocol") proto = kv[2];
      else if (kv[1] == "shape") shape = kv[2];
      else if (kv[1] == "data") ds = kv[2];
      else if (kv[1] == "streams") streams = kv[2];
      else if (kv[1] == "cache") cache = kv[2];
      else if (kv[1] == "run") run = kv[2];
      else if (kv[1] == "values") vals = kv[2];
    }
    name = shape "-" ds "-" proto "-s" streams;
    if (cache != "") name = name "-" cache;
    if (run != "") name = name "-run" run;
    f = outdir "/" name ".csv";
    n = split(vals, a, ",");
    for (j = 1; j <= n; j++) print a[j] >> f;
    close(f);
  }' "$log"
}

# Wait for a task to stop. `aws ecs wait tasks-stopped` gives up after ~40
# minutes (100 attempts x 6s), which is shorter than nvme dataset generation
# for large shapes. Retry the wait until the task is actually STOPPED so a long
# generation phase does not end the run.
wait_task_stopped() {
  local task_arn="$1"
  while true; do
    aws ecs wait tasks-stopped --cluster "$CLUSTER" --region "$REGION" --tasks "$task_arn" 2>/dev/null || true
    local status
    status="$(aws ecs describe-tasks --cluster "$CLUSTER" --region "$REGION" \
      --tasks "$task_arn" --query 'tasks[0].lastStatus' --output text 2>/dev/null || echo UNKNOWN)"
    [ "$status" = "STOPPED" ] && break
    echo "[run] sink task still $status, continuing to wait"
    sleep 10
  done
}

# Print why a task stopped before teardown destroys the evidence: the ECS
# stopped reason, the container's own reason and exit code, and the tail of
# its log stream.
diagnose_task() {
  local task_arn="$1" container="$2" log_group="$3"
  local task_id="${task_arn##*/}"
  aws ecs describe-tasks --cluster "$CLUSTER" --region "$REGION" --tasks "$task_arn" \
    --query 'tasks[0].{stoppedReason:stoppedReason,containerReason:containers[0].reason,exitCode:containers[0].exitCode}' \
    --output table 2>/dev/null || true
  echo "[run] last log lines for $container task $task_id"
  aws logs get-log-events --log-group-name "$log_group" \
    --log-stream-name "$container/$container/$task_id" \
    --region "$REGION" --limit 50 --output json 2>/dev/null \
    | jq -r '.events[].message' || true
}

for SHAPE in $SHAPES; do
  for DS in $DATA_SOURCES; do
    echo "[run] shape=$SHAPE data=$DS rows=$ROWS dataset_gb=$DATASET_GB streams=$STREAMS runs=$RUNS max_batch_size=$MAX_BATCH_SIZE use_mmap=$USE_MMAP"

    # Source starts first and listens; the sink then drives both transfers.
    SOURCE_ARN="$(run_task "$SOURCE_FAMILY" source source \
      bench_ecs_source \
      --shape "$SHAPE" --rows "$ROWS" \
      --dataset-gb "$DATASET_GB" --streams "$STREAMS" --runs "$RUNS" \
      --data-source "$DS" --dataset-dir /data \
      --max-batch-size "$MAX_BATCH_SIZE" --use-mmap "$USE_MMAP" \
      --flight-bind "0.0.0.0:${FLIGHT_PORT}" --echo-bind "0.0.0.0:${ECHO_PORT}" \
      --ctrl-bind "0.0.0.0:${CTRL_PORT}" \
      --sink-ls-addr "${SINK_IP}:${LS_PORT}")"
    echo "[run] source task: $SOURCE_ARN"

    echo "[run] waiting for source task to reach RUNNING"
    if ! aws ecs wait tasks-running --cluster "$CLUSTER" --region "$REGION" --tasks "$SOURCE_ARN"; then
      echo "[run] source task stopped before reaching RUNNING" >&2
      diagnose_task "$SOURCE_ARN" source "$SOURCE_LOG_GROUP"
      exit 1
    fi

    SINK_ARN="$(run_task "$SINK_FAMILY" sink sink \
      bench_ecs_sink \
      --shape "$SHAPE" --rows "$ROWS" \
      --dataset-gb "$DATASET_GB" --streams "$STREAMS" --runs "$RUNS" \
      --data-source "$DS" \
      --max-batch-size "$MAX_BATCH_SIZE" \
      --source-flight-addr "${SOURCE_IP}:${FLIGHT_PORT}" \
      --source-echo-addr "${SOURCE_IP}:${ECHO_PORT}" \
      --source-ctrl-addr "${SOURCE_IP}:${CTRL_PORT}" \
      --ls-bind "0.0.0.0:${LS_PORT}")"
    echo "[run] sink task: $SINK_ARN"

    echo "[run] waiting for sink task to stop"
    wait_task_stopped "$SINK_ARN"

    # Fail loudly if the sink container exited non-zero.
    EXIT_CODE="$(aws ecs describe-tasks --cluster "$CLUSTER" --region "$REGION" \
      --tasks "$SINK_ARN" \
      --query 'tasks[0].containers[0].exitCode' --output text)"
    REASON="$(aws ecs describe-tasks --cluster "$CLUSTER" --region "$REGION" \
      --tasks "$SINK_ARN" \
      --query 'tasks[0].stoppedReason' --output text 2>/dev/null || true)"

    # Fetch the sink task's log stream. The awslogs stream name is
    # <prefix>/<container>/<task-id>. get-log-events returns about 1 MB per
    # page, so follow nextForwardToken until it repeats to capture the whole
    # stream.
    SINK_TASK_ID="${SINK_ARN##*/}"
    SINK_STREAM="sink/sink/${SINK_TASK_ID}"
    echo "[run] collecting sink logs for shape=$SHAPE data=$DS"
    SINK_LOG="$RESULTS_DIR/$SHAPE-$DS.log"
    : > "$SINK_LOG"
    TOKEN=""
    while true; do
      FETCH_ARGS=(--log-group-name "$SINK_LOG_GROUP" \
                  --log-stream-name "$SINK_STREAM" \
                  --region "$REGION" --start-from-head --output json)
      [ -n "$TOKEN" ] && FETCH_ARGS+=(--next-token "$TOKEN")
      PAGE="$(aws logs get-log-events "${FETCH_ARGS[@]}" 2>/dev/null)" || break
      printf '%s' "$PAGE" | jq -r '.events[].message' >> "$SINK_LOG"
      NEXT="$(printf '%s' "$PAGE" | jq -r '.nextForwardToken')"
      [ "$NEXT" = "$TOKEN" ] && break
      TOKEN="$NEXT"
    done
    extract_series "$SINK_LOG" "$RESULTS_DIR/series"

    # Stop the source task before the next combination so it rebuilds its batch.
    # Wait for it to reach STOPPED so its host ports are released before the next
    # source binds them, since the source holds its listeners until it exits.
    aws ecs stop-task --cluster "$CLUSTER" --region "$REGION" --task "$SOURCE_ARN" \
      --query 'task.taskArn' --output text >/dev/null || true
    aws ecs wait tasks-stopped --cluster "$CLUSTER" --region "$REGION" \
      --tasks "$SOURCE_ARN" 2>/dev/null || true

    if [ "$EXIT_CODE" != "0" ]; then
      echo "[run] ERROR: sink container for shape=$SHAPE data=$DS exited with code $EXIT_CODE" >&2
      echo "[run]        stoppedReason: $REASON" >&2
      echo "[run]        see $RESULTS_DIR/$SHAPE-$DS.log" >&2
      exit 1
    fi
  done
done

SUMMARY="$RESULTS_DIR/summary.txt"
grep -h -E '^RESULT' "$RESULTS_DIR"/*.log > "$SUMMARY" 2>/dev/null || true

echo
echo "[run] ============ cross-host throughput results ============"
if [ -s "$SUMMARY" ]; then
  cat "$SUMMARY"
else
  echo "[run] no RESULT lines captured - see $RESULTS_DIR"
fi
echo "[run] logs, summary and arrival-series CSVs saved under: $RESULTS_DIR"
