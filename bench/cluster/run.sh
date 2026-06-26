#!/usr/bin/env bash
# Runs the cross-host throughput benchmark on a temporary EKS cluster.
#
# The script provisions the cluster, builds and pushes the benchmark image, then
# for each data shape runs the source and sink on separate worker nodes. At each
# stream count the sink receives the same workload twice and times each transfer
# independently, once over Arrow Flight and once over the Lightstream protocol
# parallel reader, printing one RESULT line per transport. The infrastructure is
# destroyed at the end.
#
# Both transports run plaintext over the trusted-VPC pod-to-pod network. TLS is
# assumed terminated at the ingress boundary and is excluded, so neither side
# pays encryption overhead.
#
# Required tools: AWS CLI, Terraform 1.6 or later, kubectl, Docker and envsubst.
#
# Configuration:
#
#   REGION             AWS region. Defaults to us-east-1.
#   SHAPES             Space-separated data shapes. Defaults to all four.
#   ROWS               Rows per table. Defaults to 1000000.
#   BATCHES_PER_STREAM Tables per stream per cell. Defaults to 500.
#   STREAMS            Comma-separated stream counts. Defaults to 4,8,16.
#   FLIGHT_PORT        Source Flight port. Defaults to 9101.
#   ECHO_PORT         Source latency echo port. Defaults to 9102.
#   LS_PORT            Sink Lightstream port. Defaults to 9103.
#   NAMESPACE          Kubernetes namespace. Defaults to lightstream-bench.
#   INSTANCE_TYPE      Worker node instance type. Defaults to the Terraform value.
#   KEEP               Set to 1 to retain the infrastructure after the run.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
TF="$HERE/terraform"

REGION="${REGION:-us-east-1}"
SHAPES="${SHAPES:-mixed narrow_numeric string_heavy wide}"
ROWS="${ROWS:-1000000}"
BATCHES_PER_STREAM="${BATCHES_PER_STREAM:-500}"
STREAMS="${STREAMS:-4,8,16}"
FLIGHT_PORT="${FLIGHT_PORT:-9101}"
ECHO_PORT="${ECHO_PORT:-9102}"
LS_PORT="${LS_PORT:-9103}"
NAMESPACE="${NAMESPACE:-lightstream-bench}"
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-Always}"
KEEP="${KEEP:-0}"

TF_VARS=(-var "region=${REGION}")
if [ -n "${INSTANCE_TYPE:-}" ]; then
  TF_VARS+=(-var "instance_type=${INSTANCE_TYPE}")
fi

teardown() {
  if [ "$KEEP" = "1" ]; then
    echo "[run] KEEP=1 set, leaving the cluster up. Destroy later with:"
    echo "      terraform -chdir=$TF destroy -auto-approve -var region=$REGION"
    return
  fi
  echo "[run] destroying cluster"
  if ! terraform -chdir="$TF" destroy -auto-approve -input=false "${TF_VARS[@]}"; then
    echo "[run] WARNING: terraform destroy failed. The cluster may still be running"
    echo "      and billing. Destroy it manually with:"
    echo "      terraform -chdir=$TF destroy -auto-approve -var region=$REGION"
  fi
}
trap teardown EXIT

echo "[run] provisioning EKS cluster (region=$REGION)"
terraform -chdir="$TF" init -input=false
terraform -chdir="$TF" apply -auto-approve -input=false "${TF_VARS[@]}"

CLUSTER="$(terraform -chdir="$TF" output -raw cluster_name)"
ECR="$(terraform -chdir="$TF" output -raw ecr_repository_url)"
REGISTRY="${ECR%/*}"

echo "[run] configuring kubectl for $CLUSTER"
aws eks update-kubeconfig --name "$CLUSTER" --region "$REGION"

echo "[run] building and pushing image"
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"
IMAGE="$ECR:$(git -C "$ROOT" rev-parse --short HEAD)"
docker build -f "$HERE/Dockerfile" -t "$IMAGE" "$ROOT"
docker push "$IMAGE"

export NAMESPACE IMAGE ROWS BATCHES_PER_STREAM STREAMS FLIGHT_PORT ECHO_PORT LS_PORT IMAGE_PULL_POLICY
envsubst < "$HERE/k8s/namespace.yaml" | kubectl apply -f -

RESULTS_FILE="${RESULTS_FILE:-$HERE/results-$(date +%Y%m%d-%H%M%S).txt}"
: > "$RESULTS_FILE"
echo "[run] streaming results live, and saving to $RESULTS_FILE"
placement_shown=0

for SHAPE in $SHAPES; do
  export SHAPE
  echo "[run] shape=$SHAPE rows=$ROWS batches_per_stream=$BATCHES_PER_STREAM streams=$STREAMS"

  # Source schedules first so the sink's anti-affinity places it on another node.
  envsubst < "$HERE/k8s/source.yaml" | kubectl apply -f -
  kubectl -n "$NAMESPACE" rollout status deploy/bench-vpc-source --timeout=300s
  envsubst < "$HERE/k8s/sink.yaml" | kubectl apply -f -

  # Wait for the sink pod, then stream its output live to the terminal and the
  # results file while it runs, so the RESULT lines appear as they are produced.
  sink_pod=""
  for _ in $(seq 1 60); do
    sink_pod="$(kubectl -n "$NAMESPACE" get pods -l role=sink \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    [ -n "$sink_pod" ] && break
    sleep 2
  done
  echo "[run] streaming sink output (shape=$SHAPE):"
  if [ -n "$sink_pod" ]; then
    kubectl -n "$NAMESPACE" wait --for=condition=ready "pod/$sink_pod" --timeout=300s 2>/dev/null || true
    kubectl -n "$NAMESPACE" logs -f "pod/$sink_pod" 2>&1 | tee -a "$RESULTS_FILE" || true
  fi
  kubectl -n "$NAMESPACE" wait --for=condition=complete job/bench-vpc-sink --timeout=120s \
    || kubectl -n "$NAMESPACE" wait --for=condition=failed job/bench-vpc-sink --timeout=10s || true

  if [ "$placement_shown" = "0" ]; then
    echo "[run] pod placement (source and sink must be on different nodes):"
    kubectl -n "$NAMESPACE" get pods -o wide
    placement_shown=1
  fi

  # Clear the workloads before the next shape so the source rebuilds its batch.
  kubectl -n "$NAMESPACE" delete -f <(envsubst < "$HERE/k8s/sink.yaml") --ignore-not-found
  kubectl -n "$NAMESPACE" delete -f <(envsubst < "$HERE/k8s/source.yaml") --ignore-not-found
done

echo
echo "[run] ============ cross-host throughput results ============"
grep -E '^RESULT' "$RESULTS_FILE" || echo "[run] no results captured - see $RESULTS_FILE"
echo "[run] full results saved to: $RESULTS_FILE"
