#!/usr/bin/env bash
# Like-for-like local validation of the cross-host benchmark on a kind cluster.
#
# Runs the real source and sink manifests on a two-worker local Kubernetes
# cluster, so anti-affinity places the pods on different (containerised) nodes
# exactly as on EKS, minus the real cross-host network. This catches manifest,
# Service, Job and image errors before provisioning EKS.
#
# The kind nodes share one host, so the figures are not representative. Use
# run.sh on EKS for cross-node numbers. This check is for correctness.
#
# Required tools: docker, kind, kubectl, envsubst.
#
# Configuration mirrors run.sh, plus CLUSTER for the kind cluster name. Set
# KEEP=1 to retain the cluster after the run.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

CLUSTER="${CLUSTER:-lightstream-vpc-bench}"
IMAGE="${IMAGE:-lightstream-vpc-bench:local}"
NAMESPACE="${NAMESPACE:-lightstream-bench}"
SHAPES="${SHAPES:-mixed}"
ROWS="${ROWS:-100000}"
BATCHES_PER_STREAM="${BATCHES_PER_STREAM:-100}"
STREAMS="${STREAMS:-4,8}"
FLIGHT_PORT="${FLIGHT_PORT:-9101}"
ECHO_PORT="${ECHO_PORT:-9102}"
LS_PORT="${LS_PORT:-9103}"
KEEP="${KEEP:-0}"

# The local image is loaded into kind, not pulled from a registry.
export NAMESPACE IMAGE ROWS BATCHES_PER_STREAM STREAMS FLIGHT_PORT ECHO_PORT LS_PORT
export IMAGE_PULL_POLICY=Never

teardown() {
  if [ "$KEEP" = "1" ]; then
    echo "[kind] KEEP=1 set, leaving cluster '$CLUSTER' up. Delete later with:"
    echo "      kind delete cluster --name $CLUSTER"
    return
  fi
  echo "[kind] deleting cluster $CLUSTER"
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
}
trap teardown EXIT

echo "[kind] creating two-worker cluster $CLUSTER"
kind create cluster --name "$CLUSTER" --config - <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
EOF

echo "[kind] building image $IMAGE"
docker build -f "$HERE/Dockerfile" -t "$IMAGE" "$ROOT"

echo "[kind] loading image into the cluster"
kind load docker-image "$IMAGE" --name "$CLUSTER"

envsubst < "$HERE/k8s/namespace.yaml" | kubectl apply -f -

placement_shown=0
for SHAPE in $SHAPES; do
  export SHAPE
  echo "[kind] shape=$SHAPE rows=$ROWS batches_per_stream=$BATCHES_PER_STREAM streams=$STREAMS"

  envsubst < "$HERE/k8s/source.yaml" | kubectl apply -f -
  kubectl -n "$NAMESPACE" rollout status deploy/bench-vpc-source --timeout=120s
  envsubst < "$HERE/k8s/sink.yaml" | kubectl apply -f -

  kubectl -n "$NAMESPACE" wait --for=condition=complete job/bench-vpc-sink --timeout=600s \
    || kubectl -n "$NAMESPACE" wait --for=condition=failed job/bench-vpc-sink --timeout=10s || true

  if [ "$placement_shown" = "0" ]; then
    echo "[kind] pod placement (source and sink on different nodes):"
    kubectl -n "$NAMESPACE" get pods -o wide
    placement_shown=1
  fi

  echo "[kind] results for shape=$SHAPE:"
  kubectl -n "$NAMESPACE" logs job/bench-vpc-sink | grep -E '^RESULT' \
    || kubectl -n "$NAMESPACE" logs job/bench-vpc-sink | tail -15

  kubectl -n "$NAMESPACE" delete -f <(envsubst < "$HERE/k8s/sink.yaml") --ignore-not-found
  kubectl -n "$NAMESPACE" delete -f <(envsubst < "$HERE/k8s/source.yaml") --ignore-not-found
done

echo "[kind] done"
