#!/usr/bin/env bash
# Runs the Lightstream cross-host benchmark on a temporary EKS cluster.
#
# The script provisions the cluster, builds and pushes the benchmark image,
# runs the sender and receiver on separate worker nodes, prints the receiver
# result and destroys the infrastructure.
#
# Required tools: AWS CLI, Terraform 1.6 or later, kubectl, Docker and envsubst.
#
# Configuration:
#
#   REGION         AWS region. Defaults to us-east-1.
#   SHAPE          Data shape: mixed, narrow_numeric, string_heavy or wide.
#   ROWS           Number of rows per batch. Defaults to 100000.
#   BATCHES        Number of batches to send. Defaults to 2000.
#   PORT           Benchmark port. Defaults to 9001.
#   NAMESPACE      Kubernetes namespace. Defaults to lightstream-bench.
#   INSTANCE_TYPE  Worker node instance type. Defaults to the Terraform value.
#   KEEP           Set to 1 to retain the infrastructure after the run.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
TF="$HERE/terraform"

REGION="${REGION:-us-east-1}"
SHAPE="${SHAPE:-mixed}"
ROWS="${ROWS:-100000}"
BATCHES="${BATCHES:-2000}"
PORT="${PORT:-9001}"
NAMESPACE="${NAMESPACE:-lightstream-bench}"
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
  terraform -chdir="$TF" destroy -auto-approve -input=false "${TF_VARS[@]}" || true
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

echo "[run] scheduling sender and receiver (shape=$SHAPE rows=$ROWS batches=$BATCHES)"
export NAMESPACE IMAGE PORT SHAPE ROWS BATCHES
envsubst < "$HERE/k8s/namespace.yaml" | kubectl apply -f -
envsubst < "$HERE/k8s/sender.yaml"    | kubectl apply -f -
kubectl -n "$NAMESPACE" rollout status deploy/bench-sender --timeout=180s

# Allow the sender time to start listening before creating the receiver.
sleep 3

envsubst < "$HERE/k8s/receiver.yaml" | kubectl apply -f -

echo "[run] waiting for the receiver to finish"
kubectl -n "$NAMESPACE" wait --for=condition=complete job/bench-receiver --timeout=900s \
  || kubectl -n "$NAMESPACE" wait --for=condition=failed job/bench-receiver --timeout=10s || true

echo "[run] pod placement (sender and receiver must be on different nodes):"
kubectl -n "$NAMESPACE" get pods -o wide

echo "[run] receiver result:"
kubectl -n "$NAMESPACE" logs job/bench-receiver | grep -E '^RESULT' \
  || kubectl -n "$NAMESPACE" logs job/bench-receiver | tail -10
