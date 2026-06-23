# Lightstream cross-host benchmark on EKS

This benchmark provisions a temporary EKS cluster and runs the Lightstream sender and receiver on separate worker nodes. It records receiver-side throughput across the network between the nodes, then destroys the infrastructure.

## Measurement setup

* **Separate worker nodes.** The receiver Job uses required pod anti-affinity against the sender with `kubernetes.io/hostname` as the topology key. This prevents both pods from being scheduled on the same node.
* **Consistent throughput accounting.** The receiver calculates logical-byte throughput through the same `bench_helpers` code used by the local benchmarks.
* **Isolated runs.** Each run creates resources with a random suffix, avoiding naming conflicts between concurrent runs.
* **Automatic cleanup.** The cluster is destroyed when the script exits unless `KEEP=1` is set.

## Layout

```text
bench/cluster/
  Dockerfile            builds bench_sender and bench_receiver
  terraform/main.tf     creates the VPC, EKS cluster, node group and ECR repository
  k8s/namespace.yaml    creates the benchmark namespace
  k8s/sender.yaml       defines the sender Deployment and Service
  k8s/receiver.yaml     defines the receiver Job and pod anti-affinity
  run.sh                provisions, builds, runs and destroys the benchmark
```

## Prerequisites

* AWS CLI credentials with permission to manage VPC, EKS, EC2 and ECR resources.
* Terraform 1.6 or later.
* `kubectl`.
* Docker.
* `envsubst`, provided by gettext.

Terraform downloads the `terraform-aws-modules/vpc` and `terraform-aws-modules/eks` modules during initialisation.

## Run the benchmark

Run with the default configuration:

```bash
./bench/cluster/run.sh
```

The defaults are:

* Region: `us-east-1`
* Shape: `mixed`
* Rows per batch: `100000`
* Batches: `2000`
* Worker instance type: `c5n.large`

Override the workload or region with environment variables:

```bash
REGION=us-west-2 \
SHAPE=narrow_numeric \
ROWS=1000000 \
BATCHES=500 \
./bench/cluster/run.sh
```

Keep the infrastructure after the benchmark for inspection:

```bash
KEEP=1 ./bench/cluster/run.sh
```

The script prints the sender and receiver pod placement, followed by the receiver result:

```text
RESULT shape=mixed rows=100000 batches=2000 bytes=... elapsed_s=... gib_per_s=...
```

## Cleanup

The EKS control plane and two worker instances continue to incur charges while they are running. The script destroys the infrastructure on exit unless `KEEP=1` is set.

To destroy the infrastructure manually:

```bash
terraform -chdir=bench/cluster/terraform destroy \
  -auto-approve \
  -var region=<region>
```

## Transports

The current sender and receiver use TCP.

The Dockerfile accepts a `FEATURES` build argument, and the Kubernetes manifests receive their workload configuration through `run.sh`. Supporting QUIC, HTTP/2, WebSocket or parallel streams requires adding runtime transport selection to the sender and receiver binaries.
