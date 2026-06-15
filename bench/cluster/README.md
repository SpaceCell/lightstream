# Lightstream cross-host bench - ephemeral EKS

Local benches measure a software ceiling. A deployed service crosses a
real NIC. This rig measures the second case: it stands up a throwaway
EKS cluster, schedules the lightstream sender and receiver on two
separate nodes, sends the bench workload between them, harvests the
receiver's throughput, then destroys it.

The number it produces is what a service actually sees on the wire,
inclusive of the network hop.

## Why the result is credible

- **Real network hop.** The receiver Job carries a `required` pod
  anti-affinity against the sender on `topologyKey: kubernetes.io/hostname`.
  Kubernetes will not co-schedule them, so the two pods land on distinct
  nodes and traffic crosses a real NIC rather than a shared loopback.
- **Same throughput accounting as the in-process benches.** The
  receiver reports logical-byte throughput via the same
  `bench_helpers` path used everywhere else, so the cross-host number is
  comparable to the loopback one.
- **Ephemeral and reproducible.** Every run provisions a fresh cluster
  with a random-suffixed name, runs, and destroys. Nothing lingers, and
  two runs do not collide.

## Layout

```
bench/cluster/
  Dockerfile            multi-stage build of bench_sender / bench_receiver
  terraform/main.tf     throwaway VPC, EKS cluster, 2-node group, ECR repo
  k8s/namespace.yaml    bench namespace
  k8s/sender.yaml       sender Deployment + ClusterIP Service
  k8s/receiver.yaml     receiver Job with anti-affinity to the sender
  run.sh                end-to-end: apply -> build/push -> run -> destroy
```

## Prerequisites

- `aws` CLI, authenticated with rights to create VPC, EKS, EC2, and ECR.
- `terraform` >= 1.6, `kubectl`, `docker`, and `envsubst` (gettext).

`terraform init` pulls the `terraform-aws-modules/vpc` and
`terraform-aws-modules/eks` modules.

## Run

```bash
# Defaults: us-east-1, mixed shape, 100k rows, 2000 batches, c5n.large nodes
./bench/cluster/run.sh

# Tune the workload and region
REGION=us-west-2 SHAPE=narrow_numeric ROWS=1000000 BATCHES=500 \
  ./bench/cluster/run.sh

# Leave the cluster up to inspect (destroy it yourself afterwards)
KEEP=1 ./bench/cluster/run.sh
```

`run.sh` prints the pod placement (proving the two pods are on different
nodes) and the receiver's `RESULT ...` line, for example:

```
RESULT shape=mixed rows=100000 batches=2000 bytes=... elapsed_s=... gib_per_s=...
```

The cluster is destroyed on exit unless `KEEP=1`.

## Cost and cleanup

An EKS control plane bills per hour and the node group runs two
instances. A single run is minutes, but a forgotten `KEEP=1` cluster
keeps billing. If a run is interrupted before cleanup, destroy by hand:

```bash
terraform -chdir=bench/cluster/terraform destroy -auto-approve -var region=<region>
```

## Transports

The sender and receiver speak TCP, so the rig measures TCP across the
network. The `Dockerfile` takes a `FEATURES` build arg and the manifests
pass the workload through env, so extending to the other transports
(QUIC, HTTP/2, WebSocket, and the parallel multi-stream path) is a matter
of teaching the sender and receiver to select a transport at runtime.
That lands alongside the parallel-streams API.
