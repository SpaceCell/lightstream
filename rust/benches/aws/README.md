# AWS A-to-B bench rig

A pair of binaries and a small orchestration script that measure
lightstream TCP throughput between two reachable hosts. Designed for
two EC2 instances in the same VPC, but the same workflow runs on
any pair of machines you can SSH into. The receiver-side throughput
is the headline figure; the sender reports its own throughput as a
cross-check.

## Pieces

| File                  | Role                                                                                     |
|-----------------------|------------------------------------------------------------------------------------------|
| `sender.rs`           | Binds a TCP listener, accepts one connection, streams `--batches` tables.                |
| `receiver.rs`         | Connects to the sender, reads `--batches` tables, times the receive loop.                |
| `Dockerfile`          | Multi-stage build producing a slim image with both binaries on `$PATH`.                  |
| `run.sh`              | SSH-based orchestration: launches the sender in the background, then runs the receiver. |
| `terraform/`          | Terraform module that provisions two EC2 instances ready for the rig. See [terraform/README.md](terraform/README.md). User supplies their own SSH public key; no key material is bundled. |

Both binaries share the bench table shapes from `benches/common/bench_helpers.rs`
so the workload matches the in-process `transport_bench_matrix` bench
for the same `(shape, rows, batches)` triple. Throughput uses the
same logical-bytes denominator.

## Recommended EC2 setup

A defensible cross-host number wants the two endpoints sized for
network throughput rather than compute, in the same Availability
Zone, with placement-group clustering so the path between them is
short. Suggested baseline:

- **Instance type:** `c7gn.large` or `c7gn.xlarge` (network-optimised
  Graviton). For x86, `c7i.large` works similarly. Higher tiers
  (`c7gn.4xlarge` and above) carry more guaranteed bandwidth if you
  want to verify NIC isn't the bottleneck.
- **AZ + placement group:** same AZ, `cluster` placement group, so
  the two hosts are physically close.
- **AMI:** Ubuntu 24.04 LTS or Amazon Linux 2023, x86 or arm64 to
  match your instance choice.
- **Security group:** allow TCP/9001 (or whatever `PORT` you pick)
  ingress from the receiver's security group on the sender's
  security group. SSH (22) from your workstation on both.

The numbers will reflect the instance pair's NIC class, not the
software ceiling. `c7gn.large` is a reasonable lower bound for the
"realistic interservice" measurement; bigger tiers push the ceiling
out for the headline figure.

## Workflow

### 1. Build the binaries

From the `rust/` crate root, either build with cargo on each host:

```bash
cargo build --release --example bench_sender --example bench_receiver --features tcp
```

Or build the container image and push to a registry both instances
can pull from:

```bash
docker build -t lightstream-bench:latest -f benches/aws/Dockerfile .
```

### 2. Drop binaries onto the EC2 instances

If using cargo: `scp target/release/examples/bench_sender
ec2-user@SENDER:/usr/local/bin/` and the same for `bench_receiver`
on the receiver host.

If using docker: `docker pull lightstream-bench:latest` on each
instance, then `docker run --rm --name lsbench --network host
lightstream-bench:latest bench_sender ...` from the wrapper script
(the `run.sh` below assumes plain binaries on `$PATH`; tweak as
needed for container invocation).

### 3. Run the bench

```bash
SENDER_HOST=ec2-user@<sender-public-ip>          \
RECEIVER_HOST=ec2-user@<receiver-public-ip>      \
SENDER_PRIVATE_IP=<sender-private-ipv4>          \
SHAPE=mixed                                      \
ROWS=100000                                      \
BATCHES=2000                                     \
SSH_OPTS="-i ~/.ssh/your-key.pem"                \
benches/aws/run.sh
```

The script:

1. SSHes into the sender host and launches `bench_sender` in the
   background, writing logs to a tmp file.
2. Sleeps briefly, then SSHes into the receiver and runs
   `bench_receiver`, which blocks until all batches arrive.
3. Prints the receiver's `RESULT` line to your local terminal -
   this is the machine-parsable summary: `shape=...
   gib_per_s=...`.
4. Leaves the sender's log on the sender host at `/tmp/
   lightstream_bench_sender_<pid>.log` for cross-checking.

### 4. Capture results

The receiver emits one stdout line per run:

```
RESULT shape=mixed rows=100000 batches=2000 bytes=4503599627 elapsed_s=4.521 gib_per_s=0.927
```

Easy to append into a CSV across runs - for example:

```bash
echo "$(date -u +%FT%TZ),$(uname -m),$(uname -n),$RESULT" >> results.csv
```

## Things that go wrong, and what they mean

- **Receiver-side throughput nowhere near sender's.** Your security
  group is blocking the sender's port, or the receiver's NIC is
  saturated. Confirm with `iperf3` first.
- **Sender-side throughput much higher than receiver's.** The
  receiver is decoding into the kernel send buffer and the syscall
  copy is the bottleneck. Lower the rate (smaller batches), or
  check whether the receiver instance is too small.
- **`RESULT` lines vary by >20% across consecutive runs.** Run a
  warmup iteration first and discard it; AWS instances see neighbour
  contention on shared underlying hardware below the `*.large` tier.
  Move to a `c7gn.xlarge` or larger to reduce variance.

## What this measures vs what it does not

The rig measures wire-level lightstream TCP throughput between two
hosts on the AWS network. It does not measure:

- The QUIC, WebTransport, HTTP/2, WebSocket, or UDS transports.
- Compression on the wire (sender does not pass `--compression`;
  add a flag if you need this comparison).
- Cross-region or cross-AZ network behaviour - the script as
  written assumes same-AZ deployment.
- TLS handshake or per-connection setup overhead - both are
  excluded from the timed region.

Use `cargo bench --bench transport_bench_matrix` locally for the
transport matrix, and the head-to-head Arrow Flight bench at
`benches/arrow/arrow_flight_comparison.rs` for the gRPC comparison number.
