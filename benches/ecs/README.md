# Lightstream cross-host benchmark on Amazon ECS

This benchmark provisions two dedicated EC2 instances orchestrated by Amazon ECS
(EC2 launch type) and runs the same workload over Arrow Flight and Lightstream
TCP between them. The sink drives both transfers, records receiver-side
throughput for each transport and the host-to-host round-trip latency, then the
infrastructure is destroyed.

Each shape is run over two data sources. The `memory` source materialises the
table in RAM and measures transport throughput without hard disk storage.
The `nvme` source writes a dataset of `DATASET_GB` gigabytes to the instance's
local NVMe as one Arrow IPC file per stream. Each transport then evicts the
page cache and replays the files once cold off the device, followed by `RUNS`
warm replays served from the cache (via mmap), measuring the pattern where
a host streams a large recorded dataset to a remote consumer for multiple replay runs.
Under `nvme` Lightstream replays through its zero-copy mmap reader and Arrow Flight
reads the same files through the `arrow` IPC file reader.

## Measurement setup

| Aspect | Setup |
| --- | --- |
| Instances | Two dedicated EC2 container instances. `bench_role` attribute constraints keep the source and sink on separate hosts. |
| Network path | One `cluster` placement group in a single AZ. Host networking, so traffic flows over the instances' private IPs. |
| Data sources | `memory` resends one RAM-resident table, bumping column reference counts per send. `nvme` replays one Arrow IPC file per stream off local NVMe. |
| Cache passes (nvme) | Per transport: evict, one `cache=cold` pass off the device, then `RUNS` `cache=warm` passes from the page cache. A control pulse from sink to source sequences the phases. |
| Verified delivery | nvme batches carry a global sequence in their first column. The sink requires every stream ordered and complete with the expected row counts. `memory` checks row totals. |
| Transports | Arrow Flight, N concurrent `DoGet` streams, against Lightstream TCP, one connection or N merged. Merge order is global write order under `memory` and arrival order under `nvme`. |
| Runs and stats | `RUNS` (default 5) timed runs per transport per cell. Median with min and max, `metric=gaps` percentiles labelled with sample counts, `RAW` arrival series split into CSVs. |
| Transport order | `memory` interleaves the transports run by run. `nvme` runs each transport as a block so neither inherits the other's page cache. |
| Cleanup | Infrastructure destroyed on exit unless `KEEP=1` is set. |


## Layout

```text
benches/ecs/
  Dockerfile            builds bench_ecs_source and bench_ecs_sink
  .dockerignore         build-context ignore rules (staged into the context root)
  terraform/main.tf     creates the ECS cluster, two instances, ECR and IAM
  run.sh                provisions, builds, runs and destroys the benchmark
  run-local-docker.sh   local two-container smoke test without AWS
  source.rs             the source binary (built as example bench_ecs_source)
  sink.rs               the sink binary (built as example bench_ecs_sink)
```

## Hardware rationale

The default instance type is `i3en.12xlarge`, chosen so the host never limits
what the software can do. Three requirements drive the choice:

* **At least 16 vCPUs.** The largest default cell runs 16 streams. With fewer
  cores than streams the run becomes scheduler-bound rather than
  transport-bound. `i3en.12xlarge` has 48 vCPUs.
* **Guaranteed network bandwidth.** `i3en.12xlarge` provides 50 Gbps
  (6.25 GB/s), sustained for the whole run where smaller types offer a burst
  allowance that depletes under a continuous transfer. AWS also caps each TCP
  flow at about 10 Gbps within a cluster placement group, so single-stream
  cells are flow-limited on every instance type and only the multi-stream
  cells aggregate toward the instance figure.
* **Local NVMe faster in aggregate than the network.** For the cold nvme
  passes the device read rate must exceed the network rate, or the disk
  becomes the limit rather than the transport. `i3en.12xlarge` has 4 x
  7500 GB NVMe reading around 8 GB/s in aggregate, above the 6.25 GB/s the
  network carries.
* **RAM covering the warm dataset.** The warm nvme passes serve the dataset
  from the page cache, so the default 350 GB dataset is sized to stay
  resident within `i3en.12xlarge`'s 384 GiB and the container's 350 GiB
  memory ceiling.

## Disclosure / Acknowledgements
Obviously - this a 'perfect in-memory setting' on an uncontended network.
Real-world throughput will vary based on workload actuals. The repeated runs
in the benchmark from disk work with page-faulted mmap reads that on Lightstream,
run at native RAM speeds - which, although this in itself is a major throughput upgrade
(as other libraries do not run at essentially RAM speed in that case) - the common scenario
people often face with mmap (at least when working locally) is when dataset size exceeds RAM.
So, consider that - this case shows streaming from e.g., a well-spec'd server where one is performing
replay with different e.g. parameter settings on the same dataset. In that case, as demonstrated in the benchmark,
Lightstream allows one to 'practically keep the on-disk data faulted into RAM when the capacity is available (and would otherwise
pay disk-speed reads when not), and then be able to transfer an ordered data stream utilising all available cores at high speed.

## AWS Costs
The approximate on-demand price of the pair in `us-east-1` is around
$10.9/hour (check AWS for latest spot pricing).

`KEEP=1` leaves the pair running and billing until it is destroyed.

## Configuration
The Dockerfile builds the binaries for the build host's platform and does not
cross-compile. `i3en.12xlarge` is x86_64, matching the x86_64 image a typical
x86_64 operator machine produces, and the default AMI is the x86_64
ECS-optimised Amazon Linux 2023 image resolved through SSM.

To run on Graviton instead, use an arm64 instance type and match all three:

* set `INSTANCE_TYPE` to an arm64 type (passed through to Terraform),
* set `ami_ssm_parameter` to the arm64 path
  (`/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id`),
* build the image for arm64 (for example with `docker buildx --platform
  linux/arm64`, or on a Graviton build host).

The image architecture must match the instance architecture or the ECS agent
will fail to start the container.

The ECS-optimised AL2023 AMI does not auto-mount instance storage, so the
instances' `user_data` assembles the NVMe instance-store devices into one XFS
filesystem at `/mnt/nvme` (mdadm RAID0 across the devices, or a single device
directly) before the ECS agent starts. Each task binds `/mnt/nvme` to `/data`,
the source's default dataset directory.

## Prerequisites

* AWS CLI v2 with credentials permitted to manage EC2, ECS, ECR, IAM,
  CloudWatch Logs and VPC resources.
* Terraform 1.6 or later.
* Docker.
* `jq`.

## Run the benchmark

Run with the default configuration:

```bash
./benches/ecs/run.sh
```

The defaults are:

* Region: `us-east-1`
* Shapes: `mixed narrow_numeric string_heavy wide`
* Data sources: `memory nvme`
* Rows per table: `1000000`
* Dataset size: `350` GB, split across the largest stream count
* Stream counts: `1,4,8,16`
* Warm runs per cell: `5`
* Instance type: `i3en.12xlarge`

Override the workload or region with environment variables:

```bash
REGION=us-west-2 \
SHAPES="narrow_numeric" \
DATA_SOURCES="memory nvme" \
ROWS=1000000 \
DATASET_GB=350 \
STREAMS=1,4,8 \
RUNS=5 \
./benches/ecs/run.sh
```

The script builds and pushes the image tagged with the git short SHA, runs each
shape over each data source, collects the sink's CloudWatch logs into
`benches/ecs/results/<timestamp>/<shape>-<data_source>.log`, splits the `RAW`
arrival series into CSV files under `benches/ecs/results/<timestamp>/series/`,
and prints a summary of the RESULT lines to standard output, also saved as
`benches/ecs/results/<timestamp>/summary.txt`.

### Dataset sizing

`DATASET_GB` is the whole workload budget. Both binaries derive the same
per-stream batch count from it, `dataset_bytes / (max_streams x table_bytes)`,
so a cell at `N` streams transfers `N/max_streams` of the budget per pass and
the largest cell transfers all of it. The default 350 GB is sized against
`i3en.12xlarge`: large enough that generation and cold reads are genuine
device work, and small enough to stay resident in the page cache under the
container's 350 GiB memory ceiling, so the warm passes measure RAM-served
replay. The dataset is written under a directory named by the workload
parameters and reused across runs, so a repeat run with the same shape skips
regeneration. Batch counts per cell vary with the shape's table size, and the
per-run `RESULT metric=gaps` lines label their sample count so percentile
precision is never overstated.

Keep the infrastructure after the benchmark for inspection:

```bash
KEEP=1 ./benches/ecs/run.sh
```

## Local smoke test

Verify the image build and the binaries end to end on one host, without AWS:

```bash
./benches/ecs/run-local-docker.sh
```

Both containers share one host, so the figures are not representative; this only
checks that the rig runs.

## RESULT lines

The sink prints machine-parsable RESULT lines. `run.sh` greps `^RESULT` across
the collected logs into the summary:

```text
RESULT metric=latency shape=mixed data=memory rtt_ms=...
RESULT protocol=flight shape=mixed data=memory rows=1000000 streams=4 batches=2000 run=1 gib_per_s=...
RESULT protocol=flight shape=mixed data=nvme cache=cold rows=1000000 streams=4 batches=2000 gib_per_s=...
RESULT protocol=lightstream shape=mixed data=nvme cache=warm rows=1000000 streams=4 batches=2000 run=1 gib_per_s=...
RESULT protocol=lightstream shape=mixed data=nvme cache=warm rows=1000000 streams=4 batches=2000 stat=median runs=5 gib_per_s=... min_gib_per_s=... max_gib_per_s=...
RESULT metric=gaps protocol=lightstream shape=mixed data=nvme streams=4 cache=warm run=1 n=1999 p50_us=... p95_us=... p99_us=... max_us=...
```

The `data` field is `memory` or `nvme`, matching the data source the line was
produced under. Under `nvme` the `cache` field is `cold` for the single
eviction-first pass and `warm` for the timed runs, and the medians cover the
warm runs. The `metric=gaps` lines summarise the inter-batch arrival gaps
within one run, with `p95_us` present from 100 samples and `p99_us` from
1000. The full arrival series ride along as `RAW` lines and land as CSVs
under the results directory's `series/` folder. `shape`, `rows`,
`dataset-gb`, `streams` and `runs` must match between the source and sink,
and `run.sh` passes the same values to both.

## Transports

The source and sink compare Arrow Flight `DoGet` with Lightstream TCP - the
plain table writer and reader for the single-stream cell, and the parallel
TCP writer and reader for the multi-stream cells. Under `nvme` each stream is
an independent file replay, Lightstream reading through its zero-copy mmap
reader and Arrow Flight reading the same files through the `arrow` IPC file
reader. The Flight ticket carries an evict flag for the cold pass and the
Lightstream cold pass is triggered by the control channel, so both transports
evict the same way. Both transports run plaintext over the trusted-VPC
network. TLS is assumed terminated at the ingress boundary and is excluded,
so neither side pays encryption overhead.

The Dockerfile accepts a `FEATURES` build argument and builds with
`bench_arrow_flight,tcp,mmap`. The mmap feature supplies the zero-copy reader
the `nvme` source replays through. The task workload is supplied at run-task
time by `run.sh`.

## Docker build context

The workspace depends on `minarrow` through a
`minarrow = { path = "../../minarrow" }` sibling checkout that lives outside the
repository. The Dockerfile therefore sets the build context to the parent
directory that holds both the `lightstream` and `minarrow` checkouts and copies
them into the image so the relative path resolves. `run.sh` and
`run-local-docker.sh` handle this automatically, including staging this rig's
`.dockerignore` into the context root (Docker reads `.dockerignore` only from
the context root) and restoring the operator's own file afterwards.

## Cleanup

The two EC2 instances continue to incur charges while they are running. The
script destroys the infrastructure on exit unless `KEEP=1` is set.

To destroy the infrastructure manually:

```bash
terraform -chdir=benches/ecs/terraform destroy \
  -auto-approve \
  -var region=<region>
```

Destroy the infrastructure after the benchmark completes to avoid further EC2,
ECR and CloudWatch charges.
