# Lightstream cross-host benchmark on Amazon ECS

This benchmark provisions two dedicated EC2 instances orchestrated by Amazon ECS
using the EC2 launch type and runs the same workload between them over Arrow
Flight and Lightstream TCP. The sink initiates both transfers and records
receiver-side throughput for each transport together with host-to-host
round-trip latency. The infrastructure is destroyed when the benchmark
completes.

Throughput is measured as logical payload delivered to and verified by the
receiver, from request initiation until the final required arrival. For
Lightstream, this includes reconstructing one globally ordered stream across all
parallel connections. Arrow Flight is verified for ordered and complete delivery
within each ticketed endpoint; the benchmark does not add an external
cross-endpoint merge because Flight uses ticketed endpoints with contiguous
recall of those independent partitions - it does not supply single stream 
reconstruction as part of that contract, and re-assembling it manually is prone
to head-of-line blocking that would tarnish the comparison. So, instead,
both cases supply their 'best ordering guarantee', and for a single ordered stream
the single-threaded comparison (setting parallelism to '1') compares the equivalent
case, as Flight does order independent connection streams.

Each shape is run over two data sources. The `memory` source materialises the
table in RAM and measures transport throughput without hard disk storage.
This is the main path and is the only one relevant for transport comparisons.

Separately, `nvme` benches disk recall out over the wire. The source writes a
dataset of `DATASET_GB` gigabytes to the instance's local NVMe as one Arrow IPC
file per stream, using Lightstream's native Arrow IPC writer. Each transport
then evicts the page cache, replays the files once cold off the device, and
follows with `RUNS` warm replays served from the page cache. This models a host
repeatedly streaming a large recorded and chunked dataset to a remote consumer.

By default both transports read through their native buffered file readers:
Arrow Flight through Arrow's IPC file reader, and Lightstream through its own
Arrow file reader. `mmap` is off by default. Setting `USE_MMAP=1` switches only
Lightstream to its zero-copy `mmap` reader and leaves Arrow Flight unchanged, so
it is offered for informational runs rather than a fair comparison, and
should not be inferred as such.

## Measurement setup

| Aspect | Setup |
| --- | --- |
| Instances | Two dedicated EC2 container instances. `bench_role` attribute constraints keep the source and sink on separate hosts. |
| Network path | One `cluster` placement group in a single AZ. Host networking, so traffic flows over the instances' private IPs. |
| Data sources | `memory` resends one RAM-resident table, bumping column reference counts per send. `nvme` replays one Arrow IPC file per stream off local NVMe. |
| Cache passes (nvme) | Per transport: evict, one `cache=cold` pass off the device, then `RUNS` `cache=warm` passes from the page cache. A control pulse from sink to source sequences the phases. |
| Verified delivery | Batches carry a global sequence number in their first column. Lightstream must reconstruct and verify files, record batches and row windows in dataset-wide order. Each Flight endpoint must deliver its assigned contiguous range completely and in order; no cross-endpoint order is asserted. The `memory` source also verifies total row counts. |
| Transports | Arrow Flight obtains ticketed endpoints with `GetFlightInfo` and consumes their `DoGet` streams concurrently. Those endpoint streams remain separate because the benchmark does not add a non-native global merge layer. Lightstream uses one connection per stream and merges them into one globally ordered output using the index announced by each connection. |
| Runs and stats | `RUNS` (default `5`) request-to-final-verified-arrival runs per transport and benchmark cell. Lightstream's timing includes global ordered reconstruction. Flight timing completes when all endpoint-local ranges have been received and verified. Results include median, minimum and maximum throughput, `metric=gaps` percentiles labelled with sample counts, and receiver-visible `RAW` arrival series exported as CSV files. |
| Transport order | `memory` interleaves the transports run by run. `nvme` runs each transport as a block so neither inherits the other's page cache. |
| Cleanup | Infrastructure destroyed on exit unless `KEEP=1` is set. |


The ordering guarantees represented by the results are therefore intentionally
not identical. Lightstream reports the throughput of a completed, globally
ordered output stream. Arrow Flight reports the aggregate delivery of multiple
concurrent endpoint streams, each ordered within its own ticketed range, without
charging Flight for an application-level operation that the protocol does not
provide. This distinction is retained throughout validation and result
interpretation.


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
what the software can do. Four requirements drive the choice:

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

## Disclosure and acknowledgements

This benchmark intentionally uses well-provisioned, uncontended infrastructure
to isolate transport behaviour. Real-world throughput will vary with workload
characteristics, network contention, storage performance, system configuration
and available memory.

Lightstream's reported throughput includes the work required to merge its
parallel connections into one globally ordered stream. Arrow Flight is measured
using its concurrent ticketed endpoints, with ordered and complete validation
inside each endpoint, but without an additional application-level global merge.
The benchmark therefore reports the native delivered form of each transport and
states the difference explicitly rather than silently assigning one system work
that the other does not perform.

Avoid `nvme` and `mmap` generally except for informational purposes. The repeated disk-backed runs use buffered file reads by default, so a warm replay serves the dataset from the page cache while leaving those pages cheaply reclaimable. With `USE_MMAP=1`, Lightstream instead replays through page-faulted `mmap` reads, and when the dataset remains resident in the page cache it can replay at speeds close to native memory throughput. This models a well-specified server repeatedly replaying the same dataset, potentially with different parameter settings, when sufficient memory is available to retain the working set. When the dataset does not remain resident, replay performance will also reflect the underlying device read rate.

## AWS Costs
The approximate on-demand price of the pair in `eu-west-2` is around
$12/hour (check AWS for the latest pricing). 

Long runs can be expensive - a full matrix can take up to several hours.

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

* Region: `eu-west-2`
* Shapes: `mixed narrow_numeric string_heavy wide`
* Data sources: `memory nvme`
* Rows per table: `1000000`
* Dataset size: `350` GB, split across the largest stream count
* Stream counts: `1,4,8,16`
* Warm runs per cell: `5`
* Instance type: `i3en.12xlarge`

Override the workload or region with environment variables:

```bash
REGION=us-east-1 \
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
the largest cell transfers all of it. The default 350 GB is sized to stay
resident under `i3en.12xlarge`'s 350 GiB container memory ceiling. The dataset
is written under a directory named by the workload parameters and reused across
runs, so a repeat run with the same shape skips regeneration. Batch counts per
cell vary with the shape's table size, and the per-run `RESULT metric=gaps`
lines label their sample count so percentile precision is never overstated.

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

The `data` field is `memory` or `nvme`, matching the source used for the
measurement. Under `nvme`, `cache=cold` identifies the single pass after cache
eviction and `cache=warm` identifies the timed page-cache runs; the reported
medians cover the warm runs.

Throughput is logical payload in GiB/s measured from request initiation to the
final verified arrival. For Lightstream, completion occurs only after the sink
has reconstructed and verified the dataset-wide ordered stream. For Arrow
Flight, completion occurs after every ticketed endpoint has delivered and
verified its complete ordered range. The Flight endpoint streams are not
post-processed into a synthetic global stream, so Flight results must not be
interpreted as including cross-endpoint ordered reconstruction.

The `metric=gaps` lines summarise adjacent receiver-visible arrival gaps within
one run, with `p95_us` present from 100 samples and `p99_us` from 1000. For
Lightstream, the receiver-visible series reflects the globally reconstructed
output. For Flight, observations retain endpoint-local delivery semantics. The
complete arrival series is emitted as `RAW` lines and written as CSV files under
the results directory's `series/` folder. `shape`, `rows`, `dataset-gb`,
`streams` and `runs` must match between the source and sink, and `run.sh` passes
the same values to both.

## Transports

The source and sink compare Arrow Flight `DoGet` with the Lightstream parallel
TCP protocol.

### Lightstream

Lightstream uses one TCP connection per stream. The source distributes tables
across those connections in round-robin order, and each connection announces
its stream index when it opens. The sink uses those indices and the global
sequence carried by the data to reconstruct one dataset-wide stream in original
write order, regardless of the order in which connections are accepted or data
arrives.

The ordered merge is part of the measured receiver path. Its CPU, coordination
and buffering costs therefore count against Lightstream's reported throughput.
A Lightstream run is complete only when the final item has been emitted and
verified in global dataset order.

### Arrow Flight

Arrow Flight calls `GetFlightInfo` and consumes the returned ticketed endpoints
concurrently through `DoGet` over Tonic gRPC and HTTP/2. Each endpoint carries a
contiguous dataset range and must deliver that range completely and in order.

Arrow Flight does not define a native cross-endpoint order for a partitioned
dataset and does not return a single reconstructed global stream from those
parallel endpoints. The benchmark therefore leaves the endpoint streams
separate rather than adding an application-specific merge layer. A Flight run is
complete when all endpoint-local ranges have been fully received and verified.

This means the benchmark compares Lightstream's globally ordered output against
Flight's native concurrent endpoint output. It does not claim that Flight's
separate endpoint streams are globally ordered, and it does not exclude the
cost of Lightstream's ordered reconstruction.

### NVMe replay

Under `nvme`, both transports replay the same selected files in file-index and
record-batch order. By default Lightstream reads through its own buffered Arrow
file reader and Arrow Flight uses the `arrow` IPC file reader. `USE_MMAP=1`
switches only Lightstream to its zero-copy `mmap` reader. The Flight ticket
carries an eviction flag for the cold pass, and the Lightstream cold pass is
triggered by the control channel, so both transports apply equivalent cache
eviction.

Both transports run in plaintext over the trusted VPC network. TLS is assumed
to terminate at the ingress boundary and is excluded so that neither side pays
encryption overhead.

The Dockerfile accepts a `FEATURES` build argument and builds with
`bench_arrow_flight,protocol,tcp,mmap`. The `mmap` feature provides the
zero-copy reader the Lightstream `nvme` replay path uses when `USE_MMAP=1`.
`run.sh` supplies the task workload when each ECS task is started.

## Docker build context

The workspace depends on `minarrow` through a
`minarrow = { path = "../minarrow" }` sibling checkout that lives outside the
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
