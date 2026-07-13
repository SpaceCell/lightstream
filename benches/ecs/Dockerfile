# syntax=docker/dockerfile:1.6
#
# Builds the cross-host throughput benchmark binaries `bench_ecs_source` and
# `bench_ecs_sink` for the Amazon ECS benchmark. Both tasks use the same image,
# with the command selected at run-task time by `run.sh`.
#
# The workspace requires a nightly Rust toolchain for `allocator_api` and
# `portable_simd`. Set `RUST_IMAGE` to use a specific nightly release.
#
# `FEATURES` controls the enabled Cargo features. This rig needs Arrow Flight
# (which pulls in tonic, hence protobuf-compiler), TCP and mmap. The mmap
# feature supplies the zero-copy reader the nvme data source replays through.
#
# Build context: this Dockerfile expects the build context to be the parent
# directory that contains both the `lightstream` and `minarrow` checkouts, so
# that the `minarrow = { path = "../../minarrow" }` path dependency in the
# workspace `Cargo.toml` resolves inside the image. `run.sh` and
# `run-local-docker.sh` set the context accordingly; see the comments there.

ARG RUST_IMAGE=rustlang/rust:nightly-bookworm-slim
FROM ${RUST_IMAGE} AS build

ARG FEATURES=bench_arrow_flight,tcp,mmap

# Install native dependencies required by the workspace build. protobuf-compiler
# supplies protoc, which tonic needs to generate the Arrow Flight gRPC stubs.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev ca-certificates protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Reproduce the on-disk sibling layout so the `../../minarrow` path dependency
# resolves. The workspace `Cargo.toml` declares
# `minarrow = { path = "../../minarrow" }`, which is two directories above the
# lightstream checkout. Placing lightstream at `/build/pr/lightstream` and
# minarrow at `/build/minarrow` makes `../../minarrow` point at the minarrow
# checkout. The context root is the parent directory holding both checkouts.
COPY minarrow /build/minarrow
COPY lightstream /build/pr/lightstream

WORKDIR /build/pr/lightstream

RUN cargo build --release \
        --example bench_ecs_source --example bench_ecs_sink \
        --features "${FEATURES}"

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/pr/lightstream/target/release/examples/bench_ecs_source /usr/local/bin/bench_ecs_source
COPY --from=build /build/pr/lightstream/target/release/examples/bench_ecs_sink /usr/local/bin/bench_ecs_sink

# Print the source usage unless the command is overridden at run-task time.
CMD ["bench_ecs_source", "--help"]
