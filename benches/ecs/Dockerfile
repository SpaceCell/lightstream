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
# that the `minarrow = { path = "../minarrow" }` path dependency in the
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

# Reproduce the on-disk sibling layout so the `../minarrow` path dependency
# resolves. The workspace `Cargo.toml` declares
# `minarrow = { path = "../minarrow" }`, one directory above the lightstream
# checkout, so the two checkouts sit side by side under /build. The context
# root is the parent directory holding both checkouts.
COPY minarrow /build/minarrow
COPY lightstream /build/lightstream

WORKDIR /build/lightstream

# The cache mounts keep the cargo registry and incremental build artefacts on
# the build host across image rebuilds, so a source change recompiles only the
# affected crates. Artefacts inside a cache mount do not persist into the
# image layer, so the binaries are copied out within the same step.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/lightstream/target \
    cargo build --release \
        --example bench_ecs_source --example bench_ecs_sink \
        --features "${FEATURES}" \
    && mkdir -p /out \
    && cp target/release/examples/bench_ecs_source /out/ \
    && cp target/release/examples/bench_ecs_sink /out/

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/bench_ecs_source /usr/local/bin/bench_ecs_source
COPY --from=build /out/bench_ecs_sink /usr/local/bin/bench_ecs_sink

# Print the source usage unless the command is overridden at run-task time.
CMD ["bench_ecs_source", "--help"]
