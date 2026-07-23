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
# (which pulls in tonic, hence protobuf-compiler), the Lightstream protocol
# over TCP, and mmap. The mmap feature supplies the zero-copy reader the nvme
# data source replays through.
#
# Build context: this Dockerfile expects the build context to be the parent
# directory that contains both the `lightstream` and `minarrow` checkouts.
# The workspace `Cargo.toml` pins minarrow to its crates.io version, and the
# build step patches that dependency to the copied `minarrow` checkout, so
# the image builds against the sibling sources. `run.sh` and
# `run-local-docker.sh` set the context accordingly; see the comments there.

ARG RUST_IMAGE=rustlang/rust:nightly-bookworm-slim
FROM ${RUST_IMAGE} AS build

ARG FEATURES=bench_arrow_flight,protocol,tcp,mmap

# Install native dependencies required by the workspace build. protobuf-compiler
# supplies protoc, which tonic needs to generate the Arrow Flight gRPC stubs.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev ca-certificates protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy the two checkouts side by side under /build, which mirrors their
# on-disk sibling layout. The context root is the parent directory holding
# both checkouts. `LIGHTSTREAM_DIR` names the lightstream checkout within
# the context, so a git worktree with a different directory name builds the
# code it sits in.
ARG LIGHTSTREAM_DIR=lightstream
COPY minarrow /build/minarrow
COPY ${LIGHTSTREAM_DIR} /build/lightstream

WORKDIR /build/lightstream/rust

# The cache mounts keep the cargo registry and incremental build artefacts on
# the build host across image rebuilds, so a source change recompiles only the
# affected crates. Artefacts inside a cache mount do not persist into the
# image layer, so the binaries are copied out within the same step. The target
# cache is keyed by checkout name because cargo judges staleness by file
# timestamps: a cache stamped by one checkout postdates another checkout's
# files, and sharing it would hand back the other checkout's binaries.
# The patch resolves the crates.io minarrow pin to the sibling checkout
# copied above. Writing it here replaces any machine-local cargo config the
# context carried in, so the image always builds against /build/minarrow.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,id=target-${LIGHTSTREAM_DIR},target=/build/lightstream/rust/target \
    mkdir -p .cargo \
    && printf '[patch.crates-io]\nminarrow = { path = "/build/minarrow" }\n' > .cargo/config.toml \
    && cargo build --release \
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
