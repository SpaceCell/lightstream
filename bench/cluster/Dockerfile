# syntax=docker/dockerfile:1.6
#
# Builds the cross-host throughput benchmark binaries `bench_vpc_source` and
# `bench_vpc_sink` for the Kubernetes benchmark. Both pods use the same image,
# with the command selected by their respective manifests.
#
# The workspace requires a nightly Rust toolchain for `allocator_api` and
# `portable_simd`. Set `RUST_IMAGE` to use a specific nightly release.
#
# `FEATURES` controls the enabled Cargo features. The benchmark needs Arrow Flight,
# TCP and the Lightstream protocol.

ARG RUST_IMAGE=rustlang/rust:nightly-bookworm-slim
FROM ${RUST_IMAGE} AS build
WORKDIR /src

ARG FEATURES=bench_arrow_flight,tcp,protocol

# Install native dependencies required by the workspace build.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev ca-certificates protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy the repository. The root `.dockerignore` excludes build output and
# temporary files.
COPY . .

RUN cargo build --release \
        --example bench_vpc_source --example bench_vpc_sink \
        --features "${FEATURES}"

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/target/release/examples/bench_vpc_source /usr/local/bin/bench_vpc_source
COPY --from=build /src/target/release/examples/bench_vpc_sink /usr/local/bin/bench_vpc_sink

# Print the source usage unless the command is overridden by a manifest.
CMD ["bench_vpc_source", "--help"]
