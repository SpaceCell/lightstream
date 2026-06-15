# syntax=docker/dockerfile:1.6
#
# Image carrying the lightstream cross-host bench binaries
# (`bench_sender` and `bench_receiver`) for the ephemeral Kubernetes
# rig. One image runs on both pods, and the manifest command chooses
# the sender / receiver split.
#
# The base is a nightly toolchain because the minarrow dependency uses
# `allocator_api` and `portable_simd`. Override with `--build-arg
# RUST_IMAGE=...` to pin a specific nightly.
#
# FEATURES selects the transports compiled in. The default `tcp` matches
# the current sender / receiver examples. Extend to
# `tcp,uds,websocket,quic,http,protocol,zstd` once the binaries select a
# transport at runtime.

ARG RUST_IMAGE=rustlang/rust:nightly-bookworm-slim
FROM ${RUST_IMAGE} AS build
WORKDIR /src

ARG FEATURES=tcp

# System deps for compiling the workspace in release mode with the TLS
# providers present.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Build context is the repository root. The root .dockerignore trims
# target/ and build state so the context stays small.
COPY . .

RUN cargo build --release \
        --example bench_sender --example bench_receiver \
        --features "${FEATURES}"

# ---------------------------------------------------------------------------
# Runtime image
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/target/release/examples/bench_sender /usr/local/bin/bench_sender
COPY --from=build /src/target/release/examples/bench_receiver /usr/local/bin/bench_receiver

# The manifests override this with their own sender / receiver commands.
CMD ["bench_sender", "--help"]
