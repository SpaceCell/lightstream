# syntax=docker/dockerfile:1.6
#
# Builds the Lightstream `bench_sender` and `bench_receiver` binaries for the
# Kubernetes benchmark. Both pods use the same image, with the command selected
# by their respective manifests.
#
# The workspace requires a nightly Rust toolchain for `allocator_api` and
# `portable_simd`. Set `RUST_IMAGE` to use a specific nightly release.
#
# `FEATURES` controls the enabled Cargo features and defaults to `tcp`.

ARG RUST_IMAGE=rustlang/rust:nightly-bookworm-slim
FROM ${RUST_IMAGE} AS build
WORKDIR /src

ARG FEATURES=tcp

# Install native dependencies required by the workspace build.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the repository. The root `.dockerignore` excludes build output and
# temporary files.
COPY . .

RUN cargo build --release \
        --example bench_sender --example bench_receiver \
        --features "${FEATURES}"

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/target/release/examples/bench_sender /usr/local/bin/bench_sender
COPY --from=build /src/target/release/examples/bench_receiver /usr/local/bin/bench_receiver

# Print the sender usage unless the command is overridden by a manifest.
CMD ["bench_sender", "--help"]
