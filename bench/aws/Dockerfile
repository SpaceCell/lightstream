# syntax=docker/dockerfile:1.6
#
# Image carrying the lightstream AWS rig binaries (`bench_sender` and
# `bench_receiver`). Builds in release with default features plus
# `tcp`, then copies the two binaries into a slim runtime image. The
# same image runs on both EC2 instances; selection between sender
# and receiver is done by the `CMD`.

ARG RUST_VERSION=1.85
FROM rust:${RUST_VERSION}-slim AS build
WORKDIR /src

# System deps for compiling the lightstream tree at release with TLS
# implementations available (kept light - the AWS rig defaults to
# plaintext TCP, but the dev tree compiles cleanly with these).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the workspace in. The .dockerignore (sibling to this file)
# trims target/ and ephemeral build state to keep the context small.
COPY . .

RUN cargo build --release --example bench_sender --example bench_receiver --features tcp

# ---------------------------------------------------------------------------
# Runtime image
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/target/release/examples/bench_sender /usr/local/bin/bench_sender
COPY --from=build /src/target/release/examples/bench_receiver /usr/local/bin/bench_receiver

# Default to printing usage. The orchestration scripts override
# this with explicit sender/receiver invocations.
CMD ["bench_sender", "--help"]
