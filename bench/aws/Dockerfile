# syntax=docker/dockerfile:1.6
#
# Builds the Lightstream `bench_sender` and `bench_receiver` binaries and copies
# them into a shared runtime image. The orchestration scripts select which
# binary to run.

ARG RUST_VERSION=1.85
FROM rust:${RUST_VERSION}-slim AS build
WORKDIR /src

# Install native dependencies required by the workspace build.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the workspace. `.dockerignore` excludes build output and temporary files.
COPY . .

RUN cargo build --release --example bench_sender --example bench_receiver --features tcp

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/target/release/examples/bench_sender /usr/local/bin/bench_sender
COPY --from=build /src/target/release/examples/bench_receiver /usr/local/bin/bench_receiver

# Print the sender usage unless the command is overridden.
CMD ["bench_sender", "--help"]
