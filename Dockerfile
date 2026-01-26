# InferaDB Ledger Production Dockerfile
#
# Multi-stage build for minimal production images.
# Uses Rust official image for building, distroless for runtime.
#
# Build:
#   docker build -t inferadb/ledger:latest .
#
# Run:
#   docker run -v /data:/var/lib/ledger inferadb/ledger:latest

# =============================================================================
# Stage 1: Builder
# =============================================================================
FROM rust:1.93-bookworm AS builder

# Install protobuf compiler and well-known types (required for gRPC code generation)
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/types/Cargo.toml crates/types/Cargo.toml
COPY crates/store/Cargo.toml crates/store/Cargo.toml
COPY crates/state/Cargo.toml crates/state/Cargo.toml
COPY crates/raft/Cargo.toml crates/raft/Cargo.toml
COPY crates/server/Cargo.toml crates/server/Cargo.toml
COPY crates/sdk/Cargo.toml crates/sdk/Cargo.toml
COPY crates/test-utils/Cargo.toml crates/test-utils/Cargo.toml

# Create stub source files for dependency caching
RUN mkdir -p crates/types/src crates/store/src crates/state/src \
    crates/raft/src crates/server/src crates/sdk/src crates/test-utils/src \
    && echo "fn main() {}" > crates/types/src/lib.rs \
    && echo "fn main() {}" > crates/store/src/lib.rs \
    && echo "fn main() {}" > crates/state/src/lib.rs \
    && echo "fn main() {}" > crates/raft/src/lib.rs \
    && echo "fn main() {}" > crates/server/src/lib.rs \
    && echo "fn main() {}" > crates/server/src/main.rs \
    && echo "fn main() {}" > crates/sdk/src/lib.rs \
    && echo "fn main() {}" > crates/test-utils/src/lib.rs

# Build dependencies only (cached layer)
RUN cargo build --release --package inferadb-ledger-server 2>/dev/null || true

# Copy actual source code
COPY crates crates
COPY proto proto

# Touch source files to invalidate cargo's cache for our code
RUN find crates -name "*.rs" -exec touch {} \;

# Build the actual binary
RUN cargo build --release --package inferadb-ledger-server

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM gcr.io/distroless/cc-debian12:nonroot

# Copy the binary
COPY --from=builder /build/target/release/ledger /usr/local/bin/ledger

# Default data directory (should be mounted as a volume)
VOLUME /var/lib/ledger

# gRPC port
EXPOSE 50051

# Metrics port (optional)
EXPOSE 9090

# Environment defaults optimized for Kubernetes
ENV INFERADB__LEDGER__LISTEN_ADDR=0.0.0.0:50051
ENV INFERADB__LEDGER__DATA_DIR=/var/lib/ledger
ENV INFERADB__LEDGER__METRICS_ADDR=0.0.0.0:9090

# Run as non-root user (distroless nonroot UID=65532)
USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/ledger"]
