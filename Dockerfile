# thoth-mesh-node / thoth-mesh (CLI) container image.
#
# Two stages: a full `rust` builder (needs a C toolchain for `ring`,
# see ADR-0016/ADR-0023) discarded after the build, and a minimal
# distroless runtime with no shell, no package manager, and nothing
# beyond what the dynamically-linked binaries actually need. See
# ADR-0023 for the reasoning behind every choice here.

FROM rust:1-bookworm AS builder

WORKDIR /build

# Workspace + per-crate manifests first, so `cargo fetch` is cached
# across rebuilds that only touch source, not dependencies.
COPY Cargo.toml Cargo.lock ./
COPY crates/thoth-mesh-core/Cargo.toml crates/thoth-mesh-core/Cargo.toml
COPY crates/thoth-mesh-broker/Cargo.toml crates/thoth-mesh-broker/Cargo.toml
COPY crates/thoth-mesh/Cargo.toml crates/thoth-mesh/Cargo.toml
COPY crates/thoth-mesh-tls/Cargo.toml crates/thoth-mesh-tls/Cargo.toml
COPY crates/thoth-mesh-node/Cargo.toml crates/thoth-mesh-node/Cargo.toml
COPY crates/thoth-mesh-cli/Cargo.toml crates/thoth-mesh-cli/Cargo.toml

COPY crates crates
RUN cargo build --release --workspace --locked

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

COPY --from=builder /build/target/release/thoth-mesh-node /usr/local/bin/thoth-mesh-node
COPY --from=builder /build/target/release/thoth-mesh /usr/local/bin/thoth-mesh

# 0.0.0.0, not thoth_mesh_core::DEFAULT_ADDR's 127.0.0.1 - a node
# bound to loopback is unreachable through `docker run -p`, from
# outside the container's network namespace entirely (see ADR-0023).
EXPOSE 49500
ENTRYPOINT ["/usr/local/bin/thoth-mesh-node"]
CMD ["--addr", "0.0.0.0:49500"]
