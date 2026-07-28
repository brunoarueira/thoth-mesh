# 2. Cargo workspace with layered crates

## Status

Accepted

## Context

thoth-mesh is a federated pub/sub system with genuinely separable
concerns: wire protocol/types, the federation/gossip logic, a running
daemon, and a CLI client. A single crate would force all of these to
compile and version together, and would make it harder to reason about
layering as the project grows. One goal of this project is to stress
Rust knowledge beyond CLI-toy scope, which includes practicing real
crate boundaries rather than one big binary.

## Decision

Structure the project as a Cargo virtual workspace (no root package) with
member crates under `crates/`:

- a core protocol/types library
- a federation/gossip library, depending on the core library
- a daemon binary, depending on both libraries
- a CLI client binary, depending on the core library

Shared metadata (version, edition, license, repository, homepage) is
declared once in `[workspace.package]` and inherited by each member via
`field.workspace = true`.

## Consequences

More scaffolding upfront than a single crate — four `Cargo.toml` files
instead of one, and internal path+version dependencies (`path = ".."`,
`version = "..."`) that need to move together when versions bump. In
exchange, each crate can be published, versioned, and depended on
independently, and the boundary between "protocol" and "federation
logic" is enforced by the compiler rather than by convention.
