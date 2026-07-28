# 4. Dual MIT/Apache-2.0 license

## Status

Accepted

## Context

thoth-mesh is intended to be a public, reusable set of Rust crates. The
Rust ecosystem has a de facto standard licensing convention for crates
meant to be consumed by others (used by e.g. tokio, serde, rand), and
deviating from it adds friction for downstream users — some
organizations' dependency policies are keyed specifically to "MIT or
Apache-2.0" being available.

## Decision

License all crates in the workspace under `MIT OR Apache-2.0`, the
standard dual-license SPDX expression for Rust crates. `LICENSE-MIT`
and `LICENSE-APACHE` files live at the repo root and are referenced via
`workspace.package.license`, inherited by every member crate.

## Consequences

Maximally permissive and compatible with the rest of the Rust
ecosystem. Contributions are implicitly licensed under both; there is
no CLA in place for this project.
