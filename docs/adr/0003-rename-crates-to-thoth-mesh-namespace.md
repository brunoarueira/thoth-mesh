# 3. Rename crates to the thoth-mesh namespace

## Status

Accepted

## Context

The project was initially scaffolded with the root/repo named `thoth`
and crates `thoth`, `thoth-mesh`, `thoth-node`, `thoth-cli` — intending
`thoth` as the umbrella namespace and `thoth-mesh` as one crate within
it (the federation/gossip layer specifically).

Before publishing placeholder versions to crates.io to reserve the
names, we discovered the bare `thoth` name and `thoth-cli` were already
registered by unrelated projects (a bibliographic GraphQL API, and a
terminal scratchpad app, respectively). `thoth-mesh`, `thoth-node`, and
a range of other `thoth-*` names were free. This wasn't checked before
the initial scaffold, which was a process gap.

## Decision

Revert the umbrella/root project name to `thoth-mesh` — the one name
that was both available and already the most natural flagship name —
and rescope every crate under it, following the convention used by
projects like tokio and serde (sibling crates prefixed with the
flagship crate's own name, not a shorter, separately-squattable
prefix):

- `thoth` (core protocol/types) → `thoth-mesh-core`
- `thoth-mesh` (federation/gossip) → unchanged
- `thoth-node` (daemon) → `thoth-mesh-node`
- `thoth-cli` (client) → `thoth-mesh-cli`, and its installed binary
  renamed from `thoth` to `thoth-mesh` to avoid confusing users
  installing it with the unrelated `thoth` crate

## Consequences

No crate is named bare `thoth`. Crate names are longer
(`thoth-mesh-core`, `thoth-mesh-node`, `thoth-mesh-cli`) but fully
self-consistent and collision-free. Establishes a checklist item for
future projects in this repo (and elsewhere): verify name availability
across the *entire* intended crate family on crates.io before
scaffolding, not after.
