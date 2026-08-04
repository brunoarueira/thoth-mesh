# 8. Generic async framing in thoth-mesh-core via futures-util

## Status

Accepted

## Context

ADR-0007 hand-rolled a tokio-specific async equivalent of
`thoth_mesh_core::framing` directly in `thoth-mesh-node`, to avoid
adding any async runtime dependency to `thoth-mesh-core` (per
ADR-0002/0005). That works, but it duplicates the framing control flow
in two places, and ties the *shape* of the duplication specifically to
tokio - if another consumer of `thoth-mesh-core` ever wanted async
framing under a different runtime (`async-std`, `smol`, or a future
one), they'd have to write a third copy.

The actual constraint ADR-0002 cared about was never "no async," it was
"no *runtime* dependency" - `thoth-mesh-core` shouldn't force every
consumer to pull in a specific executor just to use the protocol types.
Async I/O *traits* (as opposed to a full runtime) don't carry that
problem: `futures-io`'s `AsyncRead`/`AsyncWrite` are trait definitions
only, with no executor attached, and are implemented or bridgeable by
every major runtime.

## Decision

Add an optional `async` feature to `thoth-mesh-core` that pulls in
`futures-util` (its `io` module specifically, for
`AsyncRead`/`AsyncWrite`/`AsyncReadExt`/`AsyncWriteExt`) and exposes
`read_frame`/`write_frame` generic over those traits, in a new
`async_framing` module alongside the existing sync `framing` module.
With the feature disabled (the default), `thoth-mesh-core` has exactly
the dependency footprint it has today - no async anything.

`thoth-mesh-node` enables the `async` feature and bridges its
tokio-specific I/O types into the `futures-io` traits via
`tokio_util::compat` (`TokioAsyncReadCompatExt`/
`TokioAsyncWriteCompatExt`, i.e. `.compat()`/`.compat_write()`), rather
than implementing its own copy of the framing logic. The hand-rolled
async framing added in ADR-0007 is removed.

## Consequences

Framing logic now has exactly one implementation per calling
convention (sync, async) instead of one sync plus one per-runtime async
copy, and any future async consumer of `thoth-mesh-core` gets framing
for free regardless of which runtime it uses, as long as it can produce
(or bridge to) `futures-io` types. `thoth-mesh-core` still adds no
dependency on a specific executor - `futures-util`'s `io` module has no
runtime attached, only trait definitions and the buffer-management glue
around them.

The cost is a small indirection at every call site in
`thoth-mesh-node`: tokio's native `TcpStream`/split halves need an
explicit `.compat()`/`.compat_write()` wrap before they satisfy
`futures-io`'s traits, and `tokio-util` becomes a new dependency of
`thoth-mesh-node` (already a common pairing with tokio, not an unusual
addition). `thoth-mesh-core`'s dependency graph also grows by one
crate (`futures-util`) whenever the `async` feature is enabled, though
never for consumers who don't opt in.
