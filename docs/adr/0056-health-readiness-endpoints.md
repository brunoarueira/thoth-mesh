# 56. Health/readiness endpoints

## Status

Accepted

## Context

Issue #144 (Phase 16). Nothing today distinguishes "the process is
up" from "the node is actually ready to serve" the way orchestrators
(k8s, systemd) expect - Phase 9's packaging work has no health check
to wire up. The issue's own known shape named two decisions to make:
where this lives (likely piggybacking on `--metrics-addr`'s existing
HTTP server, ADR-0013, rather than a third port), and that liveness
("the process is alive") and readiness ("actually accepting
connections") are different questions worth answering separately,
not one combined check.

## Decision

### Piggyback on `--metrics-addr`: two new paths, no new port or flag

`/livez` and `/readyz`, served on the same listener `--metrics-addr`
already opens - no new CLI flag, no new port to open or document.
This follows the issue's own steer and this project's established
bias toward the smallest mechanism that covers the need (ADR-0013's
"no metrics crate," ADR-0019's "smaller lift proportional to what
this endpoint actually is"). The real cost: health checks are only
available when `--metrics-addr` is set, coupling two things an
operator might otherwise want independently (expose Prometheus
metrics vs. wire up an orchestrator probe). Accepted deliberately - a
dedicated health-only flag/port is a straightforward follow-up if that
coupling ever actually bites someone, not something to speculatively
build now.

Named `/livez`/`/readyz` rather than the older, single `/healthz`
convention - different tools read very different things into
`/healthz` (some treat it as liveness, some as combined liveness-and-
readiness), where `/livez` and `/readyz` name exactly one question
each with no ambiguity. This is the same pair Kubernetes' own API
server exposes for itself.

Every other path - including `/metrics`, and anything not recognized
- keeps today's behavior exactly as-is: the current Prometheus render,
regardless of method or path (see ADR-0013's original `handle_scrape`,
renamed `handle_request` by this ADR now that it routes on more than
one path). No existing scrape target pointed at a nonstandard path
breaks.

### Liveness: unconditionally `200`

`/livez` always returns `200 OK` with a short `ok\n` body - if this
handler is running at all, the process is alive enough to answer. No
state to check, nothing that can make this fail short of the process
already being unable to accept the TCP connection in the first place
(at which point nothing is answering any port, livez included, and
that's exactly what a liveness probe's own connection-level timeout
is for).

### Readiness: a one-way latch, flipped once startup actually finishes

A new `Readiness` handle (`crates/thoth-mesh-node/src/health.rs`) -
cheaply `Clone` (an `Arc<AtomicBool>` under the hood), same pattern as
`Membership`/`Interest`/`PeerLinks` elsewhere in this crate. Starts
`false`; `/readyz` returns `200 OK`/`ready\n` once set, `503 Service
Unavailable`/`not ready\n` before.

"Ready" means: TLS (if any) is built, and the on-disk store (if any)
has finished rehydrating. `run_with_tls`/`serve_with_tls` call
`.mark_ready()` right after their own `rehydrate_from_store(...)
.await` - both already await it synchronously before doing anything
else, so by the time either reaches `accept_loop`, this was already
true anyway. `spawn_with_tls` is the one exception: it isn't `async`,
so rehydration already ran in a detached background task before this
ADR (a pre-existing, deliberate choice - see its own comment), and
`accept_loop` starting is *not* evidence that task has finished.
Marking readiness from `accept_loop` uniformly, as an earlier draft of
this ADR did, would have let `/readyz` go `200` before a
`spawn_with_tls` node's persisted messages/retained values were
actually back - so `spawn_with_tls` instead marks it from inside that
same background task, right after the rehydration attempt completes
(successful or not - a failed rehydration still leaves this node
operational, just degraded, the same tolerance already in play before
this ADR); with no store to rehydrate at all, it marks immediately,
same as the other two. `accept_loop` itself marks nothing - each
entry point now owns the exact moment that's actually true for it.

Deliberately a one-way latch, never reset back to `false`: today's
failure model gives `accept_loop` nowhere to go but down on a fatal
listener error (it returns `Err`, which propagates out through
`run_with_tls` to `main`, ending the whole process - metrics/health
port included). There's no "still alive but no longer accepting"
state this node can currently reach that `/readyz` flipping back to
`false` would ever get a chance to report. Revisit if a future ADR
ever gives this node a failure mode that keeps the process up while
no longer accepting connections.

### Neither endpoint is gated by `--metrics-token-file`

Unlike `/metrics` (ADR-0019), `/livez`/`/readyz` answer with no
token required, even when one is configured for the render. A
liveness/readiness probe is a yes/no the process's own orchestrator
needs to act on (restart vs. not, route traffic vs. not) - threading
a bearer token through a kubelet `httpGet` probe is real operational
friction for a mechanism most orchestrators expect to just work, and
the two booleans these endpoints reveal aren't the kind of
operational detail (peer counts, rejection totals, etc.) ADR-0019's
token exists to gate in the first place.

### `Shared`/`Node` always carry a `Readiness`, even without a metrics port

Each entry point marks its own `shared.readiness` unconditionally,
regardless of whether a metrics port was ever opened to report it -
`Shared` gains a plain `readiness: Readiness` field (initialized in
`Shared::new`, no behavior change for anything that doesn't read it),
and `Node` (returned by `spawn_with_tls`) exposes its own clone of the
same handle for tests. Keeping this always-on rather than only
threading it through when `--metrics-addr` is given mirrors
`Metrics`' own precedent (ADR-0013: bundled into `Shared`
unconditionally) and means a future caller of `serve_with_tls`/
`spawn_with_tls` that wants to serve health checks of its own already
has a working, correctly-wired `Readiness` to hand to
`metrics_server::serve_metrics` - no further plumbing needed.

## Consequences

- New `crates/thoth-mesh-node/src/health.rs`: `Readiness`, public.
- `Shared` gains a `readiness: Readiness` field; `Node` gains one too,
  for test/introspection use (mirrors `membership`/`discover`).
- `run_with_tls`/`serve_with_tls` call `shared.readiness.mark_ready()`
  right after their own awaited rehydration; `spawn_with_tls` marks it
  from inside its background rehydration task (or immediately, with
  no store to rehydrate) - never from `accept_loop` itself, which
  starts before that background task is guaranteed to finish.
- `metrics_server::handle_request` now actually parses the request
  line (method + path, query string stripped before matching) instead
  of discarding it as an ordinary header line - routing `/livez`/
  `/readyz` distinctly, every other path unchanged.
- No new CLI flag, no new port, no new Prometheus metric.

Closes #144.
