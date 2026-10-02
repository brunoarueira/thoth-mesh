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
regardless of method or path (see ADR-0013's `handle_scrape`). No
existing scrape target pointed at a nonstandard path breaks.

### Liveness: unconditionally `200`

`/livez` always returns `200 OK` with a short `ok\n` body - if this
handler is running at all, the process is alive enough to answer. No
state to check, nothing that can make this fail short of the process
already being unable to accept the TCP connection in the first place
(at which point nothing is answering any port, livez included, and
that's exactly what a liveness probe's own connection-level timeout
is for).

### Readiness: a one-way latch, flipped once `accept_loop` starts

A new `Readiness` handle (`crates/thoth-mesh-node/src/health.rs`) -
cheaply `Clone` (an `Arc<AtomicBool>` under the hood), same pattern as
`Membership`/`Interest`/`PeerLinks` elsewhere in this crate. Starts
`false`; `accept_loop` (shared by every entry point - `run_with_tls`,
`serve_with_tls`, `spawn_with_tls` alike) calls `.mark_ready()` at the
exact point it already logs "node ready, accepting connections" -
that log line already *is* this project's definition of ready, just
not externally observable until now. `/readyz` returns `200 OK` /
`ready\n` once set, `503 Service Unavailable` / `not ready\n` before.

This means "ready" already implies TLS was built, the on-disk store
(if any) finished rehydrating, and the listener is bound - every
entry point runs that setup synchronously, in order, before
`accept_loop` is ever reached. No separate readiness logic to keep in
sync with startup order; it's a direct signal off the same moment
that already existed.

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

`accept_loop` marks readiness unconditionally, regardless of which
entry point called it or whether a metrics port was ever opened -
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
- `accept_loop` calls `shared.readiness.mark_ready()` at its existing
  "node ready, accepting connections" log line.
- `metrics_server::handle_scrape` now actually parses the request
  line (method + path) instead of discarding it as an ordinary
  header line - routing `/livez`/`/readyz` distinctly, every other
  path unchanged.
- No new CLI flag, no new port, no new Prometheus metric.

Closes #144.
