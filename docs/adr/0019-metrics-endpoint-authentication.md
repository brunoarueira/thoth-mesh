# 19. Metrics endpoint authentication via shared-secret bearer token

## Status

Accepted

## Context

Issue #75 (Phase 7), split out of #62/ADR-0018: `--metrics-addr`
(ADR-0013) has no access control at all today — any connection to
that port gets the current Prometheus render, no matter who's asking.

Neither of ADR-0017's (peer allowlist) nor ADR-0018's (per-topic
client authorization) mechanisms apply here. Both key off the SHA-256
fingerprint of a TLS client certificate, and the metrics port doesn't
go through `MaybeTlsStream` at all — ADR-0016 explicitly scoped TLS to
the client/peer port only and left `metrics_server.rs` plaintext HTTP.
There is no certificate to fingerprint on this port; whatever this ADR
lands on needs its own mechanism, not a reuse of the existing one.

Two real options were on the table:

- **A shared-secret bearer token**, checked against an `Authorization`
  header on each scrape request.
- **Extend TLS to the metrics port too**, undoing ADR-0016's explicit
  exclusion, and reuse the fingerprint-allowlist shape from ADR-0017.

## Decision

### Bearer token, not TLS

A shared-secret bearer token, not extending TLS to this port. Three
reasons:

- Prometheus's own `scrape_configs` has first-class support for a
  bearer token (`authorization: { credentials: ... }` /
  the legacy `bearer_token` field) — this is what a real operator
  would reach for anyway, no special client-side TLS/mTLS scrape
  config needed.
- Extending TLS here would reopen a decision ADR-0016 already made
  deliberately (metrics stays plaintext HTTP), for a port whose only
  client is a scrape target, not a peer or an interactive client —
  the trust model that justifies mTLS on the main port (proving
  *which* peer/client this is, individually) doesn't map onto "prove
  you're allowed to scrape," which a shared secret already answers.
- It's a smaller lift, proportional to what this endpoint actually is
  (issue #75 flagged this trade-off explicitly up front).

### `--metrics-token-file <path>`, not `--metrics-token <value>`

The token is read from a file, not passed as a raw CLI argument value.
Every other secret-shaped input this project takes (`--tls-cert`,
`--tls-key`, `--tls-ca`) is already a file path, not inlined content —
consistent with that, and it avoids a bearer token sitting in plain
sight in `ps` output or shell history the way `--metrics-token
s3cr3t` would. Read once at startup, trimmed of trailing
whitespace/newline (so a file created with a plain `echo` or a text
editor's trailing newline still works), and rejected at startup if
empty after trimming — an empty configured token would make
`Authorization: Bearer ` (empty) a trivially valid credential, which
defeats the point of turning this on at all.

Requires `--metrics-addr` (clap's `requires`, same pattern as
`--allow-peer` requiring the TLS flags) — a token with no metrics port
to guard is a no-op flag, and erroring at startup catches that
mistake instead of silently ignoring it.

### Enforcement: reject with `401` before rendering, constant-time compare

`--metrics-token-file` absent (the default): unchanged behavior, same
as today — any connection gets the render. Given: `handle_scrape`
reads the request's `Authorization` header (still reading and
discarding every other header, as before) and requires it to be
exactly `Bearer <token>`; anything else — header missing, wrong
scheme, wrong token — gets `401 Unauthorized` with a
`WWW-Authenticate: Bearer` header (RFC 6750) and a short plaintext
body, and the render is never computed for that connection. This
mirrors how a rejected `--topic-acl` request gets an explicit
rejection rather than a silently empty response (ADR-0018) — Prometheus's
own scrape-failure handling already expects an ordinary HTTP status
code, not a malformed or empty `200`.

The token comparison itself is constant-time (a manual XOR-accumulate
over the byte slices, no new dependency — the workspace has no
existing constant-time-compare crate and this is a handful of lines),
so a byte-by-byte timing side channel can't help a network-adjacent
attacker guess the token faster than brute force. This is proportional
caution, not a claim that this port faces a serious threat model: it's
a shared secret gating a metrics scrape, not a login form, but the
compare is exactly as cheap to do right as it would be to get subtly
wrong.

A new counter, `thothmesh_metrics_auth_rejections_total`, is added
alongside the existing four (ADR-0013, ADR-0018) — same rationale as
`topic_acl_rejections_total`: an operator watching this endpoint
should be able to see rejected scrape attempts, not just successful
ones. (Anyone without the token can't see this counter either, same
as anyone denied by a `--topic-acl` can't see
`topic_acl_rejections_total` about their own denial — it's for
whoever *does* have valid credentials to monitor with.)

### Where this lives: threaded through `run`/`run_with_tls` only

Unlike `TlsConfig` and `TopicAcl` (ADR-0016/0018), the metrics token
doesn't need to reach `Shared` or `connection.rs` at all — the metrics
endpoint is served by `metrics_server::serve_metrics`, called only
from `run_with_tls` (the daemon entry point), never from
`serve_with_tls`/`spawn_with_tls` (the test-oriented entry points,
which don't open a metrics port in the first place). So this is a new
`Option<Arc<str>>` parameter on `run`/`run_with_tls` and
`serve_metrics` only — `serve_with_tls`/`spawn_with_tls`'s signatures,
and every existing call site of them, are untouched by this ADR.

## Consequences

Turning this on costs one new file-based flag and a fifth Prometheus
metric; existing deployments that don't set `--metrics-token-file` see
no behavior change. Unlike TLS-based auth on the main port, this is a
single shared secret, not a per-scraper identity — anyone with the
token can scrape, and there's no way to tell one legitimate scraper
from another or revoke just one. That's an accepted trade-off for what
this endpoint is (a Prometheus target, not a multi-tenant API), not an
oversight.

Peer-scoped topic restriction remains open, tracked separately (#77) —
this ADR closes #75's half of what #62 originally bundled together.
