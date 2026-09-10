# 44. Content-type hint on `Publish`

## Status

Accepted

## Context

Filed as #131 (Phase 13). A payload is an opaque byte string with no
indication of what it is - JSON, CBOR, a PNG, a protobuf message,
plain text. A subscriber has to already know by out-of-band
convention what a given topic carries (see ADR-0035's payload-fidelity
work and the discussion that prompted this phase).

The issue's known shape asked for an optional, purely-informational
content-type field on `Publish` - the protocol still never interprets
a payload - and to decide the format: a free-form string (like HTTP's
`Content-Type`) or a small fixed enum.

## Decision

### An optional free-form string, `content_type: Option<String>`

`MessageKind::Publish` gains `content_type: Option<String>`
(`#[serde(default)]`, so an older sender omitting it decodes as
`None` - the same additive-field rolling-upgrade pattern ADR-0041's
`ack`, ADR-0042's `group`, and ADR-0043's `retain` established).
`None` means "no hint given"; `Some(s)` carries whatever the
publisher wants to say about the payload.

Free-form, not a fixed enum. The deciding factor is the issue's own
constraint: the hint is *purely informational* - nothing in the
node, the broker, or the wire protocol reads it, validates it, or
branches on it. A fixed enum (`Text`/`Json`/`Cbor`/`Binary`/...) buys
type-safety that only matters when something switches on the value,
and there's nothing to switch on here. Any fixed set is also wrong
for someone (protobuf with a message name, a versioned vendor JSON
type, `text/csv`), forcing an `Other(String)` escape hatch that is
just a string with extra ceremony. A string is also the convention
every developer already knows from HTTP.

### Not validated, not length-capped

The node accepts any string. It's recommended (in `docs/OPERATIONS.md`,
not enforced) that publishers use MIME-type syntax
(`application/json`, `text/plain; charset=utf-8`, `application/octet-stream`)
so subscribers across a mesh interpret it consistently, but a node
never rejects a `Publish` for a malformed or unrecognized
`content_type`, and never normalizes one. There's no dedicated length
limit - it's bounded in practice by the 16 MiB frame cap, the same as
`Error`'s `message` string and the payload itself. A dedicated cap is
possible future hardening if it ever matters.

### Carried, never consumed, everywhere else

`content_type` is a field on the `Publish` variant, so it rides along
in the envelope automatically: the replay buffer (ADR-0021), a
retained message (ADR-0043), fan-out and consumer-group delivery
(ADR-0042), and cross-peer forwarding all preserve it with no
code of their own. A subscriber receives it as part of the delivered
`Publish` and does whatever it likes with it (or ignores it). No
interaction with `ack`/`group`/`retain` beyond being carried
alongside them.

### CLI

`thoth-mesh-cli publish` gains `--content-type <string>`. On the
`subscribe` side, a delivered message's hint is shown when present -
`[topic] (application/json) {payload}` in `text` mode, appended to
the per-message stderr note in `raw` mode - and omitted entirely
when there's no hint, so the common unhinted case looks exactly as
it does today.

## Consequences

- `MessageKind::Publish` grows a `content_type` field; every
  construction and pattern-match site across the workspace is updated
  - the same mechanical footprint the last three ADRs each had.
- No broker change: the field is inert data that existing delivery
  paths already carry.
- `PROTOCOL.md`'s `Publish` section and `docs/OPERATIONS.md` document
  the field, the recommended-but-unenforced MIME convention, and that
  a node never interprets it.
- With this, Phase 13 (delivery semantics) is complete: at-least-once
  (ADR-0041), consumer groups (ADR-0042), retained messages
  (ADR-0043), and now a payload-type hint.
