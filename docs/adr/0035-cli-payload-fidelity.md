# 35. `thoth-mesh-cli` payload input/output fidelity

## Status

Accepted

## Context

`publish`'s payload is a UTF-8 CLI argument, and `subscribe` prints
every delivered payload through `String::from_utf8_lossy` - both
directions assume text, and both silently mangle (or simply can't
carry) a binary payload. Filed as #55 (Phase 10, docs/ROADMAP.md),
which named two changes and asked that they be designed together
since both are about the same surface: `publish` reading from stdin as
an alternative to the CLI argument, and `subscribe` gaining a
binary-safe output mode.

## Decision

### `publish`: the existing `payload` argument, or `-` for stdin

`payload` stays a required positional (no new flag, no separate `-`
option) - `thoth-mesh publish topic "text"` is unchanged. The literal
value `-` is special-cased to mean "read the payload from stdin
instead," the same convention `tar`, `curl`, and most other
Unix-family tools already use, rather than inventing a new one or
requiring an explicit flag alongside it:

```sh
cat image.png | thoth-mesh publish images.new -
```

Stdin is read to EOF as raw bytes (`tokio::io::AsyncReadExt::read_to_end`),
not decoded as UTF-8 - this is what actually makes a binary payload
possible to send at all, not just a convenience. A payload that's
literally the one-character string `-` can't be sent this way; that's
an accepted, standard limitation of the convention, not something this
ADR tries to route around with an escape syntax.

The stdin-reading itself is a small function taking `impl AsyncRead`,
not hardcoded to `tokio::io::stdin()` - so unit tests can exercise it
against an in-memory buffer (including non-UTF-8 bytes) without
needing control over the test process's real stdin.

### `subscribe`: `--output text` (default) or `--output raw`

A new `--output <MODE>` flag on `subscribe`, a `clap::ValueEnum` (not
a bare boolean) specifically so a later mode - `json`, say, per the
issue's own mention of structured output for scripting - is a pure
addition to the enum, not a breaking rename of an existing flag.
Only two variants exist today:

- **`text`** (default): exactly today's behavior,
  `[topic] {lossy-utf8}`, on stdout. Nothing about existing usage
  changes.
- **`raw`**: every delivered message's payload, written to stdout as
  exactly the bytes it arrived as - no topic label, no separator
  between messages, no lossy decoding. Binary-safe, and safe to
  redirect straight to a file or pipe into another tool. Because nothing
  but payload bytes belongs on stdout in this mode, the "Subscribed to
  ..." banner and a per-message `[topic] N bytes` note move to
  **stderr** instead - a normal `subscribe ... --output raw > out.bin`
  still shows this activity on the terminal without it corrupting the
  captured file.

`--output raw` writes messages back-to-back with nothing between them
deliberately, rather than inventing a delimiter (a NUL byte, a length
prefix): the common case is capturing exactly one payload before
hitting Ctrl-C, and a delivery-boundary convention on top of raw bytes
is really a small framing protocol of its own, which is a JSON- or
NUL-delimited-mode-shaped problem, not this ADR's.

### JSON/structured output: deliberately deferred

The issue asked that a possible JSON mode be *decided* alongside raw
output, not necessarily *shipped* alongside it. `--output` as an enum
already leaves room for a `json` variant later without revisiting this
decision. Building it now would mean also deciding a binary-payload
encoding (base64, most likely) and a schema, which is a genuinely
separate design question from "stop mangling binary payloads" - v1
here stays scoped to that.

## Consequences

`thoth-mesh publish topic -` and `thoth-mesh subscribe topic --output
raw` round-trip an arbitrary binary payload byte-for-byte; verified
manually with a few KB of random bytes through a real node. No wire
protocol change - `MessageKind::Publish`'s payload was already
`Vec<u8>`; this is entirely about how the CLI gets bytes in and back
out. `tokio`'s `io-std` feature becomes a new dependency of
thoth-mesh-cli, for `tokio::io::stdin()`.

Closes #55.
