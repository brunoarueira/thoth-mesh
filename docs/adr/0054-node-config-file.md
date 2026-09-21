# 54. `thoth-mesh-node` config file for daemon options

## Status

Accepted

## Context

Filed as #167 (Phase 16). `thoth-mesh-node` is configured purely by
CLI flags - `--addr`, `--peer` (repeatable), `--metrics-addr`,
`--tls-cert`/`--tls-key`/`--tls-ca`, `--allow-peer`, `--topic-acl`/
`--peer-topic-acl`/`--peer-topic-filter`, `--metrics-token-file`,
`--data-dir`, `--persisted-message-ttl-secs`, `--dead-letter-topic`,
`--publish-rate-limit-per-sec`/`--publish-rate-limit-burst`, and
`--log-level` - 17 flags and growing. `thoth-mesh-cli` already has a
TOML config file for its (much smaller) connection-option set via
[ADR-0034](0034-cli-config-file.md); the daemon has nothing
equivalent, and today's answer (a systemd `EnvironmentFile`/Docker
`CMD` threading a giant flag string through) works but isn't
self-documenting.

Filed alongside #143 (dynamic config reload), which this issue's own
known shape flagged as wanting to be designed together: a `SIGHUP`-
triggered reload needs something on disk to re-read, and today there
isn't one. Picked up first, deliberately - #143 stays blocked on this
landing, not the other way around (see that ADR when it exists).

## Decision

### TOML, one flat key per flag, covering every flag - not just connection options

Unlike ADR-0034 (scoped to the CLI's four connection flags, a small
slice of that tool's much larger per-invocation flag surface), the
daemon has no per-invocation surface at all - *every* flag it takes is
exactly the kind of global, rarely-changing setting a config file
exists for. So the file mirrors the full `Cli` struct 1:1, one flat
TOML key per flag, same name with hyphens as underscores
(`persisted_message_ttl_secs`, not a nested table):

```toml
addr = "0.0.0.0:49500"
log_level = "info"
peer = ["node-b.internal:49500", "node-c.internal:49500"]
metrics_addr = "0.0.0.0:9090"
data_dir = "/var/lib/thoth-mesh"
tls_cert = "/etc/thoth-mesh/node-cert.pem"
tls_key = "/etc/thoth-mesh/node-key.pem"
tls_ca = "/etc/thoth-mesh/ca-cert.pem"
allow_peer = ["3F:08:CA:...:92:10"]
topic_acl = ["anonymous|sub|status.public"]
peer_topic_acl = []
peer_topic_filter = []
metrics_token_file = "/etc/thoth-mesh/metrics-token"
persisted_message_ttl_secs = 604800
dead_letter_topic = "dead-letter"
publish_rate_limit_per_sec = 100
publish_rate_limit_burst = 200
```

Every key is optional, same as ADR-0034 - an empty or partial file is
valid. `#[serde(deny_unknown_fields)]` again, for the same reason: a
typo'd key is far more likely than a deliberate forward-compatible
extra one.

### A repeatable flag becomes a TOML array; a non-empty CLI list wins outright over the file's

`--peer`/`--allow-peer`/`--topic-acl`/`--peer-topic-acl`/
`--peer-topic-filter` each become an array of strings, parsed exactly
the same way the flag's own repeated values already are (no new
parsing code - the merge step just decides *which* `Vec<String>`
reaches the existing parser). Precedence generalizes ADR-0034's own
`cli_value.or(config_value)` rule from `Option` to `Vec` the same way:
a non-empty CLI-supplied list is used as-is, in full, with the file's
array for that key ignored entirely; only an *empty* CLI list (the
flag never given at all) falls back to the file's array. Deliberately
not a union of both - merging would mean a restrictive `--topic-acl`
given on the command line silently fails to restrict anything if the
file already had a permissive list, which is the opposite of what
giving that flag explicitly should mean. This is the same reasoning
ADR-0034 already established for scalars, just generalized.

### Location: same mechanism and crate as ADR-0034, a different file

`directories::ProjectDirs::from("", "", "thoth-mesh")` again (already
a dependency, now shared by both crates rather than added twice), but
`node.toml` instead of `config.toml` under the same
`~/.config/thoth-mesh/` directory - two files, not two directories,
since both binaries belong to one project a user thinks of as one
thing. `--config <path>` overrides the conventional location, same as
the CLI's own flag. A missing file - at either kind of path - is not
an error, same as ADR-0034; a malformed one still is.

Considered and rejected: hardcoding `/etc/thoth-mesh/node.toml`
instead, matching where this daemon's other packaged config already
lives in practice (`packaging/thoth-mesh-node.env.example`'s
`/etc/thoth-mesh/node.env`, and every cert/key/token path
`docs/OPERATIONS.md`'s systemd example spells out explicitly). Two
reasons: it only serves the systemd-unit deployment case well (a
`DynamicUser` unit and most containers have no conventional `/etc` an
operator would drop a file into casually, and Windows has no `/etc`
at all), while `~/.config/thoth-mesh/` already works everywhere the
`directories` crate does, including the actually-common case this
mechanism mostly helps: running the daemon directly for local
development, the same audience ADR-0034's own conventional location
already serves well. A real systemd/Docker deployment is already
fully explicit about every other path (cert, key, metrics token) per
existing `docs/OPERATIONS.md` precedent - adding one more explicit
`--config /etc/thoth-mesh/node.toml` to `NODE_ARGS` costs nothing and
doesn't need a new, unprecedented "system config directory" concept
invented just for this.

### Precedence: CLI flag > config file > built-in default, exactly as ADR-0034

`addr` and `log_level` drop their `default_value_t`/`default_value`
and become `Option<String>` (same shape every other flag already had),
so "was this actually given" is answerable the same way ADR-0034 made
it answerable for the CLI's `addr`. The merge is one pass over `Cli`
right after parsing: each scalar is `cli.or(config).unwrap_or(built_in
default)`, each list is the rule above. `--tls-cert requires
--tls-key` and friends (`clap`'s `requires`/`requires_all`) still
catch a pure-CLI violation before the config file is ever touched, but
can't see a cross-source combination (`--tls-cert` on the CLI,
`tls_key` only in the file) - the merge step re-checks every one of
these same constraints (the TLS trio, `--allow-peer` requiring
`tls_cert`, `--metrics-token-file` requiring `metrics_addr`,
`--persisted-message-ttl-secs` requiring `data_dir`,
`--publish-rate-limit-burst` requiring `--publish-rate-limit-per-sec`)
against the *merged* effective values, failing the same way if exactly
one side of a pair ended up set. Same reasoning as ADR-0034, just a
longer list of pairs to recheck.

### No code sharing with `thoth-mesh-cli`'s `config` module

The two `Config` structs have zero overlapping fields in practice (the
CLI's `tls_*` trio means "how I identify myself when I connect
somewhere"; the node's means "how I prove my own identity to
everything that connects to me" - the same three field *names*, doing
conceptually adjacent but distinct jobs) and belong to different
crates already free of a dependency on each other. The only genuinely
shared thing is the ~20-line load/locate mechanism itself (read a
path, treat missing as empty, parse TOML, treat malformed as a hard
error) - not worth a new shared crate or a generic `Config<T>` for
that little code, duplicated once already at this size elsewhere in
this codebase's own stated preference for three similar lines over a
premature abstraction.

## Consequences

- New `crates/thoth-mesh-node/src/config.rs`: a `Config` struct
  mirroring every `Cli` flag, a `load`/`default_path` pair structurally
  identical to `thoth-mesh-cli`'s own (ADR-0034), and the merge/
  cross-field-recheck step `main` runs right after `Cli::parse()`.
- New `--config <path>` flag on `thoth-mesh-node`. `addr`/`log_level`
  become `Option<String>` internally (CLI-visible behavior unchanged -
  the same built-in defaults still apply with nothing given at all).
- `directories` becomes a dependency of `thoth-mesh-node` too (already
  a dependency of `thoth-mesh-cli`, same pinned version).
- Unblocks #143 (dynamic config reload): a `SIGHUP` handler re-running
  this same load-and-merge step against the now-existing file is the
  natural next ADR, not designed here.
- Nothing changes for an invocation with no config file anywhere it's
  looked - every flag's built-in default behavior is exactly as
  before this ADR.

Closes #167.
