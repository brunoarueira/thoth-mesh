# 34. `thoth-mesh-cli` config file for connection options

## Status

Accepted

## Context

`--addr`, and the `--tls-ca`/`--tls-cert`/`--tls-key` trio, have to be
repeated on every `thoth-mesh` invocation - tedious for anything but a
default local plaintext node, and error-prone for the TLS flags, which
already have to agree with each other. Filed as #54 (Phase 10,
docs/ROADMAP.md), which already sketched the answer: a config file
supplies defaults for these, CLI flags still override them.

## Decision

### Format: TOML, four flat optional keys

TOML, matching the Rust ecosystem norm the issue itself pointed at. No
nested tables - today's config options are exactly today's global
connection flags, so a flat file mirrors them 1:1:

```toml
addr = "127.0.0.2:49500"
tls_ca = "/etc/thoth-mesh/ca.pem"
tls_cert = "/etc/thoth-mesh/client.pem"
tls_key = "/etc/thoth-mesh/client.key"
```

Every key is optional - an empty or partial file is valid, same as not
having one at all. `#[serde(deny_unknown_fields)]` on the `Config`
struct rejects an unrecognized key rather than silently ignoring it -
a typo'd key (`tls_key` misspelled `tls_ky`) is far more likely than a
deliberate forward-compatible extra field, and the former is worth
surfacing as an error.

### Location: `directories::ProjectDirs`, overridable with `--config`

The conventional per-OS config directory (`~/.config/thoth-mesh/config.toml`
on Linux, and the platform-appropriate equivalent on macOS/Windows),
via the `directories` crate - small, single-purpose, and about as
established as a crate gets, so worth the new dependency despite this
project's general minimalism (see issue #54's own framing of that
tradeoff). Hand-rolling `$XDG_CONFIG_HOME`-and-friends logic ourselves
would just be a worse, less-tested version of what this crate already
does.

A new global `--config <path>` flag overrides the conventional
location outright, for anyone who wants a non-default path (or
multiple named profiles, invoked as `--config foo.toml`).

### Missing file: not an error, anywhere it's looked for

Whether the path came from `--config` or the conventional default, a
config file that doesn't exist is treated the same as an empty one -
not an error. Applying that uniformly (rather than "missing default
path is fine, missing `--config` path is an error") keeps the loading
logic to one code path with one fallback branch, and it's the more
useful default besides: most invocations of `thoth-mesh` will never
have created a config file at all, and that has to already be the
common, silent case for the conventional location to be usable out of
the box. A config file that exists but fails to parse is still a hard
error, at either location - unlike a merely absent file, a malformed
one is never intentional.

### Precedence: CLI flag > config file > `DEFAULT_ADDR`

Standard `clap`/most-CLIs convention, as the issue already named. To
make "was this flag actually given" answerable, `Cli::addr` drops its
`default_value = DEFAULT_ADDR` and becomes `Option<String>`, same
shape the `--tls-*` flags already had. The merge itself is a plain
`cli_value.or(config_value).unwrap_or(built_in_default)` per field,
done once in `run()` right after parsing the config file.

`--tls-cert requires --tls-key` (and vice versa) is still enforced by
clap for a pure-CLI invocation - unchanged, and it still fails before
ever touching the config file. But the pairing can now span both
sources (`--tls-cert` on the CLI, `tls_key` from the config file, or
either direction), which clap's `requires` can't see across, so the
merge step re-checks the same constraint on the merged, effective
values and fails the same way if exactly one of the pair ended up set.

## Consequences

`thoth-mesh --config ~/.config/thoth-mesh/config.toml subscribe ...`
works today with no config file present at all (nothing to merge, same
behavior as before this change); dropping a config file at the
conventional path removes the need to repeat `--addr`/`--tls-*` on
every invocation after that. A new `directories` dependency, and
`toml`/`serde` (the latter already a dependency of thoth-mesh-core,
now also a direct one of thoth-mesh-cli for `Config`'s `Deserialize`).

If a config option beyond connection settings shows up later (output
formatting, once #55 exists, say), it's a new flat key on the same
`Config` struct - nothing about this shape assumes it's connection-only,
that's just all `Config` needs to hold today.

Closes #54.
