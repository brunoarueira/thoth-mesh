# 36. `thoth-mesh-cli` shell completions

## Status

Accepted

## Context

`thoth-mesh` has no tab completion in any shell. Filed as #56 (Phase
10, docs/ROADMAP.md), which already named `clap_complete` - the
standard companion crate for a `clap`-derived CLI, generating
completions straight from the existing `Cli`/`Command` definitions -
and flagged this as likely too small a decision to need a full ADR.
It turned out to still have two worth writing down: where the
generation is triggered from, and how far this goes toward actually
installing the result.

## Decision

### A `completions <SHELL>` subcommand, not a build-time artifact

`thoth-mesh completions <shell>` writes that shell's completion script
to stdout and exits - handled inside `run()`, short-circuiting before
any of the connection-related work (parsing `--addr`/config, dialing a
node) that every other subcommand needs, since generating a
completion script touches neither the network nor a config file.

The alternative - generating completion scripts at build time via a
`build.rs`, and shipping them as packaged files (in the crates.io
tarball, or as extra files alongside the systemd/Docker packaging from
ADR-0023) - was passed over: it would need to be regenerated and
re-packaged every time a flag or subcommand changes, whereas a runtime
subcommand is always in sync with the binary that generates it, for
free, and needs no packaging changes at all.

`shell` takes `clap_complete::Shell` directly - bash/zsh/fish (what
the issue asked for) plus elvish/PowerShell, which the crate already
supports at no extra cost.

### Generating, not installing

`thoth-mesh completions <shell>` prints a script; it doesn't try to
locate the right completion directory for the caller's shell/OS/package
manager and write there itself. That's meaningfully more
platform-specific work (and a filesystem write from a CLI invocation
that every other subcommand only ever does over the network) for
something the user redirects once, the same way they'd install
completions for any other CLI:

```sh
thoth-mesh completions bash | sudo tee /etc/bash_completion.d/thoth-mesh
thoth-mesh completions zsh > "${fpath[1]}/_thoth-mesh"
thoth-mesh completions fish > ~/.config/fish/completions/thoth-mesh.fish
```

### Two bugs this surfaced, not just wiring

Actually loading a generated script in each real shell (not just
checking that `clap_complete::generate` didn't error) caught two
pre-existing issues `--help`/`--version` never would have:

- `Cli` had no explicit `#[command(name = ...)]`, so `clap` defaulted
  it to `CARGO_PKG_NAME` - `thoth-mesh-cli`, the crate, not
  `thoth-mesh`, the binary everyone actually runs (`--version` printed
  "thoth-mesh-cli 0.1.0" before this). Harmless in isolation, but every
  generated script registered its completion function under the wrong
  command name - fixed by setting the name explicitly.
- `OutputMode::Raw`'s doc comment (the `--output raw` value's help
  text) contained a literal `"Subscribed to ..."`. `clap_complete`
  embeds doc-comment help text into each shell's script verbatim; a
  double quote inside one, embedded inside the fish generator's own
  double-quoted `-d`/`-a` arguments, breaks the script's quoting and
  fails to `source` at all. Fixed by rewriting that one doc comment to
  avoid embedded double quotes, and added a test that walks the whole
  `Cli` command tree (recursively, so it stays covered as the CLI
  grows) checking every piece of help text for an embedded `"`, rather
  than trusting doc-comment content by convention alone.

Neither is exercised by installing `fish`/`bash-completion` in CI -
the doc-comment-quoting test above is the portable stand-in for
"actually load the generated script," without a new CI dependency.

## Consequences

Tab completion for every current and future flag/subcommand comes for
free from `clap`'s existing derive - nothing to hand-maintain as the
CLI's surface grows, aside from keeping doc comments free of embedded
double quotes (now enforced by a test, not just convention). New
`clap_complete` dependency, version-matched to the existing `clap =
"4"`.

Closes #56.
