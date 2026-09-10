# AGENTS.md

Orientation for an AI coding agent working in this repo. It points at
the files that are the source of truth rather than restating them, so
it can't drift out of sync with them — when in doubt, the linked file
wins over this one.

## What this is

thoth-mesh is a federated publish/subscribe mesh written in Rust, and
a learning vehicle for going deep on async networking, concurrency,
protocol design, and distributed systems. It's a Cargo workspace of
five libraries and two binaries. See [`README.md`](README.md) for the
rest.

## Read before you change anything

1. [`README.md`](README.md) — what the project is, the crate layout,
   where the other docs live.
2. [`docs/ROADMAP.md`](docs/ROADMAP.md) — the phased plan; where the
   issue you're working on fits, and what's deliberately *not* in
   scope.
3. [`CONTRIBUTING.md`](CONTRIBUTING.md) — the workflow, in full. The
   summary below is just a summary.

For depth on a specific area:
[`PROTOCOL.md`](PROTOCOL.md) (the wire protocol, implementation-
independent), [`docs/adr/`](docs/adr/) (why things are the way they
are — start at [`docs/adr/README.md`](docs/adr/README.md)),
[`docs/FLOWS.md`](docs/FLOWS.md) (runtime flows as diagrams),
[`docs/OPERATIONS.md`](docs/OPERATIONS.md) (running a mesh end to
end).

## Workflow

Full version in [`CONTRIBUTING.md`](CONTRIBUTING.md). The essentials:

- **One issue → one branch → one PR.** Don't start the next issue
  until the current one is merged.
- **Never push directly to `main`.** Every change lands through a PR
  that gets reviewed. This is not negotiable even for a one-line doc
  fix.
- **ADR before implementation for real design decisions** — anything
  with more than one reasonable answer where the reasoning is worth
  keeping. [Nygard-style](docs/adr/0001-record-architecture-decisions.md),
  numbered sequentially, added to [`docs/adr/README.md`](docs/adr/README.md).
  A bug fix or a straightforward addition doesn't need one.
- **An accepted ADR is never edited.** If a later decision changes
  course, that's a new ADR that supersedes the old one.
- **Commit/PR titles**: imperative summary prefixed with the crate or
  area (`thoth-mesh-node: ...`, `thoth-mesh-broker: ...`, `docs: ...`,
  `release: ...`), with an `(ADR-XXXX)` suffix when the change
  implements one. PRs are squash-merged, so the PR title becomes
  `main`'s history.
- **PR body** references the issue it closes: `Closes #N.`

## Verify locally before opening a PR

CI (`.github/workflows/ci.yml`) runs exactly these on every push and
PR — run them yourself first:

```sh
cargo fmt --all -- --check
cargo build --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo test --workspace --all-features --locked
```

`cargo-audit` (`.github/workflows/audit.yml`) separately checks
dependencies whenever `Cargo.toml`/`Cargo.lock` change, and weekly.

## Workspace map

| Crate | Kind | Purpose |
| --- | --- | --- |
| [`thoth-mesh-core`](crates/thoth-mesh-core) | lib | Core protocol types and wire format shared by everything else. |
| [`thoth-mesh-broker`](crates/thoth-mesh-broker) | lib | In-process pub/sub dispatch: per-topic broadcast to subscribers. |
| [`thoth-mesh`](crates/thoth-mesh) | lib | Federation/gossip layer: peer discovery, membership, replication. |
| [`thoth-mesh-tls`](crates/thoth-mesh-tls) | lib | TLS transport helpers: certificate loading, config, and a plaintext/TLS stream shim. |
| [`thoth-mesh-node`](crates/thoth-mesh-node) | bin | Daemon that runs a mesh node over a network transport. |
| [`thoth-mesh-cli`](crates/thoth-mesh-cli) | bin | Command-line client (`thoth-mesh` binary) for publishing, subscribing, and admin. |

## Working style expected of an agent

- **Ask before non-obvious design decisions.** If a task has more than
  one reasonable approach, surface the choice and the trade-offs
  rather than silently picking one and presenting a finished diff.
  (Often the answer is "write the ADR first.")
- **Smallest change that closes the issue.** No opportunistic
  refactors riding along in the same PR.
- **Out-of-scope findings become separate issues.** If you notice a
  bug or an improvement outside the issue you're on, propose it as its
  own issue instead of fixing it inline.
- **Breaking changes are allowed, but deliberate.** Every crate is
  pre-1.0 and the wire protocol is explicitly unstable
  ([ADR-0014](docs/adr/0014-release-readiness-versioning-and-republish.md)) —
  a breaking change is fine when it's the right call and recorded,
  not something to contort the design to avoid.

## License

Dual-licensed under [MIT](LICENSE-MIT) and [Apache-2.0](LICENSE-APACHE).
By contributing, you agree your contribution is licensed under the
same terms.
