# Contributing

thoth-mesh is a learning project (see the [README](README.md)), but it
follows the same conventions a larger project would - partly because
that's the point, and partly because it keeps the history readable.
This document is where those conventions are written down.

## Where the work comes from

Work is planned in [`docs/ROADMAP.md`](docs/ROADMAP.md): the project is
broken into phases, each tracked as a GitHub Milestone with a
`[Tracking]` issue describing its goal and known shape. Concrete,
ready-to-implement work is its own issue, either filed under a phase's
milestone or - for work that doesn't depend on any particular phase -
left unmilestoned.

One issue becomes one branch becomes one PR. Work doesn't start on the
next issue until the current one is merged.

## Architecture decisions

A real design decision - something with more than one reasonable
answer, where the reasoning is worth keeping - gets an
[ADR](docs/adr/) before implementation starts: [Nygard-style](docs/adr/0001-record-architecture-decisions.md),
numbered sequentially, indexed in [`docs/adr/README.md`](docs/adr/README.md).
Once an ADR's status is `Accepted`, it isn't edited - if a later
decision changes course, that's a new ADR that supersedes it, not an
edit to the old one. Not every change needs one: a bug fix or a
straightforward addition that doesn't involve a real design choice
doesn't.

To add one: copy the format of an existing ADR, give it the next
number, and add a row to the index.

## Local development

Before opening a PR, run what CI runs:

```sh
cargo fmt --all -- --check
cargo build --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo test --workspace --all-features --locked
```

CI (`.github/workflows/ci.yml`) runs the same commands on every push
and PR; `cargo-audit` (`.github/workflows/audit.yml`) separately checks
dependencies against the RustSec advisory database whenever
`Cargo.toml`/`Cargo.lock` change, plus weekly on a schedule.

## Commits and PRs

Commit/PR titles follow the pattern already in `git log`: an imperative
summary, prefixed with the crate or area it touches (`thoth-mesh-node:
...`, `docs: ...`, `release: ...`), with an `(ADR-XXXX)` suffix when
the change implements one. PRs are squash-merged, so the PR title is
what ends up in `main`'s history - make it count.

Reference the issue a PR closes (`Closes #N.`) in the PR body.

## License

thoth-mesh is dual-licensed under [MIT](LICENSE-MIT) and
[Apache-2.0](LICENSE-APACHE). By contributing, you agree your
contribution is licensed under the same terms.
