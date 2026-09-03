# 32. Git tags and GitHub Releases for workspace version bumps

## Status

Accepted

## Context

The workspace has bumped its (lockstep, ADR-0014) version exactly once
in git history - `0.0.1` to `0.1.0` in #38 - and that bump never got a
git tag or a GitHub Release. `git tag -l` and `gh release list` are
both empty. Auditing crates.io while looking into this also turned up
a second, unrelated gap: `thoth-mesh-tls` (added in #73/ADR-0016,
after #38's republish) has never been published to crates.io at all,
even though it carries the same lockstep `0.1.0` the other five crates
are actually live at. Neither gap was caught at the time because
nothing checks for either - a version bump landing on `main` and a tag
existing for it are two unrelated facts today, and the same is true
for a crate's `Cargo.toml` version and whether that version is
actually live on crates.io.

Fixing the missing `0.1.0` tag/release retroactively, and publishing
`thoth-mesh-tls` for real, is #110. This ADR - and its implementation,
#111 - is about not repeating the tag/release half of the gap on the
next bump.

## Decision

### One tag per workspace version, named `v<version>`

Lockstep versioning (ADR-0014) means there's exactly one version
number for the whole workspace at any point in history, so there's
exactly one tag per bump - not five (or six) per-crate tags. `v0.1.0`,
`v0.2.0`, and so on, matching the conventional `v`-prefixed format
`actions/checkout` and most tooling expect without configuration.

### A GitHub Release per tag, with auto-generated notes

`gh release create <tag> --generate-notes` builds release notes from
every merged PR since the previous tag, using GitHub's own "generate
release notes" feature - no changelog file to hand-maintain (the
project has never kept one, and PR titles/ADR references already
describe what shipped, same information a changelog would duplicate)
and no new convention for anyone writing a PR description to learn.

### Automated: a new CI workflow tags and releases on every version bump landing on `main`

A new `.github/workflows/release.yml`, triggered on push to `main`,
does the following on every run:

1. Read the workspace version out of `Cargo.toml`.
2. Check whether tag `v<version>` already exists. If it does, this
   push didn't bump the version (or already got tagged) - stop, no-op.
3. Otherwise: create and push the tag, then `gh release create
   <tag> --generate-notes`.

Checking "does the tag already exist" rather than diffing this push's
`Cargo.toml` against its parent is what makes the workflow idempotent
and safe to just always run on `main` - it doesn't need to reason
about squash-merge commit ranges, force-pushes, or how many commits a
given push contains, only "is there a tag for the version that's on
`main` right now." A rerun, a second push that doesn't touch the
version, or main advancing without a version bump are all no-ops for
the same reason.

### `cargo publish` stays a separate, manual step - unchanged from ADR-0014

ADR-0014 already decided this and nothing here revisits it: a
crates.io publish can be yanked but never deleted, so running it
automatically the instant a version bump lands on `main` removes the
"actually run this once the bumped version is really what's on `main`"
deliberateness that decision was about. This workflow only creates the
tag and the GitHub Release; publishing to crates.io (in the dependency
order ADR-0014 established) is still a person running `cargo publish`
by hand, same as every release so far.

## Consequences

Closes #111. The next version bump gets a `v<version>` tag and a
GitHub Release automatically, closing the actual gap this was filed
over - without
introducing a changelog file to maintain or touching the existing
manual `cargo publish` step. `release.yml` needs `contents: write` (to
push a tag and create a release) - the first workflow in this repo
that isn't read-only permissions, since `ci.yml` and `audit.yml` never
needed to write to the repo.

If the workspace ever moves off lockstep versioning (ADR-0014 revisited),
this workflow's "one version bump, one tag" assumption goes with it -
not a concern today, since nothing in this project's current shape
motivates decoupling.
