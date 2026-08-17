# 14. Release readiness: lockstep versioning, unstable protocol, republish all five crates

## Status

Accepted

## Context

All 5 workspace crates (`thoth-mesh-core`, `thoth-mesh`,
`thoth-mesh-broker`, `thoth-mesh-node`, `thoth-mesh-cli`) are reserved
on crates.io at a stale `0.0.1` - `thoth-mesh-core` and the other three
were published before ADR-0005's wire protocol existed, and
`thoth-mesh-broker` was published later with `cargo publish
--no-verify` since it couldn't verify against the stale core. None of
them build correctly for an external consumer today: `thoth-mesh-core`
0.0.1 doesn't export `Envelope`, `Topic`, `MessageId`, `PeerId`,
`MessageKind`, or framing, and everything downstream depends on those.

Issue #37 (part of Phase 5, issue #19) left three questions open:

1. Do the crates keep moving in version lockstep
   (`version.workspace = true`, the current setup) or decouple?
2. Is the wire protocol declared stable, or explicitly marked
   unstable?
3. docs/ROADMAP.md's Phase 5 section only named `thoth-mesh-core` and
   `thoth-mesh-broker` as needing a real republish - but all 5 names
   turn out to be reserved at the same stale `0.0.1`. Which crates
   actually get a real release in this pass, and in what order?

## Decision

### Keep lockstep versioning

All 5 crates already share one version number and move together in
practice - every phase so far has touched multiple crates in the same
PR. Decoupling would mean tracking 5 independent version numbers and a
compatibility matrix for a single-repo, single-maintainer project
where the crates aren't independently useful to begin with
(`thoth-mesh` depends on `thoth-mesh-core`; `thoth-mesh-broker`
depends on `thoth-mesh-core`; `thoth-mesh-node` depends on all three;
`thoth-mesh-cli` depends on `thoth-mesh-core` and dev-depends on
`thoth-mesh-node`). There's no scenario today where a consumer would
want, say, `thoth-mesh-broker` 0.3 paired with `thoth-mesh-core` 0.1.
Lockstep is what the project already does; decoupling would be
speculative flexibility for a need that doesn't exist yet - the same
reasoning ADR-0011 and ADR-0013 already used to reject building ahead
of an actual requirement.

### Declare the wire protocol - and every crate's public API - explicitly unstable

Nothing about this project has external consumers yet, and the wire
protocol has changed in nearly every phase so far (ADR-0009's `Hello`
handshake, ADR-0011's interest-propagation messages) with nothing
ruling out the next phase changing it again. Semver's own convention
for this is 0.x: anything can break between 0.x releases without
violating semver, which is the honest state to publish in - a 1.0
would promise a stability this project hasn't earned yet.

So: bump the workspace version from the placeholder `0.0.1` to
`0.1.0` (signals "this is a real release," not a name-squat) and stay
in 0.x. Revisiting 1.0 is something to do once the protocol and each
crate's public API have gone through a release or two without a
breaking change - a state to observe later, not to declare now.

### Republish all 5 crates, in dependency order

Since versioning stays lockstep, leaving 3 of 5 crates on a fake
`0.0.1` while 2 move to a real `0.1.0` would immediately break the
"one version number for the whole workspace" property just decided
above. `thoth-mesh-node` and `thoth-mesh-cli` are also exactly the
crates an external `cargo install` would reach for, so they need to
actually build from the registry too, not just the two library crates
docs/ROADMAP.md originally called out. All 5 get a real `0.1.0`
release, published in dependency order so registry-based verification
succeeds at every step this time (unlike `thoth-mesh-broker`'s
original `--no-verify` publish - see the crates-io-publish-state
history):

1. `thoth-mesh-core` (no workspace dependencies)
2. `thoth-mesh` and `thoth-mesh-broker` (depend only on
   `thoth-mesh-core`) - either order
3. `thoth-mesh-node` (depends on `thoth-mesh-core`, `thoth-mesh`,
   `thoth-mesh-broker`)
4. `thoth-mesh-cli` (depends on `thoth-mesh-core`; dev-depends on
   `thoth-mesh-node`, which doesn't affect publishing since
   dev-dependencies aren't published)

Each path dependency's `version = "0.0.1"` requirement is bumped to
`"0.1.0"` alongside the workspace version, so `cargo publish` can
verify for real at each step instead of needing `--no-verify` again.

## Consequences

A consumer can `cargo add thoth-mesh-core`/`thoth-mesh-broker`, or
`cargo install thoth-mesh-node`/`thoth-mesh-cli`, and get code that
actually reflects `main` - the gap flagged since ADR-0003 is closed.

Every crate is explicitly 0.x, so nobody mistakes this republish for a
stability promise the project hasn't earned; a breaking wire-protocol
or API change in a future phase is a normal 0.x minor bump, not a
semver violation.

The version-bump changes land on `main` through the usual PR review.
The actual `cargo publish` calls are a separate, manual step after
that - crates.io publishes can only be yanked, never deleted, so it's
worth running them once the version going out is the one actually on
`main`, not mid-review.
