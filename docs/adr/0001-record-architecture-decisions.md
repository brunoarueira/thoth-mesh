# 1. Record architecture decisions

## Status

Accepted

## Context

Significant architecture decisions get made and discussed as the project
progresses, but that discussion doesn't leave a browsable trail once it's
over. Future contributors (including a future version of the author) need
to understand *why* a decision was made, not just what the current state
of the code is.

## Decision

We will use Architecture Decision Records (ADRs), as described by
[Michael Nygard](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions),
to record every significant architecture decision made on this project.

ADRs live under `docs/adr/`, one file per decision, named
`NNNN-title-with-dashes.md` and numbered sequentially. Each uses the
lightweight Nygard format: Title, Status, Context, Decision, Consequences.
An ADR is never edited after it's accepted — if a decision changes, a new
ADR supersedes it and says so explicitly.

## Consequences

Decisions and their rationale become discoverable in-repo instead of
living only in chat history or people's memory. This requires discipline:
an ADR needs to be written at the time a significant, hard-to-reverse
decision is made, not reconstructed much later when the reasoning has
been forgotten.
