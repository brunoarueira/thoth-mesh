# 27. Timing out the dial connect-and-handshake phase

## Status

Accepted

## Context

Issue #98, found during review of ADR-0026: that ADR bounds concurrent
outbound dial *attempts* to `DEFAULT_MAX_CONCURRENT_DIALS` (16) via a
semaphore, but the permit is held for the entire connect+TLS+handshake
phase in `dial_peer` (`peering.rs`), and none of that phase's three
steps has a timeout:

- `TcpStream::connect` to an address that's firewalled to silently
  drop packets (rather than actively refuse) can hang for the OS's own
  TCP connect timeout - tens of seconds to minutes, not milliseconds.
- `MaybeTlsStream::connect` (the TLS handshake) has no timeout - a
  peer that accepts the TCP connection but stalls mid-handshake hangs
  the permit indefinitely.
- `dial_handshake` (waiting for the peer's `Hello`) has no timeout
  either - a peer that completes TLS but never sends `Hello` hangs the
  permit forever.

Since a permit isn't released until one of these steps errors or
succeeds, enough slow or unresponsive peers can fully saturate the
16-permit semaphore and starve every other queued dial, including
dials to perfectly healthy peers, for as long as the stalls last.
ADR-0026 made this worse in one respect: before it, a stall on one
peer tied up one of many unbounded tasks; now it can occupy 1/16th of
the node's entire dial capacity, and 16 simultaneous stalls stop the
node from dialing anyone new at all.

## Decision

### Wrap the whole permit-held phase in a fixed `tokio::time::timeout`

A new constant, `DIAL_TIMEOUT` (10 seconds), bounds how long the
connect+TLS+handshake phase of `dial_peer` may run before the attempt
is abandoned. 10 seconds is generous for a real network path (well
above typical connect/handshake latency even under load) but well
short of the OS's own TCP connect timeout, so this timeout is actually
the one that fires. Not configurable via a flag in v1, consistent with
this codebase's other fixed capacities (`DEFAULT_MAX_CONCURRENT_DIALS`,
the replay buffer, the memory-footprint caps).

The permit-held work - `TcpStream::connect`, the optional TLS
handshake, and `dial_handshake` - is factored into its own async
function so it can be wrapped in one `tokio::time::timeout` call,
rather than adding a separate timeout around each of the three steps.
A single wrap covering all three is simpler and has the same effect:
none of the three should individually be allowed to run past the
budget, and there's no reason to give one step a different allowance
than another.

A timeout is treated exactly like any other dial failure at this
phase: logged via `tracing::warn!`, the permit dropped (implicitly,
falling out of scope), and the function returns. `dial_peer_with_
reconnect`'s existing backoff loop (ADR-0012) picks it up from there
the same way it already handles a refused connection or a failed
handshake - no new retry logic needed.

### Out of scope: timing out an established connection

`connection::handle_connection`'s lifetime - what runs after the
handshake completes - stays deliberately unbounded, the same posture
ADR-0025 already took for connected peers in `Membership` and
ADR-0026 already took for established links versus dial slots. This
ADR only bounds the time spent *becoming* a connection, never the
time spent *being* one.

## Consequences

A peer that's merely slow (not stuck) but takes longer than 10 seconds
to complete its handshake now fails that attempt and waits for the
next backoff-scheduled retry, rather than eventually succeeding on the
same attempt - an acceptable tradeoff, since retrying is cheap and the
alternative (no timeout) is the unbounded-starvation risk this ADR
closes. No wire-protocol change, no new metric - the existing `tracing
::warn!` on every failed dial phase already covers a timeout the same
way it covers any other failure; a dedicated timeout counter is easy
to add later if this proves to matter in practice.

This closes #98.
