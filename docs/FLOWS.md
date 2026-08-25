# Flows

`PROTOCOL.md` and the ADRs describe thoth-mesh's runtime behavior in
prose. A few of those flows involve multiple nodes acting
concurrently and are easier to follow as a picture — this page is
diagrams only, cross-linked back to the ADRs that explain the
reasoning behind each one. Not a C4 model: thoth-mesh is one system,
and what's non-obvious here is intra-node/intra-mesh control flow, not
system-to-system boundaries.

**Status:** these track the reference implementation as of the ADRs
cited under each diagram. Like `PROTOCOL.md`, don't assume anything
here holds across a breaking change.

## Peer handshake

The dialing side always speaks first. Both sides learn the other's
`PeerId` (carried on every envelope's `sender` field, not just
`Hello`'s) and the address to dial back, if any. See
[ADR-0009](adr/0009-peer-handshake-shared-port.md).

```mermaid
sequenceDiagram
    participant D as Dialing node
    participant A as Accepting node

    D->>A: connect (TCP, optionally TLS)
    D->>A: Hello { listen_addr }
    Note over A: First message on a freshly<br/>accepted connection is Hello,<br/>so this is a peer link, not a client
    A->>D: Hello { listen_addr }
    Note over D,A: Both sides now know the other's<br/>PeerId (from sender) and listen_addr
```

If an `--allow-peer` allowlist is configured and rejects the far
end's certificate, the rejecting side sends `Error { in_reply_to:
<the Hello's id> }` instead of its own `Hello` and closes the
connection — this can happen on either side, including the dialer
rejecting the accepting side's reply `Hello`. See
[ADR-0017](adr/0017-peer-allowlist-via-tls-fingerprint.md).

## Interest propagation catch-up

A node's interest (aggregated across every client *and* peer
connection) is what gets propagated outward — not each individual
client subscription. A newly-formed peer link is caught up on the
full current snapshot, then kept current by every subsequent
zero-to-one/one-to-zero transition. See
[ADR-0011](adr/0011-interest-propagation-and-loop-prevention.md).

```mermaid
sequenceDiagram
    participant C as Client
    participant N as Node N (already has<br/>subscribers for topic T)
    participant P as New peer link

    Note over N,P: Handshake completes (dial or accept side)
    N->>P: Subscribe { T } (batch catch-up,<br/>one per topic N is interested in)
    P-->>N: Ack

    Note over C,N: Later: a new topic U goes<br/>from 0 to 1 interested connections
    C->>N: Subscribe { U }
    N-->>C: Ack
    N->>P: Subscribe { U } (broadcast to every<br/>active peer link, unconditionally)
    P-->>N: Ack
```

Every peer link a topic transition reaches gets the same
`Subscribe`/`Unsubscribe`, even the peer that caused the transition in
the first place — deliberately not special-cased, since each
connection's own forwarder map is already idempotent and that's what
actually bounds the flood.

## Gossip peer discovery and the auto-dial tie-break

A node only ever dials addresses it's told about (`--peer` or
discovery); this is how it learns about a peer-of-a-peer and closes
the loop into a direct link. See
[ADR-0015](adr/0015-dynamic-peer-discovery-gossip.md).

```mermaid
sequenceDiagram
    participant A as Node A
    participant B as Node B
    participant C as Node C

    Note over A,B: A-B already a direct peer link
    C->>B: connect + Hello (C dials in)
    B->>C: Hello
    Note over B: PeerDirectory.record(C) → true (new)
    B->>C: PeerAnnounce { peers: [A] } (catch-up,<br/>excludes C itself)
    B->>A: PeerAnnounce { peers: [C] } (broadcast:<br/>C is genuinely new to B)
    Note over A: PeerDirectory.record(C) → true (new)<br/>Tie-break: does node_id(A) < peer_id(C)?
    alt A's id sorts lower
        A->>C: connect + Hello (A auto-dials C)
        C->>A: Hello
        Note over A,C: Direct A-C link now up -<br/>mesh has fully converged
    else C's id sorts lower
        Note over A: A does not dial - waits for<br/>C's own tie-break to fire the same way
    end
```

Both sides evaluate the tie-break independently, from their own local
information, with no coordination — since `PeerId`s are distinct
UUIDs, exactly one side's comparison holds, so exactly one side dials.
A peer learned about via a *direct* `Hello` is never auto-dialed
(there's already a connection); only one learned about *indirectly*,
via `PeerAnnounce`, triggers this.

## Publish delivery across a cyclic mesh

A `MessageId` is generated once and kept unchanged across every hop —
this is what loop prevention and de-duplication key on. See
[ADR-0011](adr/0011-interest-propagation-and-loop-prevention.md).

```mermaid
sequenceDiagram
    participant Pub as Publisher (client of A)
    participant A as Node A
    participant B as Node B
    participant C as Node C
    participant Sub as Subscriber (client of C)

    Note over A,C: Cyclic mesh: A-B, B-C, and A-C all directly linked
    Pub->>A: Publish { T, payload } (MessageId = m1)
    A->>A: Broker::publish(m1) - not seen before, delivers/forwards
    par A forwards to both neighbors
        A->>B: Publish (m1)
    and
        A->>C: Publish (m1)
    end
    C->>Sub: Publish (m1) delivered - C has a subscriber for T
    B->>C: Publish (m1) (B forwards on, unaware C already has it)
    Note over C: Broker::publish(m1) again -<br/>m1 already in SeenIds, dropped.<br/>Sub is not delivered a duplicate.
```

The same dedup applies to the original local publish, not just
forwarded hops — `Broker::publish` doesn't distinguish "message I just
originated" from "message a peer handed me," which is what makes this
safe on an arbitrary cyclic topology without any per-topology
reasoning.

## Connection lifecycle

There's one listener and one wire format; what a connection *is* — a
plain client or a peer link — is purely behavioral, decided by
whichever message arrives first. See
[ADR-0009](adr/0009-peer-handshake-shared-port.md) and
[ADR-0010](adr/0010-peer-links-share-client-dispatch.md).

```mermaid
stateDiagram-v2
    [*] --> Accepted: TCP accept (or outbound connect)
    Accepted --> AwaitingFirstMessage

    AwaitingFirstMessage --> PeerHandshake: first message is Hello
    AwaitingFirstMessage --> ClientSteadyState: first message is<br/>Subscribe/Publish/Unsubscribe

    PeerHandshake --> PeerRejected: --allow-peer configured<br/>and certificate missing/unlisted
    PeerRejected --> [*]: Error sent, connection closed

    PeerHandshake --> PeerSteadyState: Hello exchange completes

    state PeerSteadyState {
        [*] --> Relaying
        Relaying --> Relaying: Subscribe/Unsubscribe (own or<br/>relayed interest) / Publish / PeerAnnounce
    }

    state ClientSteadyState {
        [*] --> Serving
        Serving --> Serving: Subscribe/Publish (--topic-acl-<br/>permitted) / Unsubscribe
        Serving --> Serving: Subscribe/Publish rejected by<br/>--topic-acl - Error sent,<br/>connection stays open
    }

    PeerSteadyState --> [*]: peer disconnects -<br/>implicit Unsubscribe per<br/>held topic, PeerLinks/<br/>Membership updated
    ClientSteadyState --> [*]: client disconnects -<br/>implicit Unsubscribe per<br/>held topic

    AwaitingFirstMessage --> [*]: malformed frame/envelope -<br/>connection closed, no Error sent
```

The key asymmetry between the two steady states: a peer-link rejection
(`--allow-peer`) always closes the connection, while a client
authorization rejection (`--topic-acl`) never does — a client denied
on one topic may be entitled to others. See
[ADR-0017](adr/0017-peer-allowlist-via-tls-fingerprint.md) and
[ADR-0018](adr/0018-per-topic-client-authorization.md).
