# SQLite remote read freshness

A new remote attachment now starts a new upstream subscription round trip,
including when another observer retains the same query. Previously it could
reuse that observer's old global settlement marker. The shared attachment keeps
its observers and reference count, but gets a new request ID and clears its
settlement state. Replies to the retired ID cannot complete the new request.
Relays do the same at each hop.

A relay also honours upstream revocation when deciding whether to ship a cached
row or acknowledge a reconnect's `Hold`. A retained image can still pass the
relay's local policy after the authority has revoked it. That image no longer
causes the relay to re-admit the row downstream. The revocation lookup uses the
existing `(table, row, branch)` primary key; reconnect checks remain batched over
the rows the client declared.

Accepted writes now retain permission watches even if their writer never reads
or subscribes to the resulting rows. When a supporting membership changes, the
normal sync path rechecks those rows just as it rechecks fetched rows. No new
whole-table scan is introduced.

For ordinary one-shot Remote reads, local evaluation is constrained to the root
IDs in that request's response. An empty response therefore cannot return an
old cached write. The original offset and limit still apply over the hydrated
prefix; aggregates constrain their inputs, and flat joins qualify the root ID
by its source alias. Local-first reads continue to include pending writes.

## Worked cases

- Alice retains a live note subscription, and Bob revokes her grant. A new
  Remote read waits for a new authority response and returns no note. This holds
  through both ordinary and private storage owners.
- Alice's tab disconnects before Bob's revocation. Her owner receives it while
  the tab is away. On reconnect, the owner's cache check rejects the stale held
  row. A later grant restores the note through the existing live subscription.
- Alice writes a note and never reads it. Bob removes her accepted group grant.
  Her next Remote one-shot read or subscription excludes the retained write.

## Encoding and validation

No storage schema, wire tags, serialized fields or durable bytes change. This
uses the existing Subscribe, Unsubscribe, Hold, Unlist and Answer messages and
existing revocation table. All hops need the matching implementation for these
freshness guarantees; there is no negotiation or experimental-store migration.

The public Rust regressions are in `sqlite_remote_read_freshness.rs` and
`sqlite_candidate_relations.rs`. They cover delayed authority responses,
retained subscriptions, reconnects, restored grants and an author's unread
accepted write. Existing flat-join/history coverage also checks the qualified
root filter. The native E2EE group-administration test exercises revocation and
restoration through the SDK with real authority receipts.
