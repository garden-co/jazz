# Edge Catalogue Pull On Reconnect Design

## Context

Core/edge deployments already treat the core server as the catalogue authority.
Catalogue HTTP requests sent to an edge are forwarded upstream after local admin
secret validation, and edge servers connect to core as trusted peer clients over
the existing WebSocket sync transport.

The current sync path can avoid pushing unchanged local catalogue state to an
upstream server when catalogue hashes match, but a newly connected or stale edge
still needs an explicit way to learn the core catalogue after reconnect. The
catalogue is small enough that a full replay on mismatch is acceptable.

## Goal

On every edge reconnect, ensure the edge has the same catalogue state as core
without letting edge catalogue state become authoritative.

The common reconnect path should be cheap: if both sides already have the same
catalogue digest, no catalogue entries should be sent.

## Non-Goals

- Add per-entry catalogue manifests or incremental diffs.
- Add a new HTTP catalogue pull API.
- Let edge servers publish catalogue entries to core over sync.
- Preserve any edge-local catalogue entries that conflict with core truth.

## Protocol

Use the existing WebSocket handshake digest exchange.

1. The edge computes its local `catalogue_state_hash` from durable catalogue
   storage and includes it in `AuthHandshake`.
2. The core authenticates the connection as `ClientRole::Peer` through the peer
   secret.
3. Before sending normal queued sync traffic, the core compares the edge hash
   with its own current `catalogue_state_hash`.
4. If the hashes match, the core sends no catalogue replay.
5. If the edge hash is missing, invalid, or different, the core queues an
   authoritative `CatalogueSnapshot` for this app containing every catalogue
   entry from core storage.
6. The edge applies this snapshot through the existing `Source::Server` path,
   persists the core entries, prunes local catalogue entries for the same app
   that are absent from the snapshot, and rebuilds `SchemaManager` catalogue
   state from storage.

This is an authoritative full catalogue pull semantically, even though
transport delivery is implemented as core-to-edge sync after the edge presents
its digest. A mismatch converges the edge catalogue set to exactly the core set
for that app, rather than only filling missing entries.

## Authority Rules

The core remains source of truth for catalogue state.

Core must not accept `CatalogueEntryUpdated` from edge peers as authoritative
publication. Edge-origin catalogue publication continues to use the existing
HTTP forwarding path: an admin request sent to an edge is validated locally,
forwarded to the core, and accepted or rejected by core.

Catalogue entries received by an edge from `Source::Server` are trusted because
the upstream connection is authenticated with the peer secret. Catalogue entries
received by core from an edge peer should be ignored or rejected for catalogue
authority purposes.

## Existing Core Propagation

When core accepts a local/admin catalogue publish, the existing write path should
continue to propagate it to connected edges:

- `SchemaManager` persists schema, lens, and permissions catalogue entries via
  `SyncManager::upsert_catalogue_entry`.
- `upsert_catalogue_entry` persists changed entries and queues them to connected
  servers and clients.
- Edges already connected to core receive the entry immediately.
- Edges that were offline receive the entry during the reconnect replay if their
  digest no longer matches core.

The implementation should add integration coverage to lock this behaviour down,
including the offline/reconnect case.

## Data Flow

```text
edge reconnects
  -> AuthHandshake { catalogue_state_hash: edge_hash, peer_secret }
  -> core authenticates peer
  -> core compares edge_hash with core_hash
  -> mismatch: core scans catalogue storage
  -> core sends CatalogueSnapshot { app_id, entries }
  -> edge persists core entries
  -> edge prunes same-app catalogue entries absent from the snapshot
  -> edge rebuilds SchemaManager catalogue state from storage
```

For matching hashes:

```text
edge reconnects
  -> AuthHandshake { catalogue_state_hash: edge_hash, peer_secret }
  -> core_hash == edge_hash
  -> no catalogue replay
```

## Efficiency

The steady-state reconnect cost is one digest comparison after the existing
handshake. No catalogue entries are encoded or sent when hashes match.

The mismatch path scans and sends the full catalogue to one reconnecting edge.
That is acceptable for the prototype because catalogue mismatches should be rare
and catalogue size is expected to stay small.

`catalogue_state_hash` should be cheap enough for reconnect storms. The existing
todo about caching the catalogue state hash behind a dirty flag remains relevant
and can be handled separately.

## Error Handling

If the core cannot scan catalogue storage during mismatch replay, it should leave
the edge connected and log the failure. The next reconnect or the next accepted
catalogue publish can repair the edge.

If a snapshot catalogue entry fails to persist on the edge, the existing
`persist_catalogue_entry` warning behaviour applies. If pruning fails, the edge
logs the failed object id and keeps the entry. In both cases the digest should
remain different, causing a future reconnect to retry the snapshot.

## Testing

Prefer integration tests around real edge/core runtime sync.

Coverage should include:

- A fresh edge connects to a core with existing schema, lens, and permissions
  catalogue entries and receives them without a client query.
- A reconnecting edge with a matching catalogue hash does not receive catalogue
  replay.
- A reconnecting edge with a stale or empty catalogue hash receives an
  authoritative catalogue snapshot.
- A reconnecting edge with extra same-app catalogue entries prunes them and
  converges to the core catalogue hash.
- A catalogue publish accepted by core propagates to already connected edges.
- A catalogue publish sent to an edge is forwarded to core, then reaches peer
  edges from core.
- Core does not accept edge-origin `CatalogueEntryUpdated` as authoritative
  catalogue publication over sync.

## Implementation Notes

The likely code path is the server-side WebSocket connection setup, where the
core already has the client's handshake and the authenticated role. For peer
connections, use the handshake `catalogue_state_hash` to decide whether to queue
an authoritative catalogue snapshot for that edge connection.

The edge-side `RuntimeCore::add_server_with_catalogue_state_hash...` path should
stop treating peer-secret edge connections as permission to publish catalogue
upstream. Edge/core topology should distinguish "this link may receive catalogue
from core" from "this runtime may publish catalogue to its upstream".
