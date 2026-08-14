# Ordinary-row stream storage contract

Jazz streams are a TypeScript library data structure built entirely from
ordinary Jazz rows. The core, wire protocol, storage drivers, authorization,
branching, and history have no stream-specific path.

## Logical value

Every stream row version is a complete, independently readable snapshot:

```text
StreamSnapshot {
  rootId:        id of an immutable extent-tree root, or ""
  prefixBytes:   logical bytes covered by rootId
  inlineTail:    bounded recent bytes
}
```

Reading a snapshot follows only its `rootId` and appends its `inlineTail`. It
MUST NOT find or replay an earlier stream history version. Therefore historical
and branch reads inherit ordinary Jazz row-version semantics rather than adding
a second stream history.

## Physical ordinary rows

The conventional schema contains:

- `streams(rootId, prefixBytes, inlineTail)` — one mutable logical root;
- `stream_nodes(childIds, childLengths, height)` — immutable persistent tree
  nodes; height zero children are segment ids, other children are node ids;
- `stream_parts(data)` — immutable bounded byte segments.

The tree has bounded fanout. An append copies only the right spine. Small
appends update the bounded tail. The transaction that would cross the tail
limit writes one or more bounded immutable parts, copies the tree path, clears
the tail, and advances the root atomically.

## Public behavior

- `create()` creates an empty ordinary stream row.
- `append()` is an exclusive transaction so concurrent appenders cannot silently
  discard one another. It waits for authority acceptance by default; conflict
  rejection is surfaced to the caller for retry.
- `snapshot()` returns the complete root tuple.
- `read()` and `readRange()` materialize only the addressed snapshot/tree and
  requested segment ranges.
- `subscribe()` subscribes to the ordinary stream row and emits complete root
  tuples; consumers may range-read bytes after each root change.

Parts are at most 1 MiB, matching the ordinary `BYTEA` cell limit. The default
tail limit is 64 KiB and fanout is 32. These are library placement choices, not
replicated-format constants.

## Authorization and lifecycle

The library does not bypass row authorization. Applications define permissions
on all three conventional tables. A usable deployment MUST ensure that a writer
authorized to advance a stream root may atomically insert the referenced nodes
and parts; a reader authorized for a stream must also be able to read its
reachable immutable rows. General transaction-created-child authorization is a
separate Jazz policy capability and is not hidden inside this library.

Ordinary history retaining a stream root keeps its referenced content logically
live. Automatic reachability collection is not part of the first slice; deleting
a stream does not claim to delete rows still reachable from retained history or
branches.

## Invariants

1. `prefixBytes` equals the total length under `rootId`.
2. `inlineTail.length <= tailLimit` after every committed append.
3. Node `childIds` and `childLengths` have equal non-zero lengths; a node length
   is the sum of its child lengths.
4. Nodes and parts are immutable after insertion.
5. A range result preserves append order and equals the corresponding slice of
   a full read.
6. A saved snapshot remains readable after later appends without consulting
   stream-row history.
