# Stream adapter on an embedded content manifest

This document scopes the stream-specific layer of the ordinary-content stack.
It deliberately assumes, but does not define, the foundation's atomic embedded
content-manifest column and its materializer hooks.

## Value carried by an owning Jazz row

For a column declared as a stream, one version of its owning row carries one
complete stream snapshot:

```text
StreamManifest {
  root:      StreamRootId | null
  editTail:  Uint8Array
}
```

`root` identifies an immutable byte-tree root. `editTail` is one bounded,
append-only byte suffix. The pair is one atomic column value: a merge strategy
or index implementation must materialize the pair before making a decision;
it must never select `root` and `editTail` from independent candidates.

The owning row is the mutable identity. Copying a manifest produces an exact
historical snapshot. A separate stream-head row is neither required nor
created by this adapter.

## Immutable rows

The adapter owns these content-addressed immutable row shapes, all scoped to
the foundation's authorization/encryption domain and canonical encoding
version:

```text
StreamPart {
  id: hash("jazz.stream.part.v1", domain, bytes)
  bytes: Uint8Array                 // at most 1 MiB
}

StreamNode {
  id: hash("jazz.stream.node.v1", domain, height, childIds, childByteLengths)
  height: integer
  childIds: StreamPartId[] | StreamNodeId[]
  childByteLengths: integer[]
}

StreamRoot {
  id: hash("jazz.stream.root.v1", domain, treeRoot, prefixBytes)
  treeRoot: StreamNodeId | null
  prefixBytes: integer
}
```

All lengths and ordered child IDs participate in canonical hashing. An
existing derived ID is idempotent only if its complete canonical payload is
identical; a conflicting payload is corruption, not an ordinary insert
conflict. `prefixBytes` is the immutable-byte count represented by `root` and
does not include `editTail`.

## Operations

- **append(owner, bytes):** materialize the current manifest. If the appended
  tail remains within the configured cap, replace only the owning row's
  manifest with the same `root` and a new immutable byte tail. If it crosses
  the cap, promote the old tail plus append into bounded content-addressed
  parts, path-copy the right tree spine, insert a new root, and replace the
  manifest with that root and an empty tail in the same owning-row transaction.
- **materialize(manifest):** range-read `root`'s persistent byte tree, then
  append `editTail`. It follows only rows reachable from that manifest and
  never replays owning-row history.
- **range(manifest, start, end):** descend only overlapping immutable-tree
  paths, then include the overlapping portion of `editTail`. Bounds are over
  `root.prefixBytes + editTail.length`.
- **subscribe(owner):** subscribe to the owning row/column. Each observed
  version supplies a full manifest that can be materialized independently.

The initial cap should be selected for common appends, not as a universal
constant. Existing physical receipts make 128--256 B a good initial
small-append candidate; the foundation should allow the stream codec to state
its cap so the public default remains revisable.

## Foundation hooks consumed by this adapter

The foundation needs to expose a column codec/materializer registration that
lets the stream adapter:

1. decode and validate the complete `{ root, editTail }` column value;
2. materialize the complete value, or a requested byte range, for merge
   strategies and interior query/index evaluation;
3. report typed immutable references reachable from `root` and validate they
   are in the same domain;
4. publish the manifest as one maintained/subscription value; and
5. atomically write the owner-row replacement plus newly reachable immutable
   rows.

The stream adapter must not reimplement generic column atomicity, reachability,
conditional update, index maintenance, or authorization validation.

## Required behavioral tests

The adapter's integration suite should use public schema and Db APIs and cover:

1. A short append retains one `editTail` and changes only the owner manifest;
   an old owner-row version still materializes its former tail.
2. An append crossing the cap promotes exactly the prior tail plus appended
   bytes, clears the new tail, and leaves the prior manifest readable.
3. Repeated common-size appends produce one bounded tail, not one mutable tail
   per tree node or per history version.
4. Full and range materialization agree at boundaries spanning the immutable
   prefix/tail boundary and a persistent-tree-node boundary.
5. A saved/copied manifest remains readable after subsequent appends without
   consulting owner history.
6. Subscription notifications expose complete manifests from the owning row.
7. Re-inserting a content-addressed part/node/root is idempotent when identical
   and rejects a mismatched payload for the same ID.
8. Merge and indexed/interior-query paths receive the materialized stream value
   rather than raw `root` or `editTail` fields.

For sensitivity, the suite will temporarily plant (and restore) a defect that
either omits the tail during materialization or treats its root and tail as
independent merge fields. The affected historical/range or merge test must
fail for the planted change. This is a test procedure, not committed product
code.

## Deliberate non-goals of this PR

This adapter does not define text splice semantics, file extent patches, or
JSON node/order operations. It also does not make a stream's byte payload
directly queryable as a scalar index; the foundation only guarantees that an
interior query/index consumer can request the materialized typed value needed
by its declared operation.
