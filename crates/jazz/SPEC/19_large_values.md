# jazz — Specification · 19. Large values

## Overview

Jazz `string`, `bytes`, and `json` columns retain their ordinary logical types.
Their physical representation switches transparently between inline bytes and
one atomic large-value descriptor:

```text
LargeValue = Inline(bytes) | Chunked { root, byteLength, editTail }
BytePatch      = Replace { offset, deleteLength, insertBytes }
```

The owning application row remains the only mutable identity, history, policy,
subscription, transaction, and conflict boundary. Large values do not have a
separate mutable head or sync protocol. Query projections choose whether to
return the usual complete application value or an immutable subset; updates combine
an ordinary row snapshot with a declarative edit operation. Applications never
author descriptors or manage chunks.

Invariant digest:

- `INV-CONTENT-1`: one large value cell is the atomic replicated value.
- `INV-CONTENT-2`: every object is immutable, canonical, and domain scoped.
- `INV-CONTENT-3`: chunking is deterministic, recursive, and content defined.
- `INV-CONTENT-4`: byte patches are ordered, bounded, and total over their base.
- `INV-CONTENT-5`: logical consumers cannot observe the physical representation.

## 19.1 Logical types and large-value representation

`string`, `bytes`, and `json` remain ordinary column types. Small values use the
inline arm. A writer whose proposed inline value exceeds the built-in promotion
threshold MUST construct a large representation before publishing the row.
Once large, a value may remain large below that threshold; optional demotion is
a representation-only compaction decision and MUST NOT alter logical equality.

`INV-CONTENT-1`: the representation tag, root, byte length, and complete edit
tail MUST be admitted, retained, transported, selected, and merged as one cell.
Core MUST NOT combine a root from one candidate with a tail from another.

The initial format is alpha and has no legacy or compatibility arm. Its format,
chunking, and patch versions are explicit. Unknown versions fail closed.

## 19.2 Immutable chunk tree

The large root names a recursive tree:

```text
Leaf   { bytes }
Branch { children: [{ id, subtreeByteLength }, ...] }
```

A small tree may be one leaf; a shallow branch is naturally a flat chunk array.
Leaves are selected by a versioned FastCDC-like content-defined byte chunker
with hard minimum, target, and maximum sizes. Branches are selected by a
versioned content-defined chunker over complete child descriptors. No boundary
may split a child descriptor. Applying content-defined grouping recursively
produces one history-independent probabilistic tree (usually called a prolly
tree): the same bytes produce the same shape and root regardless of edit history.

`INV-CONTENT-2`: an object id MUST commit to the fixed Jazz content domain,
format version, object kind, authorization/encryption domain, and canonical
payload. Immutable insertion is absent-or-byte-identical; same-id/different-byte
insertion is an integrity error.

`INV-CONTENT-3`: identical domain and logical bytes MUST produce the same tree
and root independent of authoring history. Every node MUST enforce configured
size/fanout bounds and exact child aggregate lengths before returning bytes.

Chunking occurs before encryption. Cross-domain equality MUST NOT be visible in
object ids. The physical store may pack many logical objects together.

## 19.3 Recent edit tail

The edit tail is the short, bounded list of recent changes stored after the
chunk-tree root, avoiding an immediate tree rebuild for every small edit.

All built-in content families use one patch operation:

```text
Replace { offset, deleteLength, insertBytes }
```

Offsets are evaluated against the value produced by all preceding patches in
the same tail. Append is replacement at the current length with zero deletion;
insert, delete, and overwrite are ordinary replacements.

`INV-CONTENT-4`: every intermediate range operation MUST be in bounds and
arithmetic MUST be checked. Admission MUST bound both patch count and canonical
encoded tail bytes. The final materialized value MUST satisfy its logical-type
validator. Individual intermediate patch results need not be valid UTF-8 or
JSON when the complete atomic tail is valid.

When adding a patch would exceed a tail bound, the writer materializes the
current bytes, applies the proposed operation, locally rechunks until content
boundaries resynchronize, publishes the resulting immutable objects, and writes
the new root with an empty tail. Immutable publication and the owner-row update
MUST share one transaction or leave only unreachable immutable objects.

## 19.4 Built-in logical interpretation

- `bytes` accepts every byte sequence. Full queries return the existing
  usual byte primitive; range queries return an immutable byte subset.
- `string` requires the final bytes to be UTF-8. Text edit positions exposed by
  high-level APIs are Unicode-scalar positions and lower to byte offsets.
- `json` requires final UTF-8 JSON. Its root is literal JSON source bytes, not a
  graph of persistent JSON nodes. Full queries return the usual parsed JSON
  value; path projections return immutable detached JSON subsets.
- an append-only stream is a bytes interpretation that admits only append edits.
- a mutable file is a bytes interpretation admitting insert/delete/replace.

Default queries assemble complete native values efficiently from the
tree and patch tail. Query options may request a range, JSON projection, or
native streaming delivery. Those options are projections, not new stored types
or object-like value handles.

Updates take an ordinary immutable row snapshot plus a declarative operation.
They lower to a replacement patch against that snapshot. Stale snapshots use
ordinary Jazz branch/conflict semantics. There is no explicit content-value
reference API.

`INV-CONTENT-5`: equality, policies, indices, projections, subscriptions,
merges, and application reads MUST observe the assembled logical value.
They MUST NOT branch semantically on Inline versus Chunked or ignore a live tail.

## 19.5 Merge behavior

The physical foundation supplies bytes only. A three-way merge materializes the
common base and both attributed sides. Bytes and strings may initially retain
ordinary whole-cell conflict behavior or install an explicit strategy.

JSON merge parses the base, side A, and side B source bytes and produces a
semantic diff labelled by which side made each change for the selected merge
strategy. Persisted edits
remain byte-offset patches. A complete replacement value may be lowered by a
deterministic binary diff; reconstructing authorial intent is not required.
Ambiguous array identity or moves MUST be surfaced as conflicts unless the
application data supplies stable identities.

## 19.6 Lifecycle, history, and sync

Historical row versions name exact large values. A retained Chunked
version pins every immutable object reachable from its root and tail. Sync sends
the ordinary row cell plus required immutable dependencies; there is no content
subscription or separately mutable content history.

Readers may load tree nodes lazily for ranges. Missing, malformed, out-of-domain,
or hash-inconsistent dependencies fail closed. A row value MUST NOT be exposed
as successfully materialized until all dependencies required by the requested
projection have passed validation.

## Open questions

- The initial promotion/demotion thresholds and chunk-size profile require
  workload receipts for rapid text edits, random file edits, JSON changes,
  append streams, slow peers, and pre-flush memory pressure.
- The exact immutable-object transaction and garbage-collection integration is
  owned by the storage implementation, subject to the invariants above.
- Formatting policy for a semantic JSON merge remains strategy-defined; stored
  and untouched JSON source bytes otherwise remain exact.
- Lazy structural indices for JSON and Unicode-scalar indices for text may be
  added as non-authoritative caches without changing the physical format.
