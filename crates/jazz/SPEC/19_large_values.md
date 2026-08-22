# jazz — Specification · 19. Large values

## Overview

Jazz `string`, `bytes`, and `json` columns retain their ordinary logical types.
Their physical representation switches transparently between inline bytes and
one atomic large-value descriptor:

```text
LargeValue = Inline(bytes) | Chunked { root, byteLength, utf16Length?, editTail }
BytePatch = Replace { offset, deleteLength, insertBytes, utf16Effects? }
```

The owning application row remains the only mutable value identity and conflict
boundary. Immutable tree nodes are ordinary rows in hidden, versioned tables
that Jazz injects into the application schema. They therefore use the same
transactions, references, permissions, history, and sync machinery as other
Jazz rows; large values do not introduce a parallel object store or protocol.
Query projections choose whether to
return the usual complete application value or an immutable subset; updates use
an ordinary row mutation with a declarative edit operation. Applications never
author descriptors or manage chunks.

Invariant digest:

- `INV-CONTENT-1`: one large value cell is the atomic replicated value.
- `INV-CONTENT-2`: every node row is immutable, canonical, and owner scoped.
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

The descriptor itself remains a valid value of the column's existing Groove
storage type so schema-version projections do not acquire a second physical
type system. `bytes` stores the binary frame directly; `string` and `json`
store the small frame as tagged ASCII. This wrapper contains only inline bytes
or root/metrics/tail metadata, never the large tree payload.

## 19.2 Versioned system-row schema

For every application table containing large-value-capable columns, schema
compilation MUST inject a hidden table for the current node format. Its reserved
name includes the node-table format version and an unambiguous encoding of the owner
table, conceptually:

```text
__jazz_large_value_nodes_v1__<owner-table>
```

Each node row contains its complete content id, owning application-row id,
node kind/version, canonical payload, and aggregate
metrics. The owner id is a real Jazz reference to the application table. Node
read policy inherits through that reference. Node writes are Jazz-generated as
part of an authorized owner-row mutation and are absent-or-identical: user code
cannot create, update, or delete these rows directly.

The tables are part of the compiled and hashed application schema, catalogue,
history, transactions, and wire protocol, but are excluded from generated
application APIs and ordinary user schema introspection. Application migrations
do not mention them: Jazz derives their lineage from the owning table's lens.
A newer incompatible node layout uses a new table/schema version; readers may
support old and new tables concurrently and migrate reachable roots lazily.

Node identities are owner scoped. The initial format deliberately deduplicates
within one owning row, not across independently authorized rows. Logical table
and column names are not identity inputs, so ordinary rename lenses preserve
existing roots and hidden rows.
This prevents content hashes or shared-node existence from becoming a
cross-policy equality oracle.

`INV-CONTENT-2`: a node id MUST commit to the fixed Jazz large-value domain,
node-table schema version, tree format, node kind, owner-row domain, and
canonical payload. An existing row with identical canonical fields is success;
any difference is a hard integrity failure.

## 19.3 Immutable chunk tree

The large root names a recursive tree:

```text
Leaf   { bytes }
Branch { children: [{ id, subtreeByteLength, subtreeUtf16Length? }, ...] }
```

A small tree may be one leaf; a shallow branch is naturally a flat chunk array.
Leaves are selected by a versioned FastCDC-like content-defined byte chunker
with hard minimum, target, and maximum sizes. Branches are selected by a
versioned content-defined chunker over complete child descriptors. Text leaf boundaries MUST be
valid UTF-8 code-point boundaries. No boundary may split a child descriptor. Applying
content-defined grouping recursively
produces one history-independent probabilistic tree (usually called a prolly
tree): the same bytes produce the same shape and root regardless of edit history.

`INV-CONTENT-3`: identical domain and logical bytes MUST produce the same tree
and root independent of authoring history. Every node MUST enforce configured
size/fanout bounds and exact child aggregate lengths before returning bytes.

Chunking occurs before encryption. Cross-domain equality MUST NOT be visible in
node ids. Storage lowering may pack hidden node rows into shared physical structures,
but that is only an implementation detail below ordinary Jazz row semantics.

## 19.4 Recent edit tail

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
boundaries resynchronize, publishes the resulting immutable node rows, and
writes the new root with an empty tail. Node publication and the owner-row
update MUST share one Jazz transaction. A failed transaction exposes neither
the new root nor its nodes.

## 19.5 Built-in logical interpretation

- `bytes` accepts every byte sequence. Full queries return the existing
  usual byte primitive; range queries return an immutable byte subset.
- `string` requires the final bytes to be UTF-8. Every text tree edge also carries its aggregate
  UTF-16 code-unit length. Rust exposes explicit UTF-8 byte and UTF-16 coordinate APIs; TypeScript
  exposes UTF-16 coordinates. Invalid UTF-8 boundaries and UTF-16 positions that split a surrogate
  pair fail rather than round.
- `json` requires final UTF-8 JSON. Its root is literal JSON source bytes, not a
  graph of persistent JSON nodes. Full queries return the usual parsed JSON
  value; path projections return immutable detached JSON subsets.
- an append-only stream is a bytes interpretation that admits only append edits.
- a mutable file is a bytes interpretation admitting insert/delete/replace.

Default queries assemble complete native values efficiently from the
tree and patch tail. Query options may request a range or JSON projection. Those options are
projections, not new stored types or object-like value handles. Streaming helpers may be layered
over successive bounded queries.

Partial results remain ordinary primitives. Updates repeat the selected slice coordinates and
express their edit relative to that slice. They lower to a replacement patch and use ordinary Jazz
branch and conflict semantics. Whole-row compare-and-swap is an orthogonal future API rather than
part of large values. There is no explicit content-value reference API.

`INV-CONTENT-5`: equality, policies, indices, projections, subscriptions,
merges, and application reads MUST observe the assembled logical value.
They MUST NOT branch semantically on Inline versus Chunked or ignore a live tail.

## 19.6 Merge behavior

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

## 19.7 Lifecycle, history, permissions, and sync

Historical row versions name exact large values. A retained Chunked version
pins every hidden node row reachable from its root and tail. Range and
full-value queries lower to ordinary dependency reads over the injected table.
Sync ships those authorized rows through the existing query/view protocol;
there is no content subscription, external fetch channel, or separately
mutable content history.

Authorization of a node is inherited from its declared owner reference. A node
MUST NOT be delivered merely because a client knows its content id. Query and
sync planning may fetch only the tree paths required by a projection, but every
fetched node remains an ordinary policy-checked row. Node insertion is admitted
in the same transaction under policy inherited from the owner mutation; it does
not bypass either sender write policy or recipient read policy.

Readers may load tree nodes lazily for ranges. Missing, malformed, out-of-domain,
or hash-inconsistent dependencies fail closed. A row value MUST NOT be exposed
as successfully materialized until all dependencies required by the requested
projection have passed validation.

## Open questions

- The physical descriptor cannot be exposed to Groove as the logical column value: predicates,
  ordering, indices, joins, aggregates, and read/write policies must all observe the assembled
  value required by `INV-CONTENT-5`. The current prototype materializes terminal results after the
  query and therefore does not yet satisfy this for promoted cells. The intended direction is a
  query-local logical relation/source that materializes only large columns required by the plan;
  durable current/history relations remain authoritative physical storage. The planner and Groove
  source contract remain to be specified and implemented before this feature is releasable.
- The initial promotion/demotion thresholds and chunk-size profile require
  workload receipts for rapid text edits, random file edits, JSON changes,
  append streams, slow peers, and pre-flush memory pressure.
- Reachability-based collection of node rows after every retaining owner-history
  version expires remains to be specified.
- Formatting policy for a semantic JSON merge remains strategy-defined; stored
  and untouched JSON source bytes otherwise remain exact.
- Lazy structural indices for JSON may be added as non-authoritative caches without changing the
  logical format.
