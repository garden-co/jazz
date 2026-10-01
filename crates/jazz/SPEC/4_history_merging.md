# jazz — Specification · 4. History, domination & merging

## Overview

jazz keeps full edit history. A row's stored state is a DAG of immutable versions,
and its "current" value is computed from the versions a node knows. This chapter
defines that version DAG, the domination rule that selects current content,
the merge semantics for concurrent writes, and the separate deletion layer. It
builds on the transaction lifecycle of chapter 3 and supplies the currentness
model used by reads (ch. 5) and sync (ch. 8).

Invariant digest:

- `INV-HIST-1`: A row version that lists a parent MUST dominate that parent for content-current selection when both versions are present in the same layer.
- `INV-HIST-2`: Among content heads not dominated by known parents, the current content version MUST be the head with the greatest made-at/`TxId` sort key.
- `INV-HIST-6`: A merge version MUST dominate all of its parent heads and become the current content winner when present and accepted.
- `INV-HIST-8`: For a plain (`MergeStrategy::Lww`) column, Core MUST merge each accepted write into the row's post-image column by column: the write sets a column it authored iff its stamp (`min(tx_time ms, seq ms)`) is `>=` the column's stored stamp, a tie going to the later seq. Concurrent writes to different columns of one row therefore all survive, and on a shared column the higher stamp wins whatever order Core sequences them in.
- `INV-HIST-9`: `MergeStrategy::Counter` MUST be declared only on non-nullable integer user columns.
- `INV-HIST-10`: For `MergeStrategy::Counter`, a write MUST travel as its delta from the row image it was made over, and Core MUST add each accepted delta to the current value, so concurrent increments from the same base sum exactly. Core MUST reject a write whose delta would take the counter outside its column type's range rather than wrap, and a write that does not author a merge column MUST NOT replace that column's accepted ops with the writer's snapshot, also across schema versions.
- `INV-HIST-11`: Content and deletion state MUST be separate layers; content writes MUST NOT change the deletion register, and a current `DeletionEvent::Deleted` MUST hide the content-current row until a current `DeletionEvent::Restored` reveals it.
- `INV-HIST-12`: Accepted globally settled versions that become per-layer winners MUST be reflected in `jazz_{table}_global_current` or `jazz_{table}_register_global_current`.
- `INV-HIST-13`: Re-ingesting the same commit unit with identical version rows in a different order MUST be idempotent and MUST NOT create a conflict.
- `INV-HIST-14`: Rejected transactions MUST NOT appear as accepted row-history entries and MUST NOT participate in currentness/domination.
- `INV-HIST-15`: Core's post-image for a set of concurrent writes with distinct stamps MUST NOT depend on the order Core receives them: plain columns are decided by stamps, not by the seq Core assigns, and merge-strategy ops commute. Merged values MUST NOT depend on wall-clock or node-local state.
- `INV-HIST-17`: Content and deletion history MUST remain independently immutable and independently selected; a combined current row is a derived cache over their winners and MUST be reproducible from retained histories after restart or rebuild.
- `INV-HIST-18`: A version parent MUST identify an exact prior version of the same physical table, branch key, row, and content/deletion layer; it MUST NOT encode a cross-row transaction dependency or a dependency between the content and deletion layers.
- `INV-TX-6`: A write made on a node after it has applied a row state MUST carry a transaction time at least that state's highest column stamp, so it overrides every value it observed even when the node's clock is behind. Core orders writes by its own seq and does not reject a write because its clock is behind; such a write that observed nothing simply loses per column.

## Details

### 4.1 The version DAG

A row's history is modeled as a directed acyclic graph of **row versions**. Each
version is identified by the `TxId` that wrote it and names zero or more direct
`parents` (ch. 2). Every parent resolves to the same physical table, exact branch
key, `RowUuid`, and content/deletion layer as its child (`INV-HIST-18`). Thus
`parents` are history edges only: mergeable transactions have no general
dependency graph, and content does not parent deletion or vice versa. The same
separation governs exclusive first-committer-wins: a content write is compared
with the content winner, and a deletion/restore write with the deletion winner;
row/predicate read checks validate the visible state they observed instead. Thus
content `C` followed by first delete `D` gives `D` no history parent, and a
later restore parents `D` rather than `C` (covered by
`exclusive_delete_compares_the_deletion_register_not_content` and
`known_parent_must_match_exact_row_coordinate_and_layer`). Ordering is based on `TxId.time`, the HLC input, with the full
sort key `(time, node)` used for deterministic tie-breaking.

On the linear-history line versions carry no parents, and Core's seq, not
the writer's clock, orders accepted writes. Causality is kept by the clock
instead of by an admission check: a node merges the highest column stamp of
every row state it applies into its HLC, so a write made after observing a
value is stamped at least as high as that value and overrides it even when
the writer's clock is behind (`INV-TX-6`, §4.6). Core no longer rejects a
write as `CausalityViolation`.

A version **dominates** the parents it lists, and by transitivity it dominates
their ancestors. When both a version and its parent are present in the same
layer, the parent is not a content head (`INV-HIST-1`).

### 4.2 Selecting the current content version

Current content is selected from the frontier of known, non-dominated content
versions. These frontier versions are the **content heads**: versions that are
not dominated by any known version in the same layer. Among them, the current
content version is the head with the greatest `(time, node)` sort key
(`INV-HIST-2`) — **argmax by HLC, not by arrival order**. Any two nodes that know
the same versions therefore compute the same winner regardless of delivery order.

The rule is scoped to the node's _known_ history. Downstream nodes may hold
shallow or partial history and must not assume completeness (ch. 1, principle
4). The precise statement is: at most one content-current winner exists per
`(row_uuid, layer)` among the node's known non-rejected versions; the visible row
may still be absent (§4.4).

Current reads use this rule without walking the whole row history. `Global`
reads resolve the known current winner from the global-current overwrite tables
(§4.5, `INV-HIST-12`). `Local`/`None` reads start from that direct global base and
overlay only the small set of local versions ahead of global settlement. When no
versions are ahead of global settlement, local hydration is flat in the number of
current rows, not proportional to history depth. The overlay still applies the
same known-history domination and argmax rules (`INV-HIST-1`, `INV-HIST-2`); it
is a bounded currentness computation over the ahead set, not a history scan.

### 4.3 Merging concurrent writes

On the linear-history line there are no merge heads and no merge versions.
Core sequences every accepted write and merges it into the row's post-image
as it accepts it; the post-image is what Core stores and ships. Clients and
local persistence relays preserve authored patches, sync them to Core, and
store Core's post-images when they arrive; they never merge on Core's behalf.

The cells of a post-image are computed per column. A plain column
(`MergeStrategy::Lww`) takes a write's value iff the write authored it and
its stamp is at least the column's stored stamp, a tie going to the later
seq (`INV-HIST-8`, §4.6). For example, with a base row and two concurrent
writes `A (t=30)` setting `title="x"` and `B (t=20)` setting `title="y",
body="z"`, the post-image is `{title:"x", body:"z"}` whichever of them Core
sequences first: `B`'s body has no competitor and `A`'s title carries the
higher stamp.

Counter columns use delta summation instead of last-writer selection. The counter
strategy (`MergeStrategy::Counter`) may be declared only on non-nullable integer
columns (`INV-HIST-9`, ch. 2). A counter write travels as its delta from the
row image it was made over, and Core adds each accepted delta to the current
value (`INV-HIST-10`). Concurrent increments therefore converge to the exact
total: from a base of `10`, a concurrent `+3` and `+5` merge to `18`, not to a
single last-writer value. The delta is carried in the column's own integer
type as its two's-complement residue modulo 2^width and read back as a signed
delta of that width, so a decrement of an unsigned counter travels exactly. A
write made over an image whose change does not fit that signed range (more
than half the type's span in one step) is refused when it is made with an
`InvalidMergeableCommit` error. Core never wraps: when adding an op to the
row's current value would take the column outside its type's range (below `0`
or above the maximum for an unsigned type, outside `MIN..=MAX` for a signed
one), Core rejects that write with a `MalformedCommit` reason naming the
counter and the out-of-range sum, and the counter keeps its value. From `1` on
a `U64` counter, two concurrent `-1`s settle the first to `0` and reject the
second. Which of two such concurrent writes is rejected depends on the order
Core sequences them; accepted ops still commute (`INV-HIST-15`). A redelivered
commit unit is the same transaction
and is not applied again, so a retried increment counts once (`INV-EDGE-16`).

_Further invariants._ `INV-HIST-15` — for writes with distinct stamps the
post-image does not depend on the order Core receives them (stamps decide plain
columns, merge-strategy ops commute), with no wall-clock or node-local state in
merged values.

### Durable codec profile

Every persistent Jazz root supplies one closed epoch-one codec profile when it
opens its ordered-KV adapter. It contains Groove's mandatory base codecs:
generic ordered-KV, V1 large-value descriptor/immutable-node envelopes, and
the ordered-chunk-storage install-receipt wrapper; plus the Jazz-owned byte
families for branch keys; catalogue schemas, mappings, lineages, bootstrap
receipts, lenses, activations, and write pointers; and persisted
result-member, result-row-source, and program-fact keys. The profile is sorted, pinned by the adapter manifest,
and checked before any ordinary record is decoded or mutated. Groove carries
these identifiers as opaque metadata: it does not import or interpret Jazz
schemas. A missing, duplicate, substituted, or future ID therefore fails open
admission rather than leaving a `Bytes` field to a codec-specific fallback.

This inventory names encoding families, not user tables or individual values.
Ordinary scalar/record representation remains Groove's one typed record codec;
a distinct durable root composes this base with its own root-local codec family
before opening. Adding a new Jazz-owned durable byte family requires a new
storage epoch, golden fixtures, and an explicit decoder/migration decision.

`server-catalogue-entry.v1` is an existing member of this profile. Its outer
`JCAT` entry and all catalogue payload outer version bytes remain v1. Within
the existing nested relation-tree grammar, a public `UNION ALL` is the
explicit labeled tag `7`: it stores the arm count, each UTF-8 arm label in
declared order, then the matching arm trees. Labels are unique, NUL-free, and
1 through 4096 bytes. The former unlabeled tag `3` is retired and rejected;
recovery must never synthesize a traversal-position label. The exact positive
grammar receipt and retired-tag rejection live in
`catalogue_payload_codec::tests::labeled_relation_union_uses_explicit_canonical_wire_grammar`.
This changes a nested grammar of the existing family, not its outer profile or
a new durable-root family.

The native whole-root compatibility receipt is
`fixtures/native_storage_corpus.md` and its executable tests in
`node/tests/native_storage_corpus.rs`. It complements the per-family goldens:
the receipt pins one authority-issued catalogue snapshot, immutable and
current physical rows, transaction/merge metadata, and an indirect content
tree together through both SQLite and RocksDB. Current Jazz must first inspect
the committed backend store read-only, then open that same logical snapshot
without writes, materialize its content tree, and preserve it across a mixed
current-format write and a third-process reopen. Regeneration produces fresh
backend-owned candidate bytes which are copied/unpacked into independent roots
and put through that same full receipt before their checksums can be promoted;
only the logical pack is deterministic. This corpus is storage-epoch evidence,
not a compatibility decoder for pre-epoch-alpha roots.

Corpus regeneration stages candidates in implementation-owned private temporary
roots; a maintainer-supplied path is only a create-new publication destination.
Before verification and publication, the producer rejects path/root/dot and
symlink aliases and rejects a staged regular file that has the same stable
physical identity as the live SQLite image or any regular RocksDB member. This
is an accidental-alias guard for a trusted maintainer filesystem, not a hostile
concurrent-filesystem/TOCTOU security boundary. Existing output paths are never
overwritten: regeneration requires explicit deletion or a fresh output path.

### 4.4 Deletion as a separate layer

Deletion is modeled separately from content so that hiding and restoring a row do
not rewrite its content history. Deletion events live in their own register layer
(`VersionLayer::Deletion`) carrying `DeletionEvent::{Deleted, Restored}`, and a
version belongs to exactly one layer (ch. 2). A current `Deleted` event hides the
content-current row; a later current `Restored` event reveals it again; content
writes never touch the register (`INV-HIST-11`).

Physically, deletion history is one sparse, schema-independent relation shared
by all content lineages. Every event is keyed by stable physical table and
canonical branch key before row identity, so a seek for one branch-local row is
bounded to `(physical_table_id, branch_key, row_uuid)` and a branch-key scan
is bounded to `(physical_table_id, branch_key)`. It is not a universal scan
and it never identifies an branch-local row by `RowUuid` alone.

### 4.5 Global-current as derived state

Immutable history versions are the replicated source material. The separately
selected content and deletion winners are node-local derived inputs. The
per-lineage **combined current row** is then derived as:

```text
{ content_winner, deletion_winner, deletion_event, visible, projected_cells }
```

`visible` is true exactly when a content winner exists and the deletion winner
is absent or `Restored`. The current row is rewritten when either winning layer
changes; it must preserve both winner identities even while invisible. It is
not shipped and can be rebuilt atomically from retained accepted history. An
implementation may retain private per-layer helper indexes to make that rebuild
or ingestion cheap, but ordinary current reads consume the combined current
source and do not perform a deletion anti-join (`INV-HIST-17`).

The combined global-current table is the source of truth for `Global`
current-row reads and sync snapshots on a node that has observed the accepted
version. It carries only settled winner references and projected cells, so a
global current read is O(current) in the rows and values returned. Local
tiers use corresponding combined current state or a bounded overlay above this
base; neither rehydrates the global baseline from either immutable history.

_Further invariants._ `INV-HIST-13` — re-ingesting the same commit unit with its
version rows in a different order is idempotent and conflict-free. `INV-HIST-14` —
rejected transactions never appear as accepted history and never participate in
currentness or domination.

### 4.6 Column stamps (linear-history per-column LWW)

On the linear-history line Core merges each accepted write into the row's
post-image one column at a time. Every settled row state therefore records,
for each plain (`MergeStrategy::Lww`) user column and for `_deletion`, the
**stamp** of the write that last set it. Merge-strategy columns (counters,
sets) carry no stamp: their ops apply in seq order.

**Stamp of a write.** `stamp = min(tx_time physical ms, seq physical ms)`.
Core mints the accepted write's seq (`GlobalTime`) from its own wall clock when
it receives the write (monotonic: `max(now, previous seq ms)`), so this is a
zero-tolerance clamp to Core's receive time. A client whose clock runs ahead is
pulled back to Core's receive time; a client whose clock runs behind keeps its
low stamps and can only lose. Only the node that mints the seq computes stamps
authoritatively; relays and clients take Core's row states as they come.
Transaction times come from the writer's HLC, whose physical milliseconds are
non-decreasing per node, so a node's later write never has a lower stamp than
its earlier one (equal-millisecond writes tie and resolve by seq). Every node
also merges the highest stamp of each settled row state it stores into its HLC,
so a write made after observing a value is stamped at least as high as that
value and wins the tie by its later seq. The row's identity alone is not enough
for this: it is the write at the row's seq, not necessarily its newest stamp.

A node that settled its own write locally (merging it when its fate arrived,
possibly over a base that misses seqs it has not received) replaces that
provisional image with the authority's post-image at the same seq when it
arrives; only a newer seq keeps a stored row.

**Apply rule.** A write sets a plain column (or `_deletion`) it authored iff
its stamp is `>=` the column's stored stamp, and then stores its stamp for that
column. On a tie the later seq wins: a write applied in seq order wins ties
against the stored row, while a node that applies an older seq after a newer
one (a late fate) lets the stored row keep ties. The rule is a per-column max
with a seq tie-break, so replaying the same accepted writes in seq order yields
the same post-image and stamps on every node. The first
image of a row stamps the columns its write authored and stores `0` for the
rest. The row keeps the identity of the write at its seq; `updated_by` and
`updated_at` follow the write whose stamp is `>=` the row's highest stored
stamp (a merge-only write compares against, but does not raise, the stored
stamps). Two images of different authored schema layouts keep whole-row
last-writer-wins by the same comparison, and the winner's stamp covers every
slot. Merge-column ops cannot apply across layouts that way, so Core rejects a
write that authors a merge column under a schema version other than the one
the row's current image is stored under, with a `MalformedCommit` reason
saying this is not supported yet ([#3899](https://github.com/garden-co/jazz/issues/3899)).
A cross-layout write that leaves a merge column alone still carries the
writer's snapshot of it as an absolute value, which may predate ops Core has
accepted since; when such a write wins, each merge column it did not author
takes the row's settled value from the current image instead
(`INV-HIST-10`). That value carries over when the current image's schema has
a column of the same name, type and merge strategy that the lens path between
the two versions leaves alone; a column the current image's schema lacks, and
that no lens op renames or copies into, takes the lens default, since no op
can have touched it under that layout. For any other mapping (a renamed or
copied merge column, or one whose type or strategy changed) Core rejects the
write with the same not-supported-yet `MalformedCommit` reason rather than
guess. A merge column the winning write's schema does not have at all is not
carried into its image, like a plain column of that kind.
The pending local overlay is not stamped and always wins locally.

**Durable layout.** Stamps are stored as hidden constant-width groove `U48`
fields (groove SPEC §2.7, `INV-STORAGE-37`: 6 bytes little-endian in the
record's fixed-width region, no offset-table entry). The history,
global-current, ahead-current and ahead-shadow records carry, after
`authored_columns`, one stamp field per slot in slot order: slot `i < L` is the
`i`-th `Lww` user column of the image's authored table schema in schema column
order (merge-strategy columns are skipped, not zero-filled), and slot `L` is
`_deletion`. The stamp field of the cell field `F` is named `_ts_F`:
`_ts__app_<column>` in a logical layout, `_ts__app_<physical id>` in a physical
table, and `_ts__deletion`. Each value is Unix milliseconds; HLC physical
milliseconds are 46 bits wide, so every stamp fits `U48` with the two high
bits zero. Rejected-version records carry no stamps.

A physical table (§16) holds the union of its variants' stamp fields: one
`_ts__app_<id>` per physical column that is `Lww` in at least one schema
variant, plus `_ts__deletion`. Each variant layout selects exactly the stamps
of the `Lww` cells it carries, so a narrower older-schema variant has fewer
stamp fields. A projection of one variant into another layout carries a
target cell's stamp from the source field that supplies the cell (including a
lens `Rename`/`Copy`); a target cell the source does not carry, carries only as
a lens default, or carries as a merge-strategy column projects stamp `0`.

Stamps are **storage-internal** and are stripped at the **host boundary**,
not in the read pipeline. Current-row reads carry the stored current layout
unchanged, stamps included, so a projection that keeps the stored layout
borrows the stored bytes instead of re-encoding every row; internal operators
(policies, joins, sort, limit) may see the stamp fields. They are not
application columns: a query cannot filter, order by or select them, and
`select(...)`/`$` metadata projections never produce them. Where a row leaves
the engine — the shared host row batches (`binding_codec::row_batches`, used
by NAPI, WASM and relay one-shot reads, relation snapshots and subscription
deltas) and the public Rust client row bytes — every `U48` `_ts_*` field is
removed from the descriptor and the record by a per-descriptor cached
byte-copy projection; a descriptor without stamps is published as-is. The
host row grammar therefore never sees a `U48` field.

An **unstamped** image (an uploaded or pending local patch, a query witness,
or a payload whose stamps are unknown) stores `0` in every slot, which is
exactly how a merge treats it.

**Wire layout.** The wire `VersionRecord` carries one trailing field
`col_stamps` (a postcard byte sequence: varint length, then the raw bytes).
Its contents are exactly one of:

- empty — every slot is `0` (including every unstamped image);
- `6 * (L + 1)` bytes, one unsigned 48-bit little-endian stamp per slot in the
  slot order above (`byte[0]` least significant), with at least one nonzero
  slot.

Any other length, and a nonempty all-zero carrier, is invalid and rejected on
wire ingest; encoders emit the canonical form. The byte receipts are the
`node::col_stamps` unit tests (wire carrier and stored field bytes) and the
groove `U48` record/key fixtures.

**Per-row cost.** With `L` stamped columns the stamps occupy `6 * (L + 1)`
bytes of the fixed region of each history/current row, and nothing else; the
former single variable-width `Bytes` carrier cost the same payload plus a
one-byte stored-scalar tag and a 4-byte offset-table entry (5 bytes net for a
stamped row saved), while an unstamped image, formerly 5 bytes, now also costs
`6 * (L + 1)`.

### 4.8 Subsumed merge-strategy backlog

The former TODO notes on complex merge strategies are treated as future surface
area over this chapter's deterministic merge contract. Built-in strategies cover
the first engine paths; richer set/map/rich-text/custom strategies must still
produce deterministic, grouping-insensitive merge results and must fail closed
without wedging authority progress.

## Open Questions

- 🔶 [#1782](https://github.com/garden-co/jazz/issues/1782) — External merge strategies and schema-version movement.
