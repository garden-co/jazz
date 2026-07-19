# jazz — Specification · 12. Large values (`text` and `blob`)

## Overview

`text` and `blob` columns are edit-oriented, op-log-backed values, distinct
from the replace-whole-value `string` and `bytes` columns. Their content bytes
travel through a separate content channel instead of being carried directly in
the row-version cell. This chapter specifies the column kinds, op-log,
write/materialization paths, content channel, and checkpoint model. It builds on
the schema (ch. 2), history/merge (ch. 4), and the sync content lane (ch. 8).

Invariant digest:

- `INV-API-25`: TextEdit operations MUST use byte offsets relative to the current local parent value for the column and MUST lower to LargeValueEditOp::Insert/LargeValueEditOp::Delete.
- `INV-HIST-5`: An upstream node that observes two or more concurrent mergeable content heads for a row MUST create an accepted mergeable merge version with those heads as parents, un...
- `INV-HIST-6`: A merge version MUST dominate all of its parent heads and become the current content winner when present and accepted.
- `INV-HIST-15`: Merge strategy behavior MUST be deterministic, grouping-insensitive over the parent/head set, and non-wedging at merge time: registered strategy failure degrades to th...
- `INV-HIST-16`: A merge value MUST be the deterministic fold over the de-duplicated raw head set, never a fold of already-merged values. Combining divergent merge versions MUST fold t...
- `INV-LVAL-1`: text and blob columns MUST be represented as Jazz-level LargeValueKind::{Text, Blob} and MUST lower to groove ColumnType::Bytes, not to a groove text/blob type.
- `INV-LVAL-2`: Changing a column between plain Bytes, LargeValueKind::Text, and LargeValueKind::Blob MUST change the Jazz schema identity.
- `INV-LVAL-3`: A large-value column MUST NOT use MergeStrategy::Counter.
- `INV-LVAL-4`: A stored large-value version payload MUST be a deterministic textoplog op batch containing Op::Insert and/or Op::Delete records.
- `INV-LVAL-5`: Stored insert ops for locally authored large-value commits MUST store inserted bytes in the content store and encode TextContent::Ref(Extent) in the version payload.
- `INV-LVAL-6`: Content streams MUST be append-only per (writer,row,column), and appended extents MUST be assigned monotonically increasing offsets.
- `INV-LVAL-7`: Re-ingesting an already-present extent with identical bytes MUST be idempotent, and conflicting bytes for the same extent MUST be rejected.
- `INV-LVAL-8`: A content extent read MUST fail closed if any byte range in the requested extent is missing or gapped.
- `INV-LVAL-9`: A whole-value write to a large-value column MUST be stored as the diff from the materialized parent value, not as app-visible bytes verbatim in the history cell.
- `INV-LVAL-10`: Explicit large-value edits MUST reject empty op batches and MUST target a text or blob column.
- `INV-LVAL-11`: Explicit large-value edit positions MUST be byte offsets valid against the current parent value.
- `INV-LVAL-12`: Query/sync result rows MAY carry large-value handles, but any value-returning API MUST materialize the handle into Value::Bytes; encoded op payloads and raw extent han...
- `INV-LVAL-13`: Large-value materialization MUST follow a single primary-parent chain; for a multi-parent merge version, the primary parent MUST be the highest-sort-key parent.
- `INV-LVAL-14`: A node MUST park or refuse to drain commit units whose large-value op payloads reference content extents not present locally.
- `INV-LVAL-15`: Content fetch responses MUST NOT return bytes unless the requested Extent.row matches the request row and the extent is visible/member-authorized for that row.
- `INV-LVAL-16`: Local checkpoints MUST be versioned by (table,row,column,TxId) and survive reopen without becoming canonical replicated row state.
- `INV-LVAL-17`: text/blob columns MUST NOT be accepted in filters, joins, ordering, or other query-planner predicates.
- `INV-LVAL-18`: An upstream large-value merge version MUST merge concurrent head op streams since their column LCA, then store a primary-parent-relative op batch that materializes to...
- `INV-LVAL-19`: Large-value checkpoint placement MUST be opportunistic local derived state: once accepted ingestion or materialization observes at least the configured replay-op inter...

## Details

### 12.1 What `text` and `blob` are

`text` and `blob` express schema-level editing semantics, not a size threshold.
A five-byte `text` value and a multi-gigabyte `blob` both use the large-value
model because the column kind says that edits are represented as operations over
content bytes. In the reference implementation these kinds are represented as
`LargeValueKind::{Text, Blob}`.

The storage layer sees these values as groove `ColumnType::Bytes`, while Jazz
retains their distinct schema identity. Changing a column between plain `Bytes`,
`Text`, and `Blob` therefore changes the schema version (`INV-LVAL-1`,
`INV-LVAL-2`, ch. 2). A large-value column may not use
`MergeStrategy::Counter` (`INV-LVAL-3`). Both kinds use byte offsets for edits,
including edits that intersect multibyte content; whether `text` carries
Unicode-position semantics is open.

The query carve-out is part of the column contract: `text` and `blob` are not
filter, join, or order columns. Enforcement status is tracked in Open questions.

### 12.2 The op-log

The op-log is the canonical replicated representation of a large-value version.
It records the edit the author made, not a replacement copy of the whole value.
Each version carries a deterministic `text_oplog` batch of
`Op::{Insert, Delete}` records (`INV-LVAL-4`).

Positions are **byte offsets relative to the parent value as the author saw
it**. When a batch contains multiple operations, they apply in order, and later
positions are adjusted for earlier operations in that same batch
(`INV-LVAL-11`). Inserted bytes live outside the row cell in **append-only
content streams keyed by `(writer, row, column)`**, with monotonically
increasing offsets (`INV-LVAL-6`). After extent-backing, a stored insert
references those bytes as `TextContent::Ref(Extent)`; inline bytes are only an
internal diff/replay form (`INV-LVAL-5`).

### 12.3 Write paths

Large-value writes have a single history shape: they become operation batches.
When an application writes a whole value, Jazz stores the **diff against the
materialized parent**, not the app-visible bytes verbatim (`INV-LVAL-9`). When an
application submits an explicit edit, the edit is already in operation form
(`LargeValueEditCommit` / `LargeValueEditOp`, exposed through `Db::edit_text` /
`TextEdit` at the facade, ch. 13).

For example, `Db::edit_text(row, col, [Insert{pos:5, "x"}, Delete{pos:0,
len:2}])` applies the ops in order against the materialized parent, each `pos` a
**byte** offset adjusted for earlier ops in the same batch (`INV-API-25`,
`INV-LVAL-11`). Offsets are bytes, not characters: splitting a multibyte UTF-8
sequence is the caller's responsibility (whether `text` gains Unicode-position
semantics is open, §12.1).

_Further invariants._ `INV-LVAL-10` — an explicit edit rejects an empty op batch
and must target a `text`/`blob` column. `INV-LVAL-13` — materialization follows
a single primary-parent chain; for a multi-parent merge version, the primary
parent is the highest-sort-key parent.

### 12.4 Materialization

Query and sync result rows carry **large-value handles**, not bodies. A handle
names the large-value kind, materialized logical length, and the extent refs
needed to hydrate the value. Result membership, settlement, and read-frontier
coverage are independent of whether the handle's body extents are present
locally; a row can be a settled result member while its large-value bytes are
still cold.

Hydration is pull-only at the access layer. To materialize a value-returning API
result, the node fetches any missing authorized extents, folds op-log extents at
the boundary, and returns the materialized value as `Value::Bytes`. Encoded ops
and extent handles never escape a value-returning API as cell bytes
(`INV-LVAL-12`). To materialize a value, the node replays the op-log from a
local checkpoint when one is present, otherwise from origin, along the single
primary-parent chain (`INV-LVAL-13`).

### 12.4.1 Authority merge versions

Authority merge versions preserve concurrent edits in the large-value op-log
rather than selecting one visible head by the default LWW cell rule. When an
upstream authority creates a merge version for concurrent heads, it finds the
lowest common ancestor in the column's version DAG, reconstructs each head's op
stream from that LCA to the head, and merges the streams with
`text_oplog::merge_since_lca` (`INV-LVAL-18`). Concurrent same-position insert
runs are ordered by causal origin
`(TxTime, AuthorId, NodeUuid)`, so delivery order does not affect the merged
bytes. For more than two heads, authorities first sort the column heads by that
causal key and fold in ascending order while tracking the accumulator's greatest
processed origin; this keeps the large-value cell path deterministic for
INV-HIST-15. INV-HIST-16 remains target because edge-level merge-of-merges still
lacks direct topology coverage.

The merge version remains a multi-parent content version for history
domination/dedup (`INV-HIST-5`, `INV-HIST-6`), but its large-value cell is stored
as an op batch whose primary parent is the highest-sort-key head. That op batch
transforms the primary parent's materialized value into the op-merged value, so
existing single-chain materialization and checkpointing continue to work through
the merge version. Client-side/non-upstream conflict handling is unchanged: it
argmax/LWW-selects a visible head and does not expose the authority op-merged
value until it receives such a merge version.

### 12.5 The content channel

The content channel carries the bytes named by op-log extents. These extents are
an **auxiliary content payload** (ch. 2 §2.1), not one of the three stored-column
classes: they are fetched separately, keyed by extent, and never queried or
argmax-merged as row cells. The In-flight design below calls this "class 4"; it
is the same out-of-band content payload.

A node parks, and does not drain, a commit unit whose op refs name content
extents that are not present locally (`INV-LVAL-14`). Content fetch serving is
authorized by row context and membership: the serving node refuses an extent
whose `Extent.row` mismatches the request or that is not visible to the peer
(`INV-LVAL-15`, ch. 7, ch. 8).

Revocation is enforced at fetch time. If a peer received a row handle before
hydrating its body and later loses read scope, subsequent content-extent fetches
for that row are denied; Jazz does not promise post-delivery redaction of bytes
already fetched or stored locally (ch. 7 §7.3).

_Further invariants._ `INV-LVAL-7` — re-ingesting an extent with identical bytes
is idempotent; conflicting bytes for the same extent are rejected. `INV-LVAL-8` —
an extent read fails closed if any requested byte range is missing or gapped.

### 12.6 Checkpoints

Checkpoints are replay accelerators, not replicated truth. They are **local
derived state** in the content store, versioned by `(table, row, column,
version)`, rebuildable, and durable across reopen without becoming canonical
replicated state (`INV-LVAL-16`). They bound cold-load and replay cost using the
same rebuildable-derived-state model as groove's durable indices (groove spec
ch. 2).

Placement is opportunistic: after accepted ingestion or local materialization
sees at least the configured checkpoint interval of replayed ops, the node
writes a checkpoint at the version it materialized (`INV-LVAL-19`). Until a
checkpoint has been placed, a read may replay the full parent chain; checkpoints
optimize replay, but they are not required for correctness.

Large-value columns are materialized for reads, but they are not planner keys:
`text`/`blob` columns are rejected in filters, joins, ordering, and other
query-planner predicates (`INV-LVAL-17`).

### In flight & detailed design (non-normative)

_The §12.1–12.6 contract above is normative. The following is the full
large-value working design — op-log detail, derived structures, worked example,
bundles, truncation/GC, config, counters, benchmark predictions, measured
findings, and slice sequencing — retained from the former `LARGE_VALUES.md`. It
settles upward into the normative body as decisions land._

Design for storing, syncing, and serving large and frequently-edited column
values — collaborative documents, durable streams, files — without userland
modeling. The normative semantics live in §12.1–12.6 above; scenarios 5 and 6 in
[B_benchmarks.md](B_benchmarks.md) are the workloads this design must win. Informed by the eg-walker paper (arXiv 2409.14252) and the
WiscKey/Badger key–value-separation lineage; see Related work.

### Thesis

**The op log is the only canonical structure. Everything else is an index.**

A `text`/`blob` column's replicated history is a log of editing operations —
what the writer did, recorded as the writer saw it. Bytes written are
immutable forever. Every other structure in this document (materialized
values, checkpoints, the chunk index, bundles) is node-local, derived, and
rebuildable from the op log. Consequences, in order of importance:

1. nothing replicated is ever rewritten — history is never touched;
2. indexes can be built lazily, by any node, at any time, without
   coordination, because nothing they do can conflict with anything;
3. representation disagreements (chunk boundaries, cache shapes) cost
   efficiency, never correctness.

The application-facing promise is **brainless dumping**: apps may write whole
values into a column and jazz derives the ops; apps that want O(change) cost
use the edit API. Both produce identical history.

### Column types

| type              | semantics | storage                     | merge                                                                   | queryable                        |
| ----------------- | --------- | --------------------------- | ----------------------------------------------------------------------- | -------------------------------- |
| `string`, `bytes` | replace   | verbatim in the history row | whole-value HLC-LWW                                                     | filter/join ✓                    |
| `text`, `blob`    | edit      | op log (this document)      | authority op-merge; `[needs: text-merge]` rich-text merge remains gated | not filterable (planner rejects) |

The choice is schema-level semantics, not a size threshold: a 5-character
`text` field and a 2GB `blob` use the same representation, so there is no
format cliff and no migration at a magic size.

**Representation decision (2026-06-13, resolving the impl fork):** `text`/
`blob` are **jazz-level column types**; groove stores them as **opaque
bytes** (no new groove `ColumnType` — groove never filters/joins/orders
them, per the carve-out: queryable lowers to groove, op-log columns are
opaque payload). jazz owns everything semantic: the op-log codec
(encoding the diff/op runs into the column's byte payload), materialize-
by-replay on read (synthesizing the value the app sees from the nearest
anchor forward), and anchor placement. The jazz query planner rejects
filters/joins/ordering on `text`/`blob` columns (the "not filterable"
rule above). Rejected alternatives: a groove `Text` type (buys nothing —
groove can't query it — and breaks the carve-out); overloading `string`
with op-log storage (violates the taxonomy, risks ordinary string
semantics). So adding `text` is: a new jazz schema column kind that
lowers to groove `Bytes`, plus jazz-side op-log encode/materialize.

### The op log (canonical, class 1)

#### Operations

A `text`/`blob` version's payload is an op list applied against the parent
version's content:

```text
Op {
  Insert { pos, content: Extent },     // append = insert at end
  Delete { pos, len },
}
Extent  = (writer, content_range)       // range in a writer's content stream
Content = Literal | Ref(extent)         // extent-only; ops never speak hashes
```

- **Positions are relative to the parent version** — recorded as the author
  saw the document, never transformed. (This is eg-walker's representation
  and exactly what a future replay-based merge consumes.)
- **The content stream**: each `(writer, row, column)` has an append-only
  byte stream holding that writer's inserted bytes, in op order. Ops carry
  extents into it, not inline bytes — the stream is the transport/storage
  unit for content (class 4, below). Informally this stream is called the
  writer's **run**. Streams are append-only, so **every committed prefix is
  immutable by construction** — no sealing ceremony exists or is needed.
- **Ref content**: an insert's content may _reference_ existing content
  instead of new bytes — `Ref(extent)`, always extent-shaped. This is what
  makes reverts, in-document paste, and file replace-with-overlap cost one
  op instead of re-storing content. When the dump path detects known content
  via a chunk-index probe, it resolves the hash to extents **before**
  emitting the op — the canonical layer has zero dependency on hashing,
  chunker parameters, or the index (this also keeps truncation's
  pinned-extent walk free of hash indirections). A writer can only Ref
  content it holds, which is coherent: the dump path can only diff against
  a value it materialized. **Scope rule (load-bearing for GC): refs may
  only target the same `(row, column)`'s history.** Cross-row refs are
  future work behind a refcounting design; see Truncation & GC.

#### Encoding

Ops are **not** stored as a separately-compressed columnar stream (eg-walker's
intra-log RLE of op types/positions/lengths). They live in **normal row
versions**: a version's op-log column carries the op(s) that version applied —
one or several, worst case one op per version (per keystroke). The "op log" is
just the chain of these per-version op-batches across history rows.

Bulk compression happens at two transparent layers, never via a bespoke op-log
codec:

- **at rest**: the storage engine (LSM per-level + dictionary compression);
- **on the wire**: a general **columnar + RLE encoding of whole-row-version
  bundles** — every column benefits, and the op-log column's bytes benefit
  incidentally, with **no large-value-special path**.

(One-op-per-version is also what yields per-transaction op provenance for free.)

#### Write paths

- **Edit API** (`insert/delete/append` on a handle): emits ops directly.
  O(change) CPU, storage, and wire. The handle's in-memory state is the
  materialized piece structure (below); editors integrate here.
- **Dump path** (write the whole value): the runtime diffs the dump against
  the materialized current value — shared prefix/suffix via piece/chunk
  comparison, content-defined chunking of the middle, chunk-index probe for
  known content (reverts and pastes become `Ref` ops with zero new bytes).
  O(value) CPU, O(change) storage and wire. Sane for occasional dumps
  (Wikipedia-style revisions, 10Hz stream snapshots), wrong for 120Hz
  keystrokes — that's what the edit API is for.

### Merges and the effective chain

Row-level machinery creates **merge versions** for concurrent mergeable
heads. For op-log columns, merge versions are **zero-content**: they carry
no ops — synthesizing ops no writer made would violate the thesis — and
their content state is _defined, not stored_, as a deterministic function
of the DAG behind them. What a merge version does carry is its **exact
merge-strategy version**, a tiny class-1 tag from an append-only registry:
`lww` (state = the max-`(time, node)` parent's state, matching the column
table's semantics), `replay-v1` for `text-merge` (eg-walker replay since the
LCA), `replay-v2` if the algorithm is ever refined.
Pinning the exact strategy per merge keeps history semantically immutable:
shipping a new strategy changes only what _new_ merges record; historical
states, time-travel reads, and checkpoint contents never rewrite.

Replication is unaffected: **all versions keep shipping** — loser-branch
versions are class-1 history like any other; zero-content merges reduce
stored/shipped _merge_ payload, never the history itself.

**Effective chain**: materialization replays from the nearest checkpoint
along the chain that, at each merge version, continues through the path
its recorded strategy designates — under `lww`, the winning parent only;
loser-branch ops remain immutable in their streams but are _inert_ until
a `replay-*` merge consumes them. A writer editing on top of a merge
records positions relative to the merge's defined state (well-defined:
the strategy is deterministic). Merge-of-merges recurses cleanly — an
inner merge's state is itself defined.

**Checkpoint affinity**: merge versions are the _ideal_ checkpoint-cache
sites — they are exactly where the effective chain (re)converges, so one
checkpoint there serves every descendant of all merged branches; and in
under `replay-*` strategies they are precisely the states that are expensive to
derive (a full since-LCA replay), so caching them bounds everyone's
replay. Checkpoint density policy should prefer merges first.

**Checkpoint validity**: a checkpoint serves any replay target whose
effective chain passes through it. Under `lww` the effective chain is a
single path, so every version on the current chain is a valid checkpoint
site; checkpoint placement becomes more interesting under replay strategies
(see Open questions).

### Derived structures (class 3: local, disposable, rebuildable)

#### 1. Materialized value

The current value, for reads. Small values: plain bytes. Large values: a
**piece tree** — a balanced tree of extents/chunk references providing
O(log n) random access (Dropbox-style range reads of a 2GB value = tree walk
plus a few extent fetches, no full materialization). Rebuilt by replay from
the nearest checkpoint. This is cache, not format: nothing replicated ever
references a piece tree.

#### 2. Checkpoints

Materialized snapshots at **critical versions** — DAG cuts where everything
before happened-before everything after (eg-walker's rule; cheap to detect
on the version DAG, and exactly the points where replay state is empty).
Checkpoints bound replay for cold loads, historical reads, and crash
recovery; merge versions are preferred sites (see Merges and the effective
chain: checkpoint affinity). They are serving artifacts: a node ships its own checkpoint to
hydrate a cold client (descriptor + lazily-fetched extents); no cross-node
canonicity is required. Density is a config knob; `history: shallow` sync ≈
latest checkpoint + ops since.

#### 3. Chunk index

A content-addressed index over **stable content**: content-defined chunking
(FastCDC-style) over the materialized value's settled regions, yielding
entries

```text
chunk_hash → [ Extent, ... ]      // one chunk: 1+ extents, any writers
```

Chunks and streams are _independent partitions of the same bytes_ — chunks
cut by content, streams by authorship — and the index is the join between
them. A chunk's extents may interleave multiple writers' streams. No bytes
are copied: **canonicalization is indexing, not rewriting.**

Properties:

- **Anyone, anytime**: any node may index any committed prefix of any
  stream — prefix immutability makes this conflict-free; CDC determinism
  makes all nodes' entries identical up to the last settled boundary. The
  **hot fringe** is the region past that boundary.
- **Efficiency contract, not wire contract**: chunker parameters are pinned
  per column type for dedup quality, but disagreement (version skew) costs
  missed dedup only — bytes always resolve through extents.
- Uses: checkpoint descriptors and their hydration dedup (the one wire
  path that speaks hashes — see The content channel), write-time dedup
  (the dump path's revert/paste detection, resolved to extents before ops
  are emitted), and — later — cross-row storage dedup.
- **Severable and staged**: because ops never reference hashes, the entire
  content-addressed overlay is an optimization layer for cold loads and
  rehydration, not a correctness dependency. The first engine path may ship extents-only
  (checkpoint descriptors as extent lists, dedup via per-stream high-water
  marks; fatter cold loads) and add the chunk index when scenario 5/6
  cold-load and rehydration byte counts say it pays — a benchmark gate
  like everything else.

### Worked example (condensed)

```text
Alice types "Shopping: milk, eggs" into note1.text:
  her content stream:  Shopping: milk, eggs          (bytes 0..20)
  v1..v5: Insert ops, RLE'd to one run entry each commit
Alice inserts "oat " at position 10:
  stream:              Shopping: milk, eggs oat␣     (bytes 0..24, append-only)
  v6: Insert{pos:10, content:(A, 20..24)}            (bytes never move)
Indexing (lazy, any node): CDC over the settled content →
  h1 = hash("Shopping: oat ") → [(A,0..10),(A,20..24)]
  h2 = hash("milk, eggs")     → [(A,10..20)]
Bob cold-loads: receives checkpoint {h1, h2}; fetches (note1, h1), (note1, h2)
  over the content channel; edge auths by row, resolves via index.
Carol inserts "bread, ": her own stream, v8 = Insert{pos:16,(C,0..7)};
  re-indexing the dirty region later yields h3 spanning A's and C's streams.
Dave reverts to v7's content via dump: chunker emits h1,h2 — both already
  indexed → resolved to extents → v10 = Ref(extent) ops, zero new bytes.
```

### Identity and naming

- **Hot path: `(writer, int)` everywhere** — ops, extents, streams. Same
  scheme as transaction ids; varint-friendly through node-alias interning;
  no hashing on the write or read path.
- **Hashes only in the chunk index, checkpoint descriptors, and wire
  dedup — never in ops.** BLAKE3; ~32B per ~2KB chunk in index entries;
  computed during lazy indexing, never on the keystroke path.
- **Names are never credentials** (neither extents nor hashes): all content
  fetches are authorized by row context at fetch time (below). Unguessable
  is not revocable, and infrastructure leaks names.

### Sync and serving

#### Column-taxonomy class 4: content payload

Op _metadata_ is class-1 version payload (tiny, ships with the commit unit).
Class 4 is the **generic lane for immutable, content-addressed payloads that
accompany but are not row versions** — content _bytes_, checkpoint descriptors,
and chunk-index data alike: never argmax'd, never in result-set material,
never queried, fetched on demand on the content channel (below). This is the
one mechanism for "stuff auxiliary to the versions we sync"; anything new of
that shape rides it rather than inventing a side protocol.

Content _bytes_ are the primary case: replicated-immutable, peer-deduplicated,
shipped at most once per peer:

- **eagerly** on the sync channel for subscribed rows when small (≤ inline
  threshold) or when the row is live-subscribed (hot stream deltas — a
  tailing client receives appended bytes with the version, latency-relevant);
- **lazily** over the **content channel** otherwise.

Peer dedup state per hop: shipped chunk hashes + per-stream high-water
marks. (This generalizes the version-level dedup already in the protocol —
same mechanism, finer granularity.)

#### The content channel

A separate channel beside sync (bulk must not head-of-line-block fate
messages). Request/response, unordered:

```text
fetch (row, extent)        // op-shaped reads: replay, history, range reads
fetch (row, chunk_hash)    // checkpoint hydration: cold load, shallow
                           // history, resume-after-eviction
```

The split mirrors the two read shapes. Everything op-shaped fetches by
extent and dedups via per-stream high-water marks (replay walks contiguous
stream prefixes). Checkpoint hydration speaks hashes because a checkpoint's
content is a scattered set of ranges across many writers' streams: high-
water marks cannot express "I already hold these fragments" compactly, a
shipped-hash set can — and after heavy multi-writer editing the extent
decomposition fragments while hashes re-coalesce at chunk granularity, so
the same logical content yields the same short descriptor regardless of
authorship. In an extents-only path (see Chunk index: severable), the hash
form is simply absent. The serving node:

1. **row RLS check** — the same prepared-shape probe that authorized syncing
   the row, typically hot because the client just received the row;
2. **membership check** — the requested chunk/extent must belong to the
   row's value (its tree/index) — _load-bearing_: without it, any one
   readable row is a skeleton key for arbitrary content. Consequence: a
   serving node must hold the row's **op log** to answer this; serving
   nodes may evict content _bytes_ (refetch-by-name) but never the ops;
3. resolve to bytes: local store, else bundle (below).

Revocation therefore acts at fetch granularity with zero token machinery.
(A public-content CDN path with presigned grants is possible later as a
special case; it is not the foundation.)

#### Hazards (inherits the jazz hazard list)

A version can arrive before its content bytes (lazy class 4): parked-orphan
handling extends to content references, with a counter. Idempotency: content
ingestion is keyed by extent — re-delivery is a no-op.

### Relationship to groove (the carve-out)

This is the first feature to _amend_ the "everything lowers to groove"
principle rather than extend it, and the amendment is deliberate. The
**op log is class-1 version payload and lowers normally**: ops ride
commit units, live in history rows, and flow through groove's deltas
like any cells — subscriptions, policies, and the currency machinery
see op-log columns as opaque payload. What lives **below** groove is
class-4 content: writer streams, the manifest, bundles, and the chunk
index are byte-range storage keyed by extent, and IVM has nothing to
offer raw bytes — there are no predicates over them, no joins, no
incremental views; their only operations are append, ranged read, and
(under truncation) compaction. Forcing them through the record model
would buy generality nothing and cost the append-only layout
everything. Mechanically, groove offers a **direct raw column-family
handle** — the `OrderedKvStorage` interface surfaced as a named passthrough CF,
bypassing the record/IVM/query machinery — and jazz uses it directly for
these non-table stores (the content/extent store, checkpoints, op-log blob
payloads), so opaque bytes never enter the IVM and the queryable layer
stays clean. The boundary is precise: **anything queryable lowers to
groove; anything that is only ever ranged-read lives in the content
store.** Derived content structures (piece trees, checkpoints, the
chunk index) are node-local class-3 state over that store, rebuildable
by replay, exactly as groove's own indices are rebuildable from base
tables.

Corollary (pinned 2026-06-12): **history compression and tiering
happen transparently inside the storage engine** — per-level
compression, dictionary compression, tiered level placement — never by
re-packing queryable rows into opaque blocks, which would break
queries over cold history. Opaque packing is reserved for class-4
content bundles, which are never queried by construction. (This
supersedes the packed-cold-history direction from the earlier
specification lineage.)

### Bundles (private cold storage)

Nodes pack committed stream ranges into **bundles** (e.g. S3 objects), with
a local **manifest**: `extent range → (bundle, offset)`. Rules:

- bundles are **never protocol-visible** — extents and hashes are the names;
  bundle layout is each node's private business;
- bundling needs no seal: any committed prefix may be bundled now, a
  resurrected stream's tail bundled in a later pass;
- serving reads are ranged GETs into bundles, amortized by local caching —
  the client never talks to object storage directly.

### Truncation and GC

Hard deletion of history (truncation) is a **core-authored, monotone
truncation horizon** per value, propagated like a fate. The horizon ships
with:

- a **sentinel**: a checkpoint promoted to class 1 — an authored snapshot op
  carrying the value at the horizon, either as literal content (everything
  before becomes deletable) or as refs into a **pinned-extent list**;
- the pinned-extent list: computed _once, at core_ by walking surviving ops
  and checkpoints for refs into the truncated range (the same-value ref
  scope rule is what keeps this mark phase local to one op log).

Downstream nodes obey mechanically. Then:

- **chunk index**: entries over deleted extents are dropped; re-deriving
  over surviving content reproduces the _same hashes_ on new extents (CDC
  names content, not location) — so per-peer dedup state stays valid and no
  cross-node invalidation exists;
- **bundle GC** (WiscKey/Badger value-log style): manifests track live
  bytes; below an occupancy threshold, rewrite survivors into a new bundle —
  ranged GET, PUT, **flip manifest entries in one groove batch, then delete
  the old object after a grace period** (in-flight reads); a periodic
  list-and-compare sweep collects orphans from crashes between phases.
  Extent _names_ never change during compaction — only the manifest — so
  nothing ripples. Edges may simply _drop_ low-occupancy bundles and refetch
  from core (eviction as degenerate compaction); only core must rewrite.
  **No compactor exists until truncation ships** — without truncation,
  nothing dies.

#### Eviction vs. truncation (glossary)

- **eviction**: node-local, reversible, refetch-by-name; requires no
  protocol and no bookkeeping beyond the fetch path;
- **truncation**: global, permanent, authority-ordered, horizon-monotone.

They share no machinery.

### Configuration

| knob                  | default (initial)                        | notes                                       |
| --------------------- | ---------------------------------------- | ------------------------------------------- |
| inline ship threshold | 1 KB                                     | content ≤ this rides the commit unit        |
| chunker (text)        | FastCDC, ~2 KB avg                       | pinned per column type; efficiency contract |
| chunker (blob)        | FastCDC, ~64 KB avg                      |                                             |
| checkpoint density    | every N=1024 ops or at critical versions | replay bound                                |
| dump-dedup probe      | chunk-index lookup                       | covers reverts/pastes                       |
| bundle target size    | 64 MB                                    |                                             |
| bundle GC threshold   | < 40% live                               |                                             |
| bundle delete grace   | 2× max fetch timeout                     |                                             |

### Counters (deterministic; benchmark gates)

ops in/out, RLE ratio · content bytes shipped vs. referenced per link ·
indexed-prefix lag per stream · write-time dedup hits (Ref ops emitted) ·
content-channel bytes by source (cache / bundle) · checkpoint replay
lengths · pinned-extent bytes · **dangling refs (hard-fail > 0)** · bundle
occupancy histogram, compacted bytes, orphans found · duplicate-content
bytes at core (the cross-row-dedup tripwire) · client steady-state memory
for a synced document (must be ~document + indexes, independent of history
depth).

### Benchmark predictions (pre-registered; scenarios 5 & 6 check them)

1. Stream live tail: **synced bytes/token ≈ payload**; storage ≈ payload +
   RLE'd op metadata + LSM compression.
2. Keystroke trace replay (1 edit/commit): per-edit cost ≈ bytes typed +
   amortized op metadata; **no chunk-size floor** (indexing is off the write
   path).
3. Full-history storage for the Kleppmann trace: target the eg-walker
   envelope (their event graph: 20–150% of final document on sequential
   traces); jazz overhead above that = transaction metadata, measured.
4. Revert (Wikipedia trace): ≈ one Ref op, ~zero new bytes.
5. Cold load current-only: O(document), independent of history depth;
   range read into a large value: O(range + log n).
6. Client steady-state memory: independent of history depth and past
   concurrency (the eg-walker property, inherited structurally — asserted,
   not assumed).

### Measured findings (2026-06-13) — the column-delta de-risking

A column-delta prototype (text op-log + anchor/replace) was measured
against the String baseline on the three S6 sequential traces across
four axes. **Verdict: column-delta wins nothing for keystroke text —
PARKED.** It remains the right design for large _single_ values (big
blobs, low-frequency large dumps); it is irrelevant to the
keystroke-text workload. Evidence:

- **Storage (on-disk):** String == Text to <0.05 bytes/edit at every
  batch size. RocksDB LSM compression already absorbs the redundant
  full-document copies (logical text bytes/edit: String 4.3, Text 2.4
  — but on-disk identical). Confirms the cold-history doctrine: the
  storage engine handles value redundancy transparently.
- **Per-edit cost is metadata, not value:** batch=1 ~2300 bytes/edit →
  batch=256 ~119 bytes/edit (19× from amortizing per-commit metadata);
  the text value is ~4 of those 119. Prediction 1/2's "amortized op
  metadata" dominates; the value term is negligible at these doc sizes.
- **Wire:** String ≈ Text (5.1 vs 5.15 bytes/edit at batch=256);
  metadata-dominated, op-log envelope adds a hair. Delta may pay on the
  wire at _large_ doc sizes (untested — needs the full 100KB trace).
- **Steady-state memory: prediction 6 HOLDS** — flat 71→72 KB from 1k
  to 18k history depth; the client does not retain history. Text ~equal.
- **NEW finding — cold-load PEAK memory is O(history depth):** 19.7MB
  (1k) → 195MB (10k) → 352MB (18k versions) for a 4.7KB document; Text
  no better. This violates prediction 5's spirit for deep history and
  is the real memory lever — pointing at **checkpointing** (load from a
  checkpoint + recent ops, bounding peak to ~O(current doc)) as the
  priority LARGE_VALUES stage over column-delta. Diagnose first whether
  the measured load is current-only (a bug — should not touch history)
  or with-history (then it is purely the checkpoint gap).

Roadmap consequence: storage lever = transparent LSM compression +
commit batching (row store, config/tuning); wire lever = columnar
RecordSet encoding (transport, needs design); cold-load-memory lever =
checkpointing; text-merge prerequisite = per-op provenance in the
op-log (eg-walker). column-delta sits behind all of these.

### Implementation slices (sequencing, 2026-06-13)

1. _(done)_ `text`/`blob` column kinds → opaque groove `Bytes`, whole-value
   LWW (no op-log yet).
2. _(in progress)_ groove **raw column-family** passthrough — KV storage below
   the table/IVM layer, for content/extents/checkpoints.
3. jazz **content store** — per-`(writer, row, column)` append-only byte
   streams + extents over the raw CF.
4. jazz **op-log** — `Insert`/`Delete` ops carrying extents in the text-column
   payload. **Both write paths are part of the first op-log slice** (confirmed):
   the **edit API**
   emits ops directly (editors), the **dump path** diffs a whole value into ops
   (common prefix/suffix trim first; Myers later). Read = **materialize-by-
   replay** over the winner's chain (LWW strategy = single path). **Reads replay
   without checkpoints in the first op-log slice** (confirmed) — O(ops since origin) per read;
   accepted, checkpoints optimize it in slice 6.
5. **content channel** — ship extents (+ checkpoint descriptors) on the bulk
   lane; parked-orphan handling for content refs.
6. **checkpoints** — materialized-value snapshots bounding cold-load + replay.

Later (gated): chunk index / cross-row dedup; `text-merge` (eg-walker replay
since LCA).

### Resolved notes

- Merge/effective-chain handling is covered by `Merges and the effective chain`.
- Checkpoint placement is an opportunistic, format-independent heuristic
  (decided 2026-06-13). We do not adopt eg-walker's strict critical-version cut
  discipline. Checkpoints are derived class-3 state, so _where_ we cut is a
  performance knob, never a correctness or format constraint; it can change
  anytime without touching storage format or replay correctness.
- The groove boundary is covered by `Relationship to groove`.
- Content serving membership is covered by `The content channel`: serving nodes
  must hold the row's op log to answer the membership check; ops are never
  evicted on serving nodes, only content bytes are.

### In-flight TODOs (not yet decided)

1. _(decided 2026-06-13 — settled boundary = globally-accepted, pending
   hot-write validation.)_ The chunk index's determinism anchors to the
   **globally-accepted** boundary (a clean settled cut). Open caveat to
   validate empirically: globally-accepted may be **too eager under hot
   concurrent writes** (index churn as the accepted frontier advances
   rapidly); if profiling shows churn, back off to a laggier per-stream
   durable high-water. Only matters once the chunk index ships (staged
   overlay; see Chunk index: severable).

### Related work

- **eg-walker** (Gentle & Kleppmann, arXiv 2409.14252): event graph as the
  persistent format; columnar RLE op encoding; transient merge state;
  critical versions; sequential fast path; maximally non-interleaving
  ordering. jazz's op log is this design generalized to a database column;
  `[needs: text-merge]` should be an eg-walker-style replay since the LCA.
- **WiscKey / Badger value logs**: key–value separation with log GC —
  the bundle design and its compactor.
- **Piece tables / piece trees** (editor lineage): the materialized-value
  cache structure.
- **CDC dedup lineage** (casync, restic, Xet) and **prolly trees**
  (Noms/Dolt): the chunk index's ancestry — here demoted from canonical
  format to derived index, which removes their hardest constraint (the
  chunker as a compatibility contract).

### Text storage after windowed encoding (target simplification, 2026-07-03)

Groove ch. 2 §2.9 (windowed record encoding) retro-simplifies the text stack.
These are committed design consequences, gated on the window codec landing;
until then the current mechanisms remain in force.

1. **One op representation.** Text ops live inline in version records,
   period. The extent-backed op-log side-channel (the tagged hybrid that
   split ops between cells and content extents by document size) is
   deprecated-by-design: run-coded history makes inline ops compact at rest,
   so the split loses its reason to exist — and with it the mis-tagging /
   double-encoding bug class. The content store keeps exactly two roles:
   materialized states (checkpoints) and genuinely large content bytes
   (big insert payloads still spill as content, which is value separation,
   not an op-log channel).
2. **Eager chain shipping is the default.** A text version's op-chain
   ancestors usually share its window; once shipping a history span costs
   window-bytes under stream compression (ch. 8 wire posture), including
   the chain with any text version is cheaper than deciding not to. The
   receiver-side ancestor-repair lane remains as the fallback safety net,
   no longer load-bearing.
3. **Checkpoints align to window seals.** The sealed window replaces the
   free-standing op-count cadence as the checkpoint unit: materialization
   folds from the nearest window-boundary state plus the open tail. One
   cadence concept; checkpoints stay node-local derived state.
4. **State hashes at window seals (not per version).** A per-version state
   hash would fight the run encoding (32 incompressible bytes against a
   ~3 B/edit budget); a hash at each window seal costs ~32 B per few
   hundred edits and verifies materialization at every boundary — the
   affordable core of state-addressed versions.

### Future work (gated)

- `[needs: text-merge]`: eg-walker replay since LCA at the merging node;
  maximal non-interleaving as the tested property; rich-text (formatting
  span) semantics on top.
- Cross-row storage dedup: refcounted chunk promotion at core; activate only
  if the duplicate-content-at-core counter says it pays for its GC.
- CDN/public serving via presigned grants (special case, not foundation).
- Streaming materialization for very large dumps; compression of op
  metadata beyond RLE if scenario 6 shows headroom vs. eg-walker's format.

## Open Questions

### Open questions

- 🔶 **`text` vs `blob` semantics.** Both are byte-oriented; decide whether
  `text` gains Unicode-position semantics or stays byte-identical to `blob` modulo
  schema identity.
- 🔶 **Planner enforcement.** The design rejects `text`/`blob` columns in
  filters, joins, ordering, and other query-planner predicates; the planner path
  still sees them as `Bytes` in some places, so enforcement is not yet complete.
- 🔶 **Op-log design status.** §12.2–12.6 specify the op-log design. Byte-offset
  edits, materialize-by-replay, large-value-aware authority merge semantics
  (`INV-LVAL-18`), and checkpoint placement paths are built; other detailed
  mechanisms remain staged as described in In flight.
- 🔶 **Column-delta status.** A column-delta encoding for keystroke text was
  prototyped and parked because it pays for large single values, not keystrokes;
  the measured findings remain in In flight.
- 🔶 **Same-`(row, column)` ref scope.** Load-bearing for GC in the design;
  local extent-backing creates same-row/column refs, but inbound ref enforcement
  is visibility-based, not a schema-level same-row/column validator.
- 🔶 **Future/gated.** Chunk index, bundles, truncation/GC horizon, hash-based
  checkpoint hydration, cross-row dedup, and `[needs: text-merge]` rich-text
  three-way merge are designed but unimplemented beyond the first engine path.
- ✅ **File rows use ordinary blob columns.** The replacement core deliberately
  diverges from alpha's historical `files`/`file_parts` convention. File helpers
  are ordinary userland rows containing a binary large value, with `mime_type`
  and `data` as the conventional column names. Permissions, sync, subscriptions,
  persistence, and helpers must therefore behave exactly like normal table/row
  behavior for that file row; there is no privileged file-parts side table.
- 🔶 **Serving topology.** The content-channel design serves content from edges;
  the implementation path serves from core until edge serving is built.
- 🔶 **Ops as a native groove column type.** The op-log uses a custom byte
  encoding for its ops (§12.2–12.6). If groove supported variable-width members
  inside a tuple — reusing groove's record encoding (groove §2.7) within the tuple
  — an op could be a _true groove column type_ rather than a jazz-private encoding,
  folding op storage (and potentially op merge) into the normal record/column path
  instead of a parallel mechanism. Blocked on the enabling groove feature
  (variable-width tuple members; groove ch. 2 open questions).

---
