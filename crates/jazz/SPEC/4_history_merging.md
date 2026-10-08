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
- `INV-HIST-8`: For a plain (`MergeStrategy::Lww`) column and for `_deletion`, Core MUST apply each accepted write to the row's post-image as a patch in the order it accepts the writes: every cell the write authored takes the write's value (last arrival wins), every cell it did not author keeps the row's value, and nothing a write authored is dropped. Concurrent writes to different cells of one row therefore all apply, and a write that does not author `_deletion` keeps the row's deletion state, so an edit made over an image older than an accepted delete leaves the row deleted.
- `INV-HIST-9`: `MergeStrategy::Counter` MUST be declared only on non-nullable integer user columns.
- `INV-HIST-10`: For `MergeStrategy::Counter`, a write MUST travel as its delta from the row image it was made over, and Core MUST add each accepted delta to the current value, so concurrent increments from the same base sum exactly. Core MUST reject a write whose delta would take the counter outside its column type's range rather than wrap, and a write that does not author a merge column MUST NOT replace that column's accepted ops with the writer's snapshot, also across schema versions.
- `INV-HIST-11`: Content and deletion state MUST be separate layers; content writes MUST NOT change the deletion register, and a current `DeletionEvent::Deleted` MUST hide the content-current row until a current `DeletionEvent::Restored` reveals it.
- `INV-HIST-12`: Accepted globally settled versions that become per-layer winners MUST be reflected in `jazz_{table}_global_current` or `jazz_{table}_register_global_current`.
- `INV-HIST-13`: Re-ingesting the same commit unit with identical version rows in a different order MUST be idempotent and MUST NOT create a conflict.
- `INV-HIST-14`: Rejected transactions MUST NOT appear as accepted row-history entries and MUST NOT participate in currentness/domination.
- `INV-HIST-15`: Core's post-image MUST be a deterministic function of the accepted writes in seq order: no wall clock, writer clock or other node-local state enters a merged value. Concurrent writes to different cells give the same post-image in any seq order, merge-strategy ops commute, and when two writes change one plain cell the one Core sequences last keeps it.
- `INV-HIST-17`: Content and deletion history MUST remain independently immutable and independently selected; a combined current row is a derived cache over their winners and MUST be reproducible from retained histories after restart or rebuild.
- `INV-HIST-20`: Core MUST resolve a write's base exactly or refuse the write: a base seq MUST name an accepted history record of the same row at that seq, and a pending predecessor MUST be an older transaction of the writer's own node. Otherwise Core MUST reject the write with a `MalformedCommit` reason saying the base is not supported yet; it never substitutes another base. A write whose pending predecessor has no fate at Core yet MUST NOT be held or stored by Core: after its cheap admission checks pass, Core MUST answer `RetryLater` naming that predecessor and store nothing for the write. Its writer MUST keep it pending, MUST NOT let a later upload of the same writer node on that link overtake it (other writers' uploads are not held back), MUST send it again from the predecessor (when it still holds the predecessor pending) after a backoff, and only its author MUST fail it with a surfaced "predecessor lost" error when the predecessor can no longer reach Core; a relay never fates it and forwards the `RetryLater` towards its author.
- `INV-HIST-21`: Whether a write maybe conflicts MUST be derived from the row's accepted history and never stored: a write is maybe conflicting iff it has a base and an accepted record of the row that is not of the writer's own chain (the accepted writes of its pending predecessor's node with a transaction time at most the predecessor's) lies after its seen seq and before it. The seen seq is the larger of its base seq and its chain root's settled base seq (followed through accepted predecessors, and through a rejected predecessor to that one's own predecessor), never a predecessor's later accepted seq. Inserts and blind updates are never maybe conflicting. Its conflict analysis MUST be the row's accepted writes after the seen seq and before the write, outside its own chain, whose authored columns overlap the write's.
- `INV-HIST-18`: A version parent MUST identify an exact prior version of the same physical table, branch key, row, and content/deletion layer; it MUST NOT encode a cross-row transaction dependency or a dependency between the content and deletion layers.
- `INV-TX-6`: A write MUST carry the base of the row image it was made over (§4.6). Core orders writes by its own seq, never by writer clocks: a write overrides every value accepted before it whatever its clock, and Core does not reject a write because the writer's clock is behind.

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
the writer's clock, orders accepted writes: each write applies over what
Core accepted before it (`INV-TX-6`, §4.6). A write names the row image it
was made over, its **base**, so history can tell whether it was made over
the row's latest accepted image. Core no longer rejects a write as
`CausalityViolation`, and no clock value takes part in a merge.

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
(`MergeStrategy::Lww`) takes a write's value iff the write authored it; the
write Core accepts last wins (`INV-HIST-8`, §4.6). For example, from a row
`{title:"a", body:"b"}` at seq 5, two writes both made over seq 5, `A`
setting `title="x"` and `B` setting `title="y", body="z"`, give
`{title:"y", body:"z"}` when Core sequences `A` first. `B` is then maybe
conflicting (its base, seq 5, is not `A`'s seq, the record before it), and
its conflict analysis names `A`, which also authored `title`. Sequenced the
other way the row is `{title:"x", body:"z"}`, and `A` is the maybe
conflicting write.

Counter columns use delta summation instead of last-writer selection. The counter
strategy (`MergeStrategy::Counter`) may be declared only on non-nullable integer
columns (`INV-HIST-9`, ch. 2). A counter write travels as its delta from the
row image it was made over, and Core adds each accepted delta to the current
value (`INV-HIST-10`). Concurrent increments therefore converge to the exact
total: from a base of `10`, a concurrent `+3` and `+5` merge to `18`, not to a
single last-writer value. The difference of two values of a `width`-bit type
lies in `-(2^width - 1)..=2^width - 1`, so the delta is carried as its
two's-complement value one bit wider than the column's type: the op cell holds
its low `width` bits in the column's own integer type, and the patch's
**counter signs** hold its sign bit. Every single write of an in-range value
is therefore expressible, an unsigned decrement and a change across the whole
type included (a `U8` set from `200` to `1` travels as low bits `57` with the
sign set, i.e. `-199`, never `+57`). Counter signs are a byte string, bit `i`
(least significant first) belonging to the `i`-th counter column of the
version's authored table in schema order; they are empty when no op is
negative, carry no trailing zero byte, and are always empty on a settled
image. They travel as `VersionRecord.counter_signs` on the wire and as the
history field `counter_signs` (after `authored_columns`); a non-canonical value is rejected at ingest. Core never wraps: when adding an op to the
row's current value would take the column outside its type's range (below `0`
or above the maximum for an unsigned type, outside `MIN..=MAX` for a signed
one), Core rejects that write with a `MalformedCommit` reason naming the
counter and the out-of-range sum, and the counter keeps its value. From `1` on
a `U64` counter, two concurrent `-1`s settle the first to `0` and reject the
second. Which of two such concurrent writes is rejected depends on the order
Core sequences them; accepted ops still commute (`INV-HIST-15`). A redelivered
commit unit is the same transaction
and is not applied again, so a retried increment counts once (`INV-EDGE-16`).

_Further invariants._ `INV-HIST-15` — the post-image is a deterministic
function of the accepted writes in seq order, with no clock or node-local
state in merged values; concurrent writes to different cells and
merge-strategy ops commute, and on a cell two writes both change the last one
Core sequences keeps it.

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

### 4.6 Last arrival wins (linear-history per-cell patch)

On the linear-history line Core applies each accepted write to the row's
post-image as a **patch**, in the order it accepts the writes: every plain
(`MergeStrategy::Lww`) cell the write authored, and `_deletion` when
authored, takes the write's value, and every other cell keeps the value the
row holds; merge-strategy columns (counters, sets) apply their ops in seq
order (§4.3). No timestamp takes part, no earlier image is rebuilt, no cell
is compared and nothing a write authored is dropped or set aside: on each
plain cell the **last write Core accepts wins** (`INV-HIST-8`,
`INV-HIST-15`).

A write still names the image it was made over (its base, below). Core uses
the base only to validate the write and to keep one writer's chain in order
(`INV-HIST-20`); history uses it to derive whether the write was made over
the row's latest accepted image, i.e. whether it **maybe conflicts** with
writes it never saw (`INV-HIST-21`).

**Base of a write.** Every row version a writer uploads carries a **base**:

- `seq` — the seq (`GlobalTime`) of the settled image of the row the writer's
  node held when it made the write (its global-current image), or none;
- `pending` — the `TxId` of the writer's own newest write to the same row
  that has no fate on its node yet, whose patch the writer's image included,
  or none. It is the node's own write even when the newest write in its
  pending overlay is a foreign one the node relays: a writer's chain is its
  own writes, and a foreign write's patch is never part of it.

So a write made over a settled image is `{seq: S}`; a write made over the
node's own pending write `P`, itself resting on the settled image at `S`, is
`{seq: S, pending: P}`; and a write over a pending chain with no settled image
under it (the row was inserted, or blindly updated, by that chain) is
`{pending: P}`. The settled seq travels with the pending predecessor because
the node rebases its pending overlay onto every newer settled image it
receives: the image the writer saw is the image at `S` plus its own pending
patches, not the image the chain started from.

A write with an empty base is an **insert**, or a **blind update** of a row
the writer's node holds no image of (it never loaded the row, or evicted
it). It applies like any other write and is never maybe conflicting.

The originator stores each pending write's base in its history record and
uploads it unchanged, so a resent write carries the same base. When a pending
predecessor receives its fate nothing is rewritten on the client.

**Validating the base at Core.** For an incoming write `W` from node `N` with
base `{seq: S, pending: P}` Core checks the base exactly or refuses the write
with a `MalformedCommit` reason saying the base is not supported yet
(`INV-HIST-20`): when `S` is `0` or names no accepted history record of the
row (for example a seq above the row's current seq, or a seq of another
row's write; one exact point read of the row's history at `S`), when `P`'s
node is not `N`, and when `P` is not older than `W`. Core never substitutes
another base. A predecessor Core holds no fate for yet (it has not arrived,
or Core holds it as a relayed or recovered Pending unit) is an ordering race,
not a refusal. Core does not hold `W` for it:

- `W` first passes its cheap admission checks (the session is `W`'s author
  and not anonymous, the clock is within tolerance, and the provenance is
  well formed); a failing `W` gets its refusal at once.
- Core then stores nothing for `W`, decides nothing, and answers its link
  with `RetryLater { tx_id: W, awaiting: P }` (SPEC 8). Core keeps no copy,
  queue or timer for `W`, so a Core restart changes nothing.
- The writer keeps `W` pending, with its local visibility, and stops
  uploading `N`'s writes on that link at the first one it must send again,
  so nothing chained after it overtakes it; other writers' uploads on the
  link go on. After a jittered backoff it uploads again from there, in
  outbox order, sending `P` first when it still holds `P` pending (moving
  `P` ahead of `W` when it was queued behind it). Once `P` has a fate at
  Core (acceptance or any rejection, a refusal before admission included),
  Core applies `W`.
- Only `W`'s author decides that `P` is lost. When the author no longer
  holds `P` pending (it does not know `P`, `P`'s upload already failed
  there, or `P` is settled there without having reached Core), `P` can
  never reach Core, and `W` fails at the author with a surfaced "predecessor
  lost" rejection instead of being retried forever.
- A relay that uploaded `W` for another node never fates it. It forwards the
  `RetryLater` towards `W`'s author. When it holds `P` queued it retries
  both as above; otherwise it drops its copy of `W` from its outbox, and the
  author's retry (or its reconnect) brings `P` and `W` to it again in order.

So one writer's chain always applies in its own order, and the two ways `P`
can be rejected end differently for `W`. A `P` rejected at Core has a fate
there: Core applies `W` over the settled row (the rejected `P`'s cells are
not in the image). A `P` rejected locally, before it reached Core, never gets
a fate there: Core keeps answering `RetryLater`, and `W` fails at its author
as "predecessor lost".

**Patch rule (Core only).** For each plain cell `c` the write authored, and
for `_deletion` when authored, the post-image takes the write's value. A cell
the write did not author keeps the current value. A write without
`authored_columns` (a full image) authors every cell, `_deletion` included.
Merge-strategy columns apply their op whatever the bases.

The post-image keeps the incoming write's identity and seq (it is the row as
of this write), takes `updated_by` and `updated_at` from that write, so
`updated_by` is always the author of the latest write applied to the row,
and keeps `created_by` and `created_at` from the previous image. Two
concurrent writes that change one cell therefore resolve to the one Core
sequences last; concurrent writes to different cells all apply (`INV-HIST-8`,
`INV-HIST-15`).

**Edit after delete.** Deleting is writing `_deletion`; an ordinary update
does not author it. An update whose base predates a delete Core has already
accepted therefore leaves the row deleted: Core accepts it, its authored
cells apply to the post-image underneath the deletion (a later restore
reveals them), and its history record is kept. It is maybe conflicting by the
rule below, since the delete was accepted after its base. A write that does
author `_deletion` (a delete, a restore, or a full image) applies it like
any other cell: the last one Core accepts wins.

**Maybe conflicting (derived, never stored).** Whether a write may have
overridden values its writer never saw is derived from the row's accepted
history, not stored: there is no flag bit and no copy of an overridden
value (`INV-HIST-21`).

- **What a writer saw.** A write's base names everything its writer had
  seen of the row: the accepted records up to its base seq `S` (the settled
  image its node held), plus its own pending chain. A predecessor's accepted
  seq is not part of that: Core may accept another writer's write after the
  chain's settled image and before the predecessor, and the writer saw none
  of it. So a write's **seen seq** is the larger of `S` and its chain root's
  settled base seq: following `P` to the newest accepted write of `P`'s node
  to the row whose transaction time is at most `P`'s (`P` itself when Core
  accepted it; when Core rejected `P`, which is not in history, `P`'s own
  predecessor), and from it along its own base the same way, to the first
  write whose base has no `P`; without `P` it is `S`. `S` is never older than
  the chain root's base (a node's settled image only moves forward), and is
  newer when the writer received Core's image at `S` before `P`'s fate: the
  writer then saw everything up to `S`, `P` included if accepted by then.
- The writer's **own chain** is the accepted writes of `P`'s node to the
  row whose transaction time is at most `P`'s. They are the writer's own
  pending writes it wrote over, so they never count against the write.
- A write is **maybe conflicting** iff it has a base and an accepted record
  of the row that is not of its own chain lies after its seen seq and before
  it. Its **resolved base seq** is the newest accepted record its writer had
  seen: the seen seq, moved forward over the own-chain records that directly
  follow it (none when the seen seq is none and the row's first accepted
  record is not of its own chain). A write is maybe conflicting exactly when
  its resolved base seq differs from the seq of the row's accepted history
  record immediately before it (none when it is the row's first accepted
  record).
- A write with an empty base (an insert or a blind update) is never maybe
  conflicting. Neither is a write whose base is its own author's
  immediately preceding accepted write of the row, nor any write of an
  offline chain that nobody else interrupted: in each case only the
  writer's own chain lies between what it saw and the write. Once another
  writer's write lands after a chain's settled image, every later write of
  that chain is maybe conflicting, not only the first: each overrode the
  row without having seen that write.

For example, carol, offline over the seed at `S`, writes `p1` (title) and
then `w` (body, base `{seq: S, pending: p1}`); dave's body edit is accepted
first, then `p1`, then `w`. `p1` is maybe conflicting with an empty conflict
analysis (dave touched only the body). `w` is maybe conflicting and its
conflict analysis names dave's write: `w`'s body overrode dave's, which
carol never saw, although the record right before `w` is her own `p1`.

One walk of the row's history computes every write's seen seq (each chained
write reuses its predecessor's), and any node holding the row's accepted
history records (their seq, transaction id, base and authored columns)
derives the same answer.

A write's **conflict analysis** is a walk of the row's history from its
seen seq to the write: the accepted records with a seq greater than the seen
seq (every earlier accepted record when it is none) and less than the
write's own, not of its own chain, whose authored columns overlap the
write's (a full image authors every column; `_deletion` counts as a column).
A maybe conflicting write whose intervening foreign writes all touched other
columns has an empty conflict analysis. The flag and the walk are exposed by
the node (`NodeState::write_conflict`); no public read API or TypeScript
surface carries them yet.

**Across schema versions.** Cells are matched by physical column id, so a
lens rename keeps its cell, and every authored cell applies whatever layout
the current image is stored under. The post-image takes the incoming
write's layout: its cells the write did not author take the current image's
value where the current layout carries the same physical column with the
same type, and the writer's snapshot value otherwise. A cell only the
current image's layout carries is not part of a post-image in the incoming
layout (readers under the other schema version see it through the lens
path). Merge-column ops cannot apply across layouts this way, so Core rejects a
write that authors a merge column under a schema version other than the one
the row's current image is stored under, with a `MalformedCommit` reason
saying this is not supported yet ([#3899](https://github.com/garden-co/jazz/issues/3899)).
A cross-layout write that leaves a merge column alone still carries the
writer's snapshot of it as an absolute value, which may predate ops Core has
accepted since; in the post-image each merge column it did not author takes
the row's settled value from the current image instead
(`INV-HIST-10`). That value carries over when the current image's schema has
a column of the same name, type and merge strategy that the lens path between
the two versions leaves alone; a column the current image's schema lacks, and
that no lens op renames or copies into, takes the lens default, since no op
can have touched it under that layout. For any other mapping (a renamed or
copied merge column, or one whose type or strategy changed) Core rejects the
write with the same not-supported-yet `MalformedCommit` reason rather than
guess. Core also refuses, with the same reason, a cross-layout write whose
schema would not carry a merge column of the current image: one the write's
table lacks, or has under another type or strategy, or that the lens path
renames, copies, adds or drops. Its image would replace the row without that
column, and the next write under the image's schema would rebuild it from the
lens default, silently losing every op Core had accepted on it (v2 adds a
counter `likes`; a v1 write over a v2 image with `likes = 5` would leave a
later v2 write reading `likes = 0`). The refusal holds whatever the stored
value, including one equal to the lens default, and also when the write's own
table has no merge column at all. It applies on every path that mints a seq:
a foreign commit unit and Core's own mergeable or exclusive commit.

**Only Core derives post-images.** A `FateUpdate` carries the fate and seq,
not Core's post-image; Core's post-image reaches other nodes in view updates
for the rows they subscribe to. An originator that receives the accepted fate
of its own write may settle it locally by applying its patch over the image it
holds (authored plain cells overwrite, merge ops apply; across layouts the
write replaces the row, keeping carried merge cells), but that image is only a
prediction of Core's, and it replaces it with the authority's post-image at
the same seq when that arrives; only a newer seq keeps a stored row. The
originator never validates bases, and makes no prediction (keeping its stored
image, and failing nothing) when

- it already holds Core's image at a later seq: Core applies writes in seq
  order, so that image already counts the write, and applying it again would
  apply its merge ops twice and move the row back to an older seq;
- the prediction cannot be made over its stale image: a merge column's
  settled value cannot be carried across schema versions (above), or a
  counter op leaves its type's range over the stale value. Core accepted the
  write over its own image, so this is the local image's staleness, not a
  reason to fail the fate (which would wedge fate ingest).

In both cases the write shows locally once Core's post-image for the row
arrives. Only Core, which mints seqs in order and refuses such writes before
minting one, treats either case as an error. The pending local overlay is not
merged and always wins locally.

A base seq names Core's image at that seq. When the originator's image at
`S` is its own prediction of Core's post-image (its write's fate arrived
before Core's image did), a value Core accepted before `S` that the
originator had not yet received still counts as seen: a write over that
image is not maybe conflicting although its writer never saw that value.
Naming the seq of the newest image Core itself delivered would make the
derivation exact; it is listed under Open Questions.

**Durable layout.** History, global-current, ahead-current and ahead-shadow
records carry no stamp fields. A history record carries, after
`authored_columns` and `counter_signs` (§4.3):

- `seq` (`U64`) — the write's seq; `0` on a pending record (below);
- `base_seq` (nullable `U64`) — the base's settled seq;
- `base_pending` (nullable `(U64 tx time, UUID node)`) — the base's pending
  predecessor.

History is keyed `(branch_key, row_uuid, seq, tx_time, tx_node_id)`: "the row
at seq `S`" is one prefix read, and "the row's writes after `S`" is one range
read. Pending writes (a node's own uploads, and foreign writes a relay holds
before their fate) are held in the same table with the same record layout and
`seq = 0`, so they sort before every accepted write of the row and never fall
in a range after a base seq. An accepted fate moves each of the transaction's
records from its `seq = 0` key to its key at the transaction's seq in the batch
that stores the fate; a rejected fate deletes them (SPEC 2 §2.7.1).

**Wire layout.** The wire `VersionRecord` carries, after `authored_columns`,
the version's `base` (`{ seq: Option<GlobalTime>, pending: Option<TxId> }`),
then `counter_signs` (§4.3). An upload carries its write's base. A history
record a peer receives carries the base Core stored; its seq is the
transaction's accepted `GlobalTime`, which travels with the bundle and is not
repeated per record. The byte receipt is the protocol test of the
`VersionRecord` postcard layout.

**Per-row cost.** A history record costs its `seq` (8 bytes) and its two
nullable base fields over the former stamp-free layout, and nothing on
current rows.

### 4.8 Subsumed merge-strategy backlog

The former TODO notes on complex merge strategies are treated as future surface
area over this chapter's deterministic merge contract. Built-in strategies cover
the first engine paths; richer set/map/rich-text/custom strategies must still
produce deterministic, grouping-insensitive merge results and must fail closed
without wedging authority progress.

## Open Questions

- 🔶 [#1782](https://github.com/garden-co/jazz/issues/1782) — External merge strategies and schema-version movement.
- A base seq that names the originator's own prediction of Core's post-image
  (§4.6) is taken as Core's image at that seq, so the maybe-conflicting
  derivation can miss a value Core accepted before it that the originator
  had not received. An exact derivation needs the originator to name the
  newest seq whose image Core delivered, with its own writes accepted after
  it as the pending chain (no tracking issue filed yet).
