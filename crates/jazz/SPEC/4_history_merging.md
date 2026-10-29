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
- `INV-HIST-24`: A text-merging column (`MergeStrategy::Text`, `Markdown` or `Html`) MUST be declared only on a non-nullable string user column and MUST store a plain string; writes to it travel as plain values. Only Core merges: when a version before the write that its writer never saw (§4.6) authored the column, Core MUST three-way merge the value its writer last saw of the column (the newest commit on its chain that authored the column, as that commit sent it, or else the accepted version the chain was built on), the value it holds and the write's value at token boundaries: changes to disjoint parts of the base all apply, a change both sides made applies once, inserts at one point apply in seq order, overlapping pure inserts and deletes union (as do one side's inserts with the other's replacements), and of two overlapping replacements the write sequenced later wins. Otherwise, or when the merge would spend more than a fixed budget of diff steps, the write's value stands as sent. For a version whose value a merge changed, Core MUST keep the edits back to the value as sent (`_jazz_text_in`) while its sender may still build on it.
- `INV-HIST-11`: Content and deletion state MUST be separate layers; content writes MUST NOT change the deletion register, and a current `DeletionEvent::Deleted` MUST hide the content-current row until a current `DeletionEvent::Restored` reveals it.
- `INV-HIST-12`: Accepted globally settled versions that become per-layer winners MUST be reflected in `jazz_{table}_global_current` or `jazz_{table}_register_global_current`.
- `INV-HIST-13`: Re-ingesting the same commit unit with identical version rows in a different order MUST be idempotent and MUST NOT create a conflict.
- `INV-HIST-14`: Rejected transactions MUST NOT appear as accepted row-history entries and MUST NOT participate in currentness/domination.
- `INV-HIST-15`: Core's post-image MUST be a deterministic function of the accepted writes in seq order: no wall clock, writer clock or other node-local state enters a merged value. Concurrent writes to different cells give the same post-image in any seq order, counter and set ops commute, and when two writes change one plain cell the one Core sequences last keeps it.
- `INV-HIST-17`: Content and deletion history MUST remain independently immutable and independently selected; a combined current row is a derived cache over their winners and MUST be reproducible from retained histories after restart or rebuild.
- `INV-HIST-20`: Core MUST resolve a write's base or refuse the write: a pending predecessor MUST be an older transaction of the sender's own node, otherwise Core MUST refuse the write; it never substitutes another predecessor. Core MUST NOT wait for a pending predecessor it holds no fate for (unknown there, or held as a relayed or recovered Pending unit): such a predecessor never arrives, and Core MUST reject the write as built on a rejected write. A write chained on a predecessor parked at Core for a missing schema MUST park behind it and be decided after it, for a bounded wait: once the predecessor has been parked that long, Core MUST decide the write as written, as for a predecessor that never arrives. A write built on a rejected write is rejected too (`predecessor_rejected`, §4.6).
- `INV-HIST-21`: Whether a write maybe conflicts MUST be derived from the row's accepted history and never stored. A write's base is the version of the row it was built on: an accepted seq, or an unsettled commit (resolved at the authority to the seq it landed at). Its writer saw every version up to a base seq, or a base commit's version and everything that version saw, down the chain. A write is maybe conflicting iff it has a base and an accepted version of the row before it is one its writer never saw; inserts and blind updates are never maybe conflicting. Its conflict analysis MUST be those missed versions whose authored columns overlap the write's. A value-merging column MUST merge a write with its base version's value only when a missed version authored that column, and otherwise keep the write's value as sent.
- `INV-HIST-23`: One writer node's commit units MUST reach the fate authority in the order they were written, on every link, and Core MUST decide them in arrival order. The order a writer's writes were written in is their `TxId` order: a writer MUST mint a write's `TxId` when the write takes its place in the writer's order (its owner's mutation queue, which publishes one write at a time in that order), never later, so `TxId` order, local commit order, upload order and recovery's `TxId`-sorted replay agree. A writer's link MUST NOT upload a write ahead of an earlier write of the same node that it holds back (for example while a large value waits out a rate limit); other writers' uploads are not held back. A write that reaches Core after Core accepted a newer write of the same node to the same row is a protocol violation: Core MUST reject it with a `MalformedCommit` reason and MUST NOT apply it.
- `INV-HIST-18`: A version parent MUST identify an exact prior version of the same physical table, branch key, row, and content/deletion layer; it MUST NOT encode a cross-row transaction dependency or a dependency between the content and deletion layers.
- `INV-REF-1`: A read MUST compose its layers by one rule: the top layer holding a row supplies it whatever its stamp, each layer masks the layers below it before any predicate or relational operator, and content and deletion registers are each stacked by that rule independently.
- `INV-REF-2`: A read's refs MUST follow its tier: `Global` reads origin alone; `Local` and `None` read the local delta over origin.
- `INV-REF-3`: A layer narrowed by an access path MUST mask lower layers by a relation exact under that narrowing (its own rows only when one row-id prefix bounds every layer; otherwise its presence, read without the access path: the local delta's presence over origin under an index, read whole or by origin's candidates' row ids, whichever reads fewer rows; the head's presence read by the base candidates' row ids over a branch view's base), and an indexed branch-view read MUST read each content layer only under the index prefix and each deletion-register layer only for the content candidates' row ids.
- `INV-REF-4`: No stored or sent byte MUST encode that a write's unsettled base is its writer's own: a base names an unsettled commit by its sender's id (only the sender's own may go by how far back it is), and each relay renames bases into names its upstream knows. A replica shows its pending writes over each newer settled image it receives, but a write over a pending write is built on that write alone and never claims the settled images shown under it.
- `INV-REF-5`: A node MUST store its local ref only as the sparse delta over origin: committing, rebasing and settling a pending write MUST NOT store a copy of the origin image it overrides, and a read MUST mask origin by the delta itself.
- `INV-TX-6`: A write MUST carry the base of the row image it was made over (§4.6). Core orders writes by its own seq, never by writer clocks: a write overrides every value accepted before it whatever its clock, and Core does not reject a write because the writer's clock is behind.

## Details

### 4.1 The version DAG

A row's history is modeled as a directed acyclic graph of **row versions**. Each
version is identified by the `TxId` that wrote it and names zero or more direct
`parents` (ch. 2). Every parent resolves to the same physical table, exact branch
key, `RowUuid`, and content/deletion layer as its child (`INV-HIST-18`). Thus
`parents` are history edges only: mergeable transactions have no general
dependency graph, and content does not parent deletion or vice versa. The same
separation governs serializable first-committer-wins: a content write is compared
with the content winner, and a deletion/restore write with the deletion winner;
row/predicate read checks validate the visible state they observed instead. Thus
content `C` followed by first delete `D` gives `D` no history parent, and a
later restore parents `D` rather than `C` (covered by
`serializable_delete_compares_the_deletion_register_not_content` and
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

Text-merging columns merge concurrent edits to one string. The text
strategies (`MergeStrategy::Text`, `Markdown`, `Html`; `text`, `markdown`
and `html` in the DSL) are opt-in and may be declared only on non-nullable
string columns (`INV-HIST-24`). The column stores a plain string, inline or
as a large value (ch. 19), and a write to it travels as its plain value.
Only Core merges; replicas apply a pending text write as sent, like any
patched cell, until Core's image arrives.

Core reads what the write was built on from its base (§4.6). When no
version before the write that its writer never saw authored the column
(the common case: nothing concurrent, or a concurrent write to other
columns), the write's value stands as sent: no base is read and nothing is
diffed. Otherwise Core merges three values: the base, i.e. the column's
value the write's writer last saw of the column (walking its chain of
unsettled commits back to the newest that authored the column, at that
commit's value as it sent it, or else to the accepted version the chain
was built on: a commit that left the column alone holds Core's value,
which its writer never saw); the value Core holds; and the write's value:

- each value is cut into tokens (words, single whitespace and other
  characters; for `Markdown` also delimiter runs, ordered-list markers and
  link destinations; for `Html` also tags and entities), so a merge never
  splits one. In scripts written without spaces (CJK, Thai and the like)
  and for emoji each character is a token; combining marks, variation
  selectors, skin-tone modifiers and zero-width-joined characters stay
  with the character they complete. Which characters form words is fixed
  by Jazz's own tables, not the toolchain's Unicode version, so every Core
  cuts a value alike. A `<`, `&` or `](` with no closer within a bounded
  window is a token of one character;
- each side's changes are diffed against the base, and both apply where
  they touch disjoint tokens; a change both sides made identically applies
  once (near-identical concurrent inserts both apply, as in diff3);
- inserts at one point apply in seq order, the held value's first;
- where changes overlap and each only inserts or only deletes, or one side
  only inserts, the deletes and replacements apply and the inserts are kept;
- of two overlapping replacements, the incoming write (sequenced later)
  wins that span.

A diff that would take more than a bounded number of edit steps to make
minimal splits where its search got furthest and diffs the rest on its
own: the result may be less than minimal, but stays fine-grained, so a
heavily edited value loses no side's edits to the bound, and merging costs
time linear in the value's size times the bound.

A merge may also spend only a fixed budget of diff work, counted in search
steps (each diagonal a diff search visits and each token it slides over;
`MERGE_WORK_BUDGET`, 100 million: a 1 MB value with 10,000 scattered edits
a side takes about a fifth of it). A merge that would spend more stops,
and the write's value stands as sent, exactly as the later write wins two
overlapping replacements. The budget counts work, not time, so every Core
reaches the same result; the conflict analysis reports the column as
overlapping either way.

From `The cat sat.`, a concurrent `The black cat sat.` and `The cat
slept.` merge to `The black cat slept.` in either order. When a merge
changes a write's value, Core keeps for the merged version (in
`_jazz_text_in`, keyed by table, row, branch key and seq, made with the first
such merge) the token-level edits from the merged value back to the value
as sent, per column: a later write built on that commit (an offline
chain) is merged over what its writer saw, the value as sent. A later write
of the same upload to the row was built on the earlier one as sent, and
merges over it the same way. Only a commit's sender builds on it
unsettled, and a sender's commits arrive in order, so once one of its
writes to a row is built on an accepted version (or on none), its older
entries for that row are dropped: a row keeps entries only for its
writers' current unsettled chains. A write built on a rejected commit is rejected
too (§4.6), so a base is always a value Core accepted. The merge is
token-level, not tree-aware: it keeps markup tokens whole but does not
check that merged markdown or HTML is well formed. Conflict analysis
(`INV-HIST-21`) stays column-granular, so concurrent writes to one
text-merging column are reported as overlapping even when they merged
cleanly.

_Further invariants._ `INV-HIST-15` — the post-image is a deterministic
function of the accepted writes in seq order, with no clock or node-local
state in merged values; concurrent writes to different cells and
counter and set ops commute, and on a cell two writes both change the last one
Core sequences keeps it.

### Durable codec profile

Each persistent Jazz root declares one complete Jazz storage format:
`jazz.node-storage.v2` for clients, relays and Core; `jazz.catalogue-storage.v1`
for the independent server catalogue; `jazz.account-storage.v1` for the account
journal. All compose Groove's mandatory base codecs; node roots additionally
require `groove.durable-index.v2`. Auxiliary roots do not inherit node formats.

The manifest requires an exact inventory match before any ordinary record is
decoded or mutated. Groove treats Jazz identifiers as opaque metadata. Each
individual encoding still has a named byte contract, golden fixtures, and
rejection/reopen receipts in the encoding registry, but no separate persisted
Jazz marker. A change incompatible with a released root layout versions that
root's marker; changes within this unreleased storage-breaking batch are
consolidated into the same v1 contract. There is no physical epoch change.
Earlier per-family profiles are refused without migration. Some of these
encodings also change wire bytes; the wire version bump for them ships with
the alpha.60 release (`dev/proofs/storage-format-cleanup-compatibility.md`).

`jazz.server-catalogue-entry.v1` remains an encoding receipt covered by
`jazz.catalogue-storage.v1`. Its outer
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
row holds; merge-strategy columns (counters, sets, text) apply their ops in
seq order (§4.3). No timestamp takes part, no earlier image is rebuilt, no cell
is compared and nothing a write authored is dropped or set aside: on each
plain cell the **last write Core accepts wins** (`INV-HIST-8`,
`INV-HIST-15`).

A write still names the image it was made over (its base, below). Core uses
the base only to validate the write (`INV-HIST-20`); history uses it to
derive whether the write was made over
the row's latest accepted image, i.e. whether it **maybe conflicts** with
writes it never saw (`INV-HIST-21`).

**Base of a write.** Every row version a writer uploads over an image of
the row it held carries a **base**: the version of the row that image was.
It is one of:

- `Seq(S)` — the accepted version at seq `S` (the settled image the writer's
  node held, with no pending write of its own to the row over it);
- `Own { back }` — the writer's own newest write to the row that has no fate
  on its node yet, `back` transactions before this one: the writer saw that
  write as it sent it, and whatever that write was built on;
- `Commit { node, tx }` — an unsettled commit by its sender's id, as a relay
  names one of its own (below).

A replica shows its pending writes over each newer settled image it
receives, but a write over a pending write is built on that write alone
(`INV-REF-4`): it never claims the settled images shown under it, and only
Core merges concurrent edits.

A write with no base is an **insert**, or a **blind update** of a row the
writer's node holds no image of (it never loaded the row, or evicted it). It
applies like any other write and is never maybe conflicting.

The originator stores each pending write's base with it and uploads it
unchanged, so a resent write carries the same base.

**A write built on a rejected write is rejected.** When Core rejects a write
`P`, every write built on `P` (directly, or down a chain of writes each built
on the one before) is rejected too, with reason `predecessor_rejected`: its
image carried `P`'s values, and applying it would bring them back. How an app
retries such a chain is its own (`INV-HIST-21`).

**Validating the base at Core.** For an incoming write `W` from node `N`
built on a pending predecessor `P`, Core refuses the write when `P`'s node
is not `N` or `P` is not older than `W` (`INV-HIST-20`). Core never
substitutes another predecessor. A `Seq` base is not checked against the
row's history: it only feeds the derived analysis, where a seq the writer
could not have seen makes the write count as built on less, never more.

Core never waits for `P`. A writer's commit units reach Core in the order
they were written (SPEC 14: the Writes channel is one FIFO and every link
uploads the shared outbox in order), so when `W` arrives, `P` has either
reached Core already or never will:

- When Core holds no fate for `P` (it does not know `P`, or holds it only as
  a relayed or recovered Pending unit), `P` never arrives for a decision.
  Core treats it like a `P` it rejected: `W` is rejected too
  (`predecessor_rejected`).
- When `P` is parked at Core because its schema is not known yet, `W` parks
  behind it (an in-memory lookup) and is decided after it once the schema
  arrives (`INV-HIST-20`). The wait is bounded: once `P` has been parked for
  `PARKED_PREDECESSOR_WAIT_MS` (60 s) on Core's ingest clock, the next commit
  ingest decides `W` as written, as if `P` never arrived. If `P`'s schema
  arrives after that, `P` is late and refused (below), so a write whose
  schema never arrives costs only itself, not its writer's later writes.
  The ingest clock is the newest wall time any commit ingest has seen, held
  in memory with the parked units, and no timer drives it: on an idle Core
  `W` stays parked past the wait until the next commit ingest (or `P`'s
  schema) arrives, and a forward jump of Core's clock runs out every
  parked wait at once. A restart drops the parked units and the clock
  together; writers resend their pending writes in order and the waits
  start again.
- When `P` reaches Core after Core accepted a newer write of `N` to the same
  row, the order was violated. Core rejects `P` with a `MalformedCommit`
  reason saying it arrived after a later write of its writer, and does not
  apply it, so the row never goes back to an older image (`INV-HIST-23`).
  The check costs at most the writes accepted since `P`'s base, never the
  row's whole history:
  - It reads nothing more when the row's current image was written by `N`
    (compare its transaction time) or is `P`'s base.
  - When `P` has a base seq `B` and the row's current seq is `C`, it reads
    the key fields of the row's history records at seqs `B+1..=C`: one range
    read per history table, since history is keyed
    `(branch, row, seq, tx_time, node)`. `N` had seen `B` when it wrote `P`,
    so a later write of `N` lies there if anywhere.
  - When `P` has no base seq (an insert, upsert or restore made over no
    image of the row), it reads one per-row marker of `N`. When Core decides
    a write `W` of `N` chained on a predecessor `X` it holds no decision for
    (unknown, or stored Pending), it first records in the marker (the
    node-local direct record store `jazz_superseded_writes_v1`, keyed by
    physical table id, branch, row and node) that `N`'s writes to the row
    through `X`'s transaction time were passed over; a write of `N` at or
    before that time arriving later is late. Core records it before it
    applies `W`, and also when it refuses `W`. The physical table id is kept
    across table renames, so a marker recorded under one schema version
    guards writes made under another. The marker is exact for `P`: the
    first write of `N` Core accepted after `P` was written while `P` was
    still pending at `N`, so it was chained on `P` or on a later write `X`
    Core had not accepted either. If Core held no decision for `X`,
    applying the write recorded a marker at or after `P`; if Core had
    refused `X`, refusing `X` did, or one link further down the chain, down
    to the write chained directly on `P`. A chain over predecessors Core
    already decided (accepted or refused) records nothing and reads at most
    the predecessor's transaction.
  - Markers are not history. A Core rebuilt from history alone
    (`INV-HIST-17`) starts with none, so until a writer passes over a
    predecessor again, a write without a base seq that arrives after a
    later write of its node accepted before the rebuild is no longer
    refused as late; writes with a base seq keep the exact range check.
  - Markers are never collected. Row deletion leaves a restorable tombstone
    and keeps the row's history, so a late write to a deleted row must
    still be refused, and nothing removes a branch's rows. A marker is
    written only when a write passes over a predecessor Core never saw (a
    transport fault, or a parked predecessor), so their number is bounded by
    those events, not by write volume.

A `P` rejected locally before it reached Core (for example its large value
was not staged) therefore ends like a `P` rejected at Core: `W` is rejected
too. A writer's link keeps the order on its side too: while one
of its writes waits (a large value rate-limited or still uploading), none of
that writer's later writes goes up ahead of it; other writers' writes on the
same link go on.

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

- **What a writer saw.** A write built on `Seq(S)` saw every accepted version
  of the row up to `S`. A write built on an unsettled write `P` (accepted at
  some seq) saw `P` and everything `P` saw, down the chain. It did not see a
  version Core accepted between what `P` saw and `P`: the writer only had
  `P` as it sent it.
- A write is **maybe conflicting** iff it has a base and an accepted version
  of the row before it is one its writer never saw. Its **resolved base
  seq** is the newest accepted version such that its writer saw it and every
  version before it.
- A write with no base (an insert or a blind update) is never maybe
  conflicting. Neither is a write of an offline chain that nobody else
  interrupted: each write of the chain saw every version before it. Once
  another writer's write lands between a chain's settled image and one of
  its writes, that write and every later write of the chain are maybe
  conflicting, not only the first: each overrode the row without having seen
  that write.

For example, carol, offline over the seed at `S`, writes `p1` (title, built
on `Seq(S)`) and then `w` (body, built on `p1`); dave's body edit is accepted
first, then `p1`, then `w`. `p1` is maybe conflicting with an empty conflict
analysis (dave touched only the body). `w` is maybe conflicting and its
conflict analysis names dave's write: `w`'s body overrode dave's, which
carol never saw, although the version right before `w` is her own `p1`.

A write's **conflict analysis** is the accepted versions before it that its
writer never saw whose authored columns overlap the write's (a full image
authors every column; `_deletion` counts as a column). A maybe conflicting
write whose missed versions all touched other columns has an empty conflict
analysis.

**Merging needs the same base.** A merge-strategy column that merges values
rather than ops (text, §4.3) merges a write with the version it was built on
only when a version the writer missed authored that column; otherwise the
write's value stands as sent, with no lookup. The base value is the column's
value in the version the write was built on (for a write built on `P`, `P`'s
value as accepted).

On SQLite a commit carries, beside its operations, the base of each row it
writes over an image its writer held (`WireBase`: the row, by the index of
the commit's first op that writes it and its branch key, and the version it
was built on as `BuiltOn`). A non-durable session (a worker's tab) may hold
its relay's unsettled version of a row and send it as `Seq` of a seq at or
above the local base. A relay renames every base into names its upstream
knows before it forwards the commit: an upstream seq stays; its own
unsettled version (one of its own commits, pending or settled), the
session's `Own` predecessor and the session's own `Commit` all become
`Commit { relay node, tx }` of the relay's own commit that carries them, or
the upstream seq that commit already landed at, so Core never sees a
relay's local seqs. A version in the relay's local range that is no commit of
its own (one from before the relay's store was reset) cannot be named
upstream: the write counts as built
on the newest version of the row the relay holds accepted upstream, so it may
be flagged over a version its writer saw but never the other way round. A
base on a commit the relay rejected or never received rejects the write there
(`predecessor_rejected`); a `Commit` of another node than the sender's is
refused.

Core resolves each base before it applies the commit: `Seq(S)` is the
accepted version at `S` (not checked against the row's history: a seq the
row never had only makes the analysis more conservative); `Own` and `Commit`
(only of the sender's own node) are where the named commit of the sender
landed (`_jazz_settled`), and one Core rejected or never received rejects the
commit (`predecessor_rejected`). It keeps, in the version the commit wrote
(the row table and history alike), `_base`: the resolved `Seq` as is, or
the seq the named commit landed at negated; and `_cols`, the authored stored
columns as a bitmask over their positions in the row table (stored tables
only grow by `ADD COLUMN`, so a position never moves). An app column cannot
take the name of a stored row column (`id`, `rid`, `_b`, `_seq`, `_st`,
`_base`, `_cols`). A delete or restore authors `_st`, the version's state
bits (bit 0: its commit wrote no content; bit 1: deleted). Both are NULL on every other replica and for Core's
own writes: those are made over the current image, so they are never maybe
conflicting, and in another write's analysis they author every column. The
flag, the walk and the merge base are derived on demand
(`Db::write_conflicts` and `Db::merge_base`, behind the `testing` feature);
no public read API or TypeScript surface carries them yet.

**Across schema versions.** Core merges every write into the row one
physical column at a time, whatever schema version it was authored under
([#3899](https://github.com/garden-co/jazz/issues/3899)). The post-image is
laid out in the row's **union layout** (ch. 10 §10.3, "Union row cases"): the
columns of the schema version that maps the table and ranks last in the
catalogue order every node shares, followed
by every other column of the table's lineage that no lens path derives from
one of those (a column a newer schema dropped, or one a sibling schema
added). For each column of that layout:

- when the write authored the column it is carried on from (the same
  physical column, or one the lens path renames or transforms into it), a
  plain column takes the write's value and a merge column applies the write's
  op to the row's value;
- otherwise the column keeps the row's value. A column the current image
  does not hold yet (its schema came after the image was written) takes the
  value a reader of that column would derive through the lens path: the
  carried-on or copied cell, or the lens default.

A copy (`CopyColumn`) is a column of its own: a write of the source column
does not change the copy. `_deletion` and the identity and provenance rules
are unchanged. Counter range checks read the row's value of the counter's
physical column, so they hold across schema versions too.

Examples. v1 `todos {title, done}`; v2 adds `dueDate`. alice (v2) inserts
`{a, false, 10-01}`; bob (v1) writes `done = true`. The post-image is
`{a, true, 10-01}`: a v2 reader keeps alice's `dueDate`, a v1 reader sees
`{a, true}` ([#3562](https://github.com/garden-co/jazz/issues/3562)). If v2
instead also drops `legacy`, and bob writes `legacy = "x"` and `done = true`,
the post-image holds v2's columns with `done = true` and `dueDate` kept, plus
`legacy = "x"` as a union column; a v1 reader sees `legacy = "x"`, and a
later v2 write keeps it ([#3838](https://github.com/garden-co/jazz/issues/3838)).
A v1 write without the `likes` counter v2 added leaves `likes` at its
accepted value, and a v2 counter op applies over a row a v1 write created
(`INV-HIST-10`).

Core refuses a write with a `MalformedCommit` reason saying it is not
supported yet only when a cell it would carry into another physical column
is an enum value with no case in that column's enum.

**Only Core derives post-images.** A `FateUpdate` carries the fate and seq,
not Core's post-image; Core's post-image reaches other nodes in view updates
for the rows they subscribe to. An originator that receives the accepted fate
of its own write may settle it locally by applying its patch over the image it
holds with the same per-column merge (authored plain cells overwrite, merge
ops apply, every other column keeps its value), but that image is only a
prediction of Core's, and it replaces it with the authority's post-image at
the same seq when that arrives; only a newer seq keeps a stored row. The
originator never validates bases, and makes no prediction (keeping its stored
image, and failing nothing) when

- it already holds Core's image at a later seq: Core applies writes in seq
  order, so that image already counts the write, and applying it again would
  apply its merge ops twice and move the row back to an older seq;
- the prediction cannot be made over its stale image: a cell cannot be
  carried into another column (above), or a counter op leaves its type's
  range over the stale value. Core accepted the
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

**Durable layout.** History, global-current and ahead-current records carry
no stamp fields. A history record carries, after
`authored_columns` and `counter_signs` (§4.3):

- `seq` (`U64`) — the write's seq; `0` on a pending record (below);
- `base_seq` (nullable `U64`) — the base's settled seq;
- `base_pending` (nullable record `{time: U64, node: UUID}`, which costs a null byte and an offset when null) — the base's pending
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

### 4.7 Refs: local work over origin

Every row image a node holds has a coordinate `(table, branch_key,
row_uuid)` (SPEC 11) and a **ref**, which says where the image comes from:

- **origin** (`Remote(Origin)`): Core's settled state as this node last
  received it. Core is the only remote and the only one that mints seqs.
- **local** (`Local`): this node's working state, which is origin plus every
  pending write the node holds (its own, and those it relays) that origin has
  not fated yet.

This is git with one remote. Origin is `origin/main`, the local ref is the
local branch with unpushed commits, and uploading is a push. Core accepting a
write is a rebase onto origin with per-cell auto-resolve (§4.6). A **maybe
conflicting** write (`INV-HIST-21`) is one Core rebased rather than
fast-forwarded: another writer's write landed between what its writer saw and
the write. Settling moves a write from the local ref into origin.

Storage is delta-encoded and does not name refs. Origin is
`jazz_physical_{id}_global_current`, stored in full. Local is origin plus
the sparse delta `jazz_physical_{id}_ahead_current`, which holds the newest
pending image of each row, already folded over origin's image. Nothing else
is stored per ref: no copy of the origin images the delta overrides (the
`ahead_shadow` table that held them is retired; the read masks origin by the
delta's presence instead). The tier of a read picks its refs
(`INV-REF-2`): `Global` reads `[origin]`, and `Local` and `None` read
`[local delta, origin]`.

**The stack rule.** A read is a stack of layers, top first, and the top
layer holding a row supplies it, whatever its stamp (`INV-REF-1`). Every
layer masks the rows of the layers below it before any predicate or
relational operator runs. Content and the deletion register are stacked
separately by the same rule (`INV-BVIEW-9`, `INV-BVIEW-10`). The stacks are:

```text
plain current read        [local, origin]
branch view, live base    [head@local, head@origin, base@local, base@origin]
branch view, frozen base  [head@local, head@origin, snapshot of base]
```

**Masks.** A layer narrowed by the read's access path must hide lower rows
by a relation that stays exact under that narrowing (`INV-REF-3`):

- _Own rows._ The layer's own row ids. Exact when the layer is whole, or
  when one row-closed path (a row-id prefix) bounds every layer of the stack.
- _Presence._ The layer's row ids, read without the index, so an edit that
  left the predicate still hides the row below. Only the layer's rows that
  share a lower candidate's id can hide one, and ids no candidate holds hide
  nothing, so the presence may be read whole or just for those ids, and it
  is read whichever way is bounded by the smaller side:
  - Under an index the local delta masks origin by its presence read
    whichever way reads fewer rows (a pending edit that moved a row out of
    the prefix still hides its origin image either way). Read whole, it
    costs the delta's pending rows; looked up by origin's candidates under
    the prefix, one probe each. A synced node holds few pending rows and
    many candidates (read whole); a local-only node, a long-offline one or
    a draft branch that exists only on this node holds a delta that is the
    whole table and few or no candidates (looked up). The node knows the
    pending count per table and branch key in memory, summed over the
    read's branch keys (pending rows on another branch are not read whole),
    and counts the candidates from origin's index keys under the prefix
    (an upper bound: tombstoned rows' entries and rows a further predicate
    drops still count), stopping at the pending count, so choosing never
    reads more than the cheaper mask does.
  - A branch view masks its base by the head's presence read by the base
    candidates' row ids: a keyed lookup of the head's refs
    ([#4005](https://github.com/garden-co/jazz/issues/4005)). A head can be
    as large as its base, so this costs the base candidates, not the head.

  Both are exact whatever the layers' sizes later become, so a live read
  stays correct as the delta grows and shrinks; the choice only moves cost.
  Inside a recursive step every presence is the masking layer's rows read
  whole (see below).

Under an index prefix each content layer of a branch view therefore reads
only that prefix, and the base's content costs its index matches. The
deletion registers are read by row id, not whole: every layer of both
branches is a keyed lookup by the row ids of the content candidates of
either branch. A head row inside the prefix can still be deleted by its base
image anywhere in the base key, and that image has the head row's id. Every
reader of a register reads it only for a content row's id, and stacking is
per row id, so narrowing each layer to the candidates' ids before stacking is
exact (`INV-BVIEW-10`). A lookup stays live: a key entering or leaving the
candidates, and any write to a looked-up row, updates it. A row-id read
bounds every layer by its row-id prefix instead.

A lookup cannot run inside a recursive step: Groove evaluates the step
without storage reads. Only a trusted include-deleted read hands an index
path to a step's source; there every presence and every deletion-register
layer is read whole, as without the lookup. That is exact (ids no candidate
holds hide or delete nothing); a presence costs its masking layer, and a
branch view's deletion registers cost the branches' rows outside the prefix.

**Peers (out of scope).** A peer remote would be one more sparse delta, read
as `[local, peer, origin]`. Merging all peers into one delta keeps the stack
short, as a relay's overlay already does. The model leaves room for it,
because no stored or sent byte encodes "a pending predecessor is the
writer's own node" (`INV-REF-4`): a base names an unsettled commit by its
sender's id (only the sender's own may go by how far back it is), and
pending history is keyed by writer. Peers would need:

- per-transaction signatures, so Core can accept a write forwarded by a node
  that did not author it (SPEC 7 is not an end-to-end signature protocol);
- grant-based sharing of pending rows, since a peer cannot evaluate read
  policy authoritatively;
- relaxing `INV-HIST-20` to accept a foreign pending predecessor;
- push forwarding the predecessors a write contains ahead of it on the
  pusher's Core link, so link order still puts the predecessor first.

[#4006](https://github.com/garden-co/jazz/issues/4006) tracks this design.

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
