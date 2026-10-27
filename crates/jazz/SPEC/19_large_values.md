# jazz — Specification · 19. Large values

## Overview

`string`, `json` and `bytes` columns hold values of any size through one API.
A value up to `INLINE_VALUE_MAX_BYTES` (64 KiB) is stored in its cell as
ordinary SQLite TEXT or BLOB. A larger value is a content-defined chunk tree
plus an **epoch**: a growing list of edits on top of that tree. A row version's
cell names an epoch and how many of its edits apply. That prefix never changes,
so the version's value is immutable while the epoch grows.

Writes to a row are linearized by its authority (serializable transactions
trivially, mergeable ones through their merge strategy), so most edits extend
the newest version. One edit then costs its few encoded bytes in the epoch
plus a few-byte cell in the version. A write that does not extend the newest
version starts a new epoch. Every store keeps its own epochs; sync carries
values as edits against a version the receiver holds, or as a tree.

Everything about a large value is authorized through the row that holds it:
its edits travel inside the row's commits, and its nodes are served only to
readers of a version whose tree contains them.

Invariant digest:

- `INV-CONTENT-1`: large values have one implementation, and a chunked value
  reads, filters, orders and indexes as its logical value.
- `INV-CONTENT-2`: a version's cell is immutable: `(epoch, n)` always names the
  same logical value, however the epoch grows.
- `INV-CONTENT-3`: nodes and edits are disclosed only through a readable row
  version that reaches them; knowing a hash or holding a cached node grants
  nothing, and the server never reveals whether it holds a node.
- `INV-CONTENT-4`: staging nodes creates no readable state and authorizes no
  write.
- `INV-CONTENT-5`: only an ordinarily authorized write of the owning row adds
  edits to an epoch or publishes a new root, and the authority validates both.
- `INV-CONTENT-6`: tree nodes are content-addressed and policy blind; equal
  content in one store is stored once.
- `INV-CONTENT-7`: sync carries large values as edits against a base version
  the receiver holds, or as a tree reference; epochs never cross the wire.
- `INV-CONTENT-8`: collection never removes an epoch or node that a retained
  version, staging lease or receipt reaches.
- `INV-CONTENT-9`: a chunk store holds only leaf bytes; metadata commits with
  the rows, and bytes move or go only after the commit that allows it.
- `INV-CONTENT-10`: `delta(a, b)` returns edits that turn a's value into b's,
  at a cost that follows the change, not the value.

## 19.1 Cells

A large-capable column is declared without a type, like every stored column,
so each cell keeps its SQLite storage class:

| Value | Cell |
| --- | --- |
| inline string or JSON (≤ 64 KiB) | TEXT, its UTF-8 bytes |
| inline bytes (≤ 64 KiB) | BLOB, its bytes |
| chunked (> 64 KiB) | INTEGER `epoch * 4096 + n` |
| null | NULL |

`typeof(cell) = 'integer'` is the chunked arm. `cell / 4096` is the epoch id
and `cell % 4096` is `n`, the number of the epoch's edits that apply, so
triggers and queries read both in plain SQL. There are no tag bytes, every
TEXT cell is valid UTF-8, and no cell carries a hash, root or descriptor.

The inline/chunked choice is canonical: a value is chunked exactly when its
logical length exceeds `INLINE_VALUE_MAX_BYTES`. So an inline cell never equals
a chunked one, and a write that brings a chunked value back under the limit
stores it inline.

A column with a declared index additionally has a hidden sibling column
`<column>__k`. It holds the value's sort prefix (its first `SORT_PREFIX_BYTES`
= 256 logical bytes, cut on a code point for text) for chunked cells and NULL
otherwise. The column's indexes are on `COALESCE(<column>__k, <column>)`.
Unindexed columns have no sibling.

## 19.2 Trees and nodes

A tree's leaves are content-defined chunks of the logical bytes, with the
frozen V1 FastCDC profile (`V1_FASTCDC_LEAF_*`: 16 KiB minimum, 64 KiB target,
256 KiB maximum). Branches group children by the same content-defined rule.
Chunking depends only on content, so two stores that build a tree over the
same bytes build the same nodes, and an edit changes only the leaves around
it and the path above them.

A node is identified by its hash: BLAKE3 over its canonical encoding, in a
domain fixed by the format version and the column kind (bytes, string, JSON).
A JSON tree therefore never reads as a string tree. A leaf encodes its bytes;
a branch encodes, per child, the child's hash, byte length and (for strings)
UTF-16 length. A node has no locator and no second identity.

Nodes live in `_jazz_lv_node (hash PRIMARY KEY, bytes)`, edges in
`_jazz_lv_edge (child, parent)`, and the roots epochs hold in
`_jazz_lv_root (root PRIMARY KEY, refs)`. Every node read is verified against
the hash its parent or epoch names.

`INV-CONTENT-6`: nodes are immutable, content-addressed and carry no schema,
row or policy. Storing a node that exists is a no-op.

## 19.3 Epochs and the edit format

```text
_jazz_lv_epoch (
  id         INTEGER PRIMARY KEY,   -- local to this store, never reused
  root       BLOB NOT NULL,         -- the tree the edits apply to
  len        INTEGER NOT NULL,      -- the tree's byte length
  utf16_len  INTEGER,               -- the tree's UTF-16 length (strings)
  edits      BLOB NOT NULL,         -- encoded edits, appended in order
  count      INTEGER NOT NULL,      -- how many edits `edits` holds
  refs       INTEGER NOT NULL       -- cells referencing this epoch
)
```

The value of `(epoch, n)` is the tree at `root` with the first `n` edits
applied in order. `n = 0` is the tree itself. Appending an edit increments
`count`; nothing else in a version's value can change.

`INV-CONTENT-2`: an epoch's first `count` edits are never rewritten while any
cell references a prefix that includes them. Appending is the only way an
epoch grows, and truncation (§19.7) removes only edits no cell references.

### Edit encoding

Each edit is one tag byte and a few fields. Varints are unsigned LEB128;
zigzag varints encode a signed delta.

```text
tag = kind << 4 | small
  kind:  0 append, 1 insert, 2 delete, 3 replace
  small: 1..15  the inserted length (append, insert, replace) or the deleted
                length (delete), inline
         0      that length follows as a varint

append   tag [len] bytes
insert   tag offset [utf16_offset] [len] bytes
delete   tag offset [utf16_offset] [len] [utf16_len]
replace  tag offset [utf16_offset] delete [utf16_delete] [len] bytes
```

- `offset` is a zigzag varint relative to the end of the previous edit in the
  epoch (the offset just after its inserted bytes; 0 before the first edit).
  Ordinary typing therefore spends one byte on it.
- Coordinates address the value as the previous edits left it.
- An append's offset is the current logical length, known while replaying.
- String columns carry the UTF-16 offset (also relative) and the UTF-16 length
  of every deletion, which cannot be derived without reading the deleted text.
  An insertion's UTF-16 length is derived from its bytes. Bytes and JSON
  columns carry byte coordinates only.
- No lengths are stored per edit: a version's length is the tree's length plus
  the sum of `inserted − deleted` over its prefix.

A one-character append is 2 bytes; a one-character insertion is 3 (bytes) or 4
(string).

### Epoch limits

An epoch's `edits` stays within `EPOCH_EDIT_BYTES` = 3,584 bytes and `count`
within 4,095, so its row fits in one SQLite page and an append dirties only
that page. An edit that would cross either limit, or that inserts more than
`EPOCH_INSERT_BYTES` = 256 bytes, materializes (§19.4).

## 19.4 Writing

A write of a large-capable column produces the new value's cell:

1. **Inline result** (≤ 64 KiB): the cell is the TEXT or BLOB.
2. **Edits on the newest version.** When the write's base is `(E, n)` with
   `n = E.count`, and its edits fit the epoch limits, the edits are appended to
   E and the cell is `(E, n + k)`.
3. **Edits on an older version** (`n < E.count`: a concurrent plain write that
   wins, a branch's first divergent edit, a restore): a new epoch E′ copies
   E's root and E's first `n` edits, appends the new edits, and the cell is
   `(E′, n + k)`. The copy is at most one epoch's edit bytes; epochs never
   point at each other.
4. **Materialization.** When the edits do not fit, or the value is new, the
   writer builds the resulting value's tree, reusing every unchanged node:
   only the regions the epoch's and the new edits touched are chunked again.
   A new epoch with that root and no edits holds it, and the cell is
   `(E″, 0)`.

Any store may materialize; trees are deterministic, so stores that
materialize the same value agree on its nodes. Materialization is local and
never synced.

A new value, or one edit that inserts more than `INLINE_VALUE_MAX_BYTES`, is
prepared into nodes before the write (streaming for `insert_streaming_value`
and the binding upload APIs). Those nodes are held by an upload lease
(`_jazz_lv_upload`) or, when pushed by a client, by a receipt
(`_jazz_lv_receipt`) until the commit referencing them lands or the staging
maximum age passes.

`INV-CONTENT-4`: staged nodes are unreachable from any row; staging needs no
write permission and grants none, and an expired lease or receipt releases
its nodes.

### Authority admission

The authority applies a write's large-value edits only after ordinary
row-write authorization, in the same SQLite transaction as the version. It
validates each edit against the base value: coordinates in range, byte
offsets on code-point boundaries and inserted bytes valid UTF-8 for strings,
and UTF-16 coordinates consistent with the byte coordinates (found through
the tree's UTF-16 metrics and one leaf, not by scanning). A tree reference is
admitted only when its root and every node beneath it are present and
verified, and its lengths match.

JSON validity is a writer's responsibility: anyone allowed to write a value is
trusted not to produce malformed JSON, and Jazz's JSON mutation APIs and JSON
merge strategies produce only valid JSON. The authority does not reparse JSON.
Readers treat malformed JSON as a typed error, never a panic.

`INV-CONTENT-5`: an epoch grows, and a root is published, only inside an
ordinarily authorized write of the owning row, after admission validated the
edits or the tree. A caller cannot hand in a cell, epoch or root.

## 19.5 Reads and queries

A chunked value is read by replaying its prefix over the parts of its tree the
read needs. Partial reads (byte, UTF-8 and UTF-16 ranges, JSON pointers) map
their range through the edits and read only the leaves it covers.

| Operation | How it runs |
| --- | --- |
| `=` against an inline-size literal | SQL: an inline cell compares directly; a chunked cell never matches |
| `=`, `IN`, DISTINCT, GROUP BY between chunked values | equal cells are equal values; unequal cells are compared by length, then content, in the Rust prep |
| `=` against a literal over 64 KiB | the Rust prep |
| length | the Rust prep: tree length plus the prefix's net edits |
| ORDER BY and ranges, indexed column | the index on `COALESCE(c__k, c)`; the Rust prep orders values sharing a prefix |
| ORDER BY and ranges, unindexed column | the Rust prep ranks the column's chunked values |
| `contains`, LIKE, JSON pointer | the Rust prep reads the content |

The prep answers before the statement runs and hands its answers to the
statement as parameters, so every query, trigger and schema object uses only
SQLite built-ins and runs on any SQLite, Durable Objects included.

`INV-CONTENT-1`: every read, comparison, edit and sync path goes through the
one implementation in `jazz-db`, and its answers equal those of the
materialized logical value.

## 19.6 Deltas

`delta(a, b)` takes two cells of one column and returns edits, in the edit
encoding, that turn a's value into b's:

| Case | Computation |
| --- | --- |
| same epoch, `n_a ≤ n_b` | b's edits after a's prefix: a byte range of the epoch |
| same epoch, `n_a > n_b`, or epochs over the same root | the common edit prefix (a byte comparison), then the inverse of a's remaining edits and b's remaining edits; inverting a deletion reads the deleted bytes from the base |
| different roots | each side as pieces (leaf spans and inserted bytes); both trees are walked together, subtrees with equal hashes are skipped, and only differing leaves are compared byte by byte |
| either side inline | one replace of the whole value |

The result need not be minimal when content moves. Reading it needs only the
leaves in changed regions. Sync (§19.8), merge strategies, and the public
delta API (§19.11) are built on it.

`INV-CONTENT-10`: applying `delta(a, b)` to a's value yields b's value, and
its cost is bounded by the changed regions plus the tree depth.

## 19.7 Local replicas

A client or relay keeps its own epochs. Its optimistic writes append to them
like any write. When the authority's outcome for a pending write arrives:

- **accepted unchanged**: nothing changes;
- **rejected, or accepted as a different value**: the replica truncates the
  epoch back to its last accepted prefix (setting `count` and cutting
  `edits`) and applies the authority's versions on top as received.

Cascading rejection (a write built on a rejected write is rejected) ensures no
retained cell references a truncated edit. A replica never rebases.

## 19.8 Sync

A large-capable cell in a row-version message has one of three forms:

| Form | Contents | Used when |
| --- | --- | --- |
| inline | the TEXT or BLOB | the value is inline |
| `Delta` | edits relative to the same column at the message's base version | the receiver holds the base version and the edits insert at most 64 KiB in total |
| `Tree` | root hash, byte length, UTF-16 length, then edits | otherwise |

**Client to authority.** A write's base is its existing base annotation
(`Seq(s)` or `Pending(on)`), so `Delta` adds no field. A `Tree` whose nodes
the authority may lack is preceded by the nodes, pushed under a receipt.

**Authority to subscribers.** The server records the last version of each row
it sent a session, and a resuming session reports the versions it holds. A
new version is sent as `Delta` from that version. For a subscriber at the
newest version this is a byte range of one epoch, computed once per version
and shared by every subscriber. A session holding no version gets `Tree`.

**Receiving.** The receiver resolves the base to its own cell `(E′, n′)`,
then applies §19.4 locally: append when `n′` is its newest, otherwise a new
epoch. A `Tree` whose nodes it lacks is demand: the nodes are pulled.

**Node pulls.** A pull names the row version it reads through (table, row,
branch, version, column) and the node hash. The authority answers only when
the requester may read that version and the node is reachable from that
version's root; otherwise it answers as for an absent node. A relay answers
from its own store when it holds the node and forwards upstream otherwise.

**History.** `__h` versions travel as consecutive `Delta`s, each against the
one before.

**Relays.** A worker is its tabs' authority for bases, deltas and pulls.

`INV-CONTENT-7`: epoch ids, cells and materializations never cross the wire;
a value crosses as inline bytes, as edits against a base the receiver holds,
or as a tree reference whose nodes are pulled through a readable version.

## 19.9 Authorization

- **Writes.** Edits are part of the row commit, so the row's write policy
  covers them. Staged nodes are inert until such a commit references them.
- **Reads.** A reader receives a version's edits only as part of that
  version, and nodes only beneath roots its readable versions reach. A reader
  allowed to see one version but not a later one never receives the later
  version's edits. Losing read access takes effect at once.
- **No existence oracle.** The server never tells a client it already holds a
  node the client cannot read: an unreadable pull is answered as absent, and
  uploads are never skipped on the server's word. Deduplication is silent.

`INV-CONTENT-3`: only a readable row version discloses edits or authorizes a
node; content hashes, cached nodes and other rows' roots authorize nothing.

## 19.10 Chunk storage

Node metadata (hash, edges, root references, receipts, leases, the
reclamation queue) and all epochs live in the store's SQLite file and commit
with the rows. Leaf bytes have one of two homes:

- **The SQLite file** (default): `_jazz_lv_node.bytes`; the file is
  self-contained.
- **A chunk store** (`jazz::chunk_store::ChunkStore`, set with
  `DbConfig::with_chunk_store`): an immutable, policy-blind byte KV keyed by
  node hash, with asynchronous `put`, `get` and `delete` and an optional
  synchronous `get_now`. A directory (`FileChunkStore`) answers at once;
  network object stores (S3, R2 from a Durable Object) answer only
  asynchronously. Puts are idempotent: a hash always names the same bytes.

A new leaf is written to SQLite with its row. A tick moves settled leaves
(held by no lease, receipt or unpushed mark) in three crash-safe steps: mark
the leaf as being put in its own committed transaction; put its bytes; then
drop its bytes from SQLite (an empty blob, which no encoded node is) and clear
the mark, unless it was reclaimed meanwhile. Reclaiming a leaf that is in the
chunk store, or whose put may be in flight, queues its deletion in the
reclamation's transaction, and a tick deletes it only after that commit and
any put the same tick started. Storing the leaf again before the deletion ran
cancels the deletion. Branches and epochs never leave the file.

Every byte a chunk store returns is verified against its hash. A leaf the
chunk store does not hold is data loss and fails the read with a storage
error. A read of a leaf only an asynchronous chunk store holds is demand: the
read fails as missing chunks, the leaf is fetched, and the read runs again.
One-shot reads and writes on `Db` await the fetch; subscriptions refresh after
the tick that fetched; a server holds a session's messages that need such
leaves and serves them in order once they arrive, never refusing a commit for
it. A file that moved leaves refuses to open without its chunk store; a file
first opened with one queues every leaf it holds.

`INV-CONTENT-9`: a chunk store holds only leaf bytes. Metadata commits with
rows. A leaf leaves the file only after its put succeeded, and leaves the
chunk store only after its reclamation committed.

## 19.11 History, retention and collection

Reference counts are kept by triggers:

- every cell in a table or its `__h` history holds its epoch (`refs`);
- every epoch holds its root (`_jazz_lv_root.refs`);
- every branch holds its children through `_jazz_lv_edge`;
- leases and receipts hold their staged nodes.

An epoch whose `refs` reaches 0 is deleted, releasing its root; a root at 0
releases its tree; a node no edge, root, lease or receipt holds is reclaimed.
Reclamation is queued in the transaction that released the last reference and
runs on tick, so a version's value is retained exactly as long as the version.

`INV-CONTENT-8`: collection removes only epochs and nodes unreachable from
every retained version, lease and receipt.

## 19.12 Binding and public API

Schemas declare ordinary string, bytes and JSON columns. Small and large values
use the same full-value API. The TypeScript surface is the typed query and
mutation DSL; it does not expose imperative large-value helpers on `Db` or
`JazzClient`.

A `select` object may request one partial scalar per selected field:

```ts
app.things.where({ id: thingId }).select({
  byteField: { from: 1_000_000, to: 2_000_000 },
  textField: { from: 4, to: 124 },
  textFieldUtf8: { fromUtf8: 4, toUtf8: 67 },
  jsonField: { at: "/someKey/11/otherKey" },
});
```

Ranges are half-open `[from, to)`. Byte ranges apply only to bytes; `{from,to}`
on text is in UTF-16 code units; `{fromUtf8,toUtf8}` opts into UTF-8 bytes.
`{at}` applies only to JSON and is an RFC 6901 JSON Pointer. Results are an
ordinary `Uint8Array`, `string`, or decoded JSON subtree. Selecting a field by
name (or omitting `select`) materializes the complete value. Object-form
partial selections apply to the root query only; in an include builder they
fail explicitly rather than returning the complete value.

`Db.update` accepts ordinary replacements and an `applyDiffs` option. Both are
one atomic mutation, but the same column must not appear in both inputs:

```ts
db.update(
  app.things,
  thingId,
  { name: "renamed" },
  {
    applyDiffs: {
      byteField: { within: bytePage, splices: [{ at: 4, delete: 3, insert: bytes }] },
      textField: { within: textPage, splices: [{ at: 3, delete: 1, insert: "x" }] },
      textFieldUtf8: { within: utf8Page, splices: [{ atUtf8: 3, deleteUtf8: 1, insert: "x" }] },
      jsonField: { edits: [{ op: "set", at: "/someKey/11", value: next }] },
    },
  },
);
```

`within` is the range descriptor used by the read and makes splice coordinates
relative to its start. Splices apply sequentially, so each later coordinate
addresses the result of the previous ones. A deletion stays within the
selected page; insertion at its end is allowed. Text defaults to UTF-16; UTF-8
splice fields opt in. Bounds, integer overflow, UTF-8 boundaries and UTF-16
surrogate splits fail rather than round. JSON edits use RFC 6901 pointer
escaping and fail for a missing path or incompatible schema, kind or
nullability. Splices and JSON edits lower to epoch edits (§19.3). All fields in
one `update`, including `applyDiffs`, commit atomically; there is no page
staleness check.

Transactions accept `applyDiffs` on `tx.update`. Coordinates address the value
the transaction reads, its snapshot overlaid with its own earlier writes, so
repeated diffs in one transaction compose into one version. The row read counts
as a transaction point read, so a serializable transaction conflicts if the row
changes before commit; in a mergeable transaction the diffed column resolves
through its merge strategy, or last-writer-wins. Branch views reject
`applyDiffs` explicitly.

`Db` exposes `value_delta(table, row, column, from_version, to_version)`,
returning `delta` (§19.6) as splices in the column's coordinates, for callers
allowed to read both versions. Subscriptions may opt into receiving a large
string column's changes as splices from the previous result, so an editor can
apply remote changes as transactions.

Native Rust exposes `Db::insert_streaming_value`: the caller supplies the
ordinary row cells, the target column and a `std::io::Read`; Jazz derives the
kind from the column's schema, chunks the stream into a leased upload, and
publishes the row only after EOF, complete text validation, staging-policy
admission and ordinary row-write admission. TypeScript exposes
`Db.insertStreaming`, `Db.updateStreaming` and `Db.upsertStreaming` with one
column replaced by a `ReadableStream` or `AsyncIterable` source; NAPI and
Browser/WASM feed the same leased upload in bounded windows without
collecting the whole value or holding a transaction open. Explicit abort and
terminal validation failure release the lease at once; the staging maximum
age bounds abandoned uploads.

Bindings copy completed results into host-owned buffers. The public API does
not expose epochs, cells, node hashes, tree nodes, leases or retry state.

## Open questions

- Direct leaf downloads from an object store (lower egress, higher bandwidth):
  object names keyed by a server-only secret and revealed only after the read
  check, with short-lived presigned URLs so revealed names expire.
- Store-local leaf patches (a new leaf kept as its base leaf plus edits until
  it is offloaded), as a storage optimization outside the format.
- A lazily filled content-hash table, if exact equality between chunked values
  in plain SQL turns out to matter.
- Retention and history thinning: removal of old versions must be ordinary
  refcounted row deletions, never direct node deletion.
