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
values as edits extending a value the receiver was sent, or as a tree.

Everything about a large value is authorized through the row that holds it:
its edits travel inside the row's commits, and its nodes are served only to
readers of a version whose tree contains them.

Invariant digest:

- `INV-CONTENT-1`: large values have one implementation, and a chunked value
  reads as its logical value. Queries compare it by cell, never by content
  (19.5).
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
- `INV-CONTENT-7`: sync carries large values as edits extending a value that
  crossed the same link before, or as a tree reference; epochs never cross the
  wire.
- `INV-CONTENT-8`: collection never removes an epoch or node that a retained
  version, staging lease or receipt reaches.
- `INV-CONTENT-9`: a chunk store holds only leaf bytes; metadata commits with
  the rows, and bytes move or go only after the commit that allows it.
- `INV-CONTENT-10`: `delta(a, b)` returns edits that turn a's value into b's,
  at a cost that follows the change, not the value.

## 19.1 Cells

A large-capable column is declared without a type, like every stored column,
so each cell keeps its SQLite storage class:

| Value                            | Cell                       |
| -------------------------------- | -------------------------- |
| inline string or JSON (≤ 64 KiB) | TEXT, its UTF-8 bytes      |
| inline bytes (≤ 64 KiB)          | BLOB, its bytes            |
| chunked (> 64 KiB)               | INTEGER `epoch * 4096 + n` |
| null                             | NULL                       |

`typeof(cell) = 'integer'` is the chunked arm. `cell / 4096` is the epoch id
and `cell % 4096` is `n`, the number of the epoch's edits that apply, so
triggers and queries read both in plain SQL. There are no tag bytes, every
TEXT cell is valid UTF-8, and no cell carries a hash, root or descriptor.

The inline/chunked choice is canonical: a value is chunked exactly when its
logical length exceeds `INLINE_VALUE_MAX_BYTES`. So an inline cell never equals
a chunked one, and a write that brings a chunked value back under the limit
stores it inline.

A large-capable column has no other stored column: its indexes are on the
cell itself, and no query reads a chunked value's bytes (19.5).

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
  id         INTEGER PRIMARY KEY,   -- local to this store
  root       BLOB NOT NULL,         -- the tree the edits apply to
  base       BLOB NOT NULL,         -- the tree's descriptor: lengths, kind
  edits      BLOB NOT NULL,         -- encoded edits, appended in order
  count      INTEGER NOT NULL,      -- how many edits `edits` holds
  refs       INTEGER NOT NULL       -- cells referencing this epoch
)
```

The value of `(epoch, n)` is the tree at `root` with the first `n` edits
applied in order. `n = 0` is the tree itself. Appending an edit increments
`count`; nothing else in a version's value can change. An id is local to its
store and is reused only after the epoch was deleted, when no cell names it.

A write interns its value's cell: the epoch over the same tree whose edits
start with the value's edits names it (`(E, n)` for the first `n`); else an
epoch whose edits the value's edits extend is appended to; else a new epoch is
created. Edits are self-delimiting and concatenated, so these are byte-prefix
tests on `edits`.

`INV-CONTENT-2`: an epoch's first `count` edits are never rewritten while any
cell references a prefix that includes them. Appending is the only way an
epoch grows; edits are never removed from an epoch a cell names.

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
- String and JSON columns carry the UTF-16 offset (also relative) and the
  UTF-16 length of every deletion, which cannot be derived without reading the
  deleted text. An insertion's UTF-16 length is derived from its bytes. Bytes
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
verified, its lengths match, and the writer **possesses** it.

A node held by the store proves nothing about the writer, since hashes are
not secret: a writer possesses a tree when each node is either under the tree
of a row version it may read that its commit names (the version a delta
extends, or the settled version its pushes assumed, §19.8), or pushed by the
writer itself under its own receipt with its children possessed in turn. A
tree the writer does not possess is refused exactly as one with absent nodes,
so a write never tells whether the store holds a tree, and never copies
content the writer cannot read. A trusted link's writes are not checked.
Relays check their sessions' writes the same way.

JSON validity is a writer's responsibility: anyone allowed to write a value is
trusted not to produce malformed JSON, and Jazz's JSON mutation APIs and JSON
merge strategies produce only valid JSON. The authority does not reparse JSON.
Readers treat malformed JSON as a typed error, never a panic.

`INV-CONTENT-5`: an epoch grows, and a root is published, only inside an
ordinarily authorized write of the owning row, after admission validated the
edits or the tree and that the writer possesses the tree. A caller cannot hand
in a cell, epoch or root.

## 19.5 Reads and queries

A chunked value is read by replaying its prefix over the parts of its tree the
read needs. Partial reads (byte, UTF-8 and UTF-16 ranges, JSON pointers) map
their range through the edits and read only the leaves it covers.

Queries compare cells, in plain SQL. An inline cell compares as its value,
exactly as any other string or bytes value. A chunked cell (a value over
`INLINE_VALUE_MAX_BYTES`, 64 KiB) is compared by identity, never by content:

| Operation on a value over 64 KiB          | Result                                                                                                            |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `=`, `!=`, `IN` against an inline literal | never equal: an inline literal is under the threshold, a chunked value over it                                    |
| `=`, `!=`, `IN` against a longer literal  | refused when the query compiles: "comparing `t.c` with a literal over 65536 bytes is not supported yet"           |
| `=`, `!=` between two columns, joins      | equal only when both name the same cell                                                                           |
| GROUP BY, DISTINCT                        | one group per cell                                                                                                |
| `<`, `<=`, `>`, `>=`                      | never matches, against a literal or another column                                                                |
| `contains`                                | never matches                                                                                                     |
| ORDER BY                                  | after NULL and before every inline value ascending (reversed descending), among themselves in no meaningful order |

Equal cells always hold equal values (`INV-CONTENT-2`), so cell equality can
under-report equality but never over-report it. Two cells of one value are
the same cell when both are tail-free (`n = 0`): a tree is content-defined,
so the same bytes build the same root, and interning reuses the epoch over
it. A value written with an edit tail, or interned into a different epoch on
another replica, is a different cell; equality between such values depends on
how they were written. Cells are store-local: a client and the server may
hold one value as different cells, so a policy that compares two
large-capable columns can decide a row over 64 KiB differently on each.
Values up to 64 KiB, the common case, have none of these limits.

These are limits, not a design commitment. A column flag that consolidates
every write to a tail-free tree (making equality on that column exact, at the
cost of cheap edits) is the planned escape hatch; content `contains` would
need a write-time index (SQLite FTS5 with the trigram tokenizer runs on every
target, Durable Objects included).

The only Rust-side query work on chunked values is reading them: whole
projections, partial selections (19.12), splices and deltas. Every query,
trigger and schema object uses only SQLite built-ins and runs on any SQLite,
Durable Objects included, and reads never write.

`INV-CONTENT-1`: every read, edit and sync path goes through the one
implementation in `jazz-db`, and every read returns the materialized logical
value.

## 19.6 Deltas

`delta(a, b)` takes two cells of one column and returns edits, in the edit
encoding, that turn a's value into b's:

| Case                                                  | Computation                                                                                                                                                                      |
| ----------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| same epoch, `n_a ≤ n_b`                               | b's edits after a's prefix: a byte range of the epoch                                                                                                                            |
| same epoch, `n_a > n_b`, or epochs over the same root | the common edit prefix (a byte comparison), then the inverse of a's remaining edits and b's remaining edits; inverting a deletion reads the deleted bytes from the base          |
| different roots                                       | each side as pieces (leaf spans and inserted bytes); both trees are walked together, subtrees with equal hashes are skipped, and only differing leaves are compared byte by byte |
| both sides inline                                     | nothing when equal, else one replace between their common prefix and suffix                                                                                                      |
| one side inline                                       | one replace of the whole value                                                                                                                                                   |

The result need not be minimal when content moves. Reading it needs only the
leaves in changed regions. Merge strategies and the public
delta API (§19.12) are built on it.

`INV-CONTENT-10`: applying `delta(a, b)` to a's value yields b's value, and
its cost is bounded by the changed regions plus the tree depth.

## 19.7 Local replicas

A client or relay keeps its own epochs, and its optimistic writes intern like
any write. When the authority rejects a pending write, or accepts it as a
different value, the replica restores the settled version and replays its
other pending writes over it. A replayed edit whose base is no longer its
epoch's newest edit starts a new epoch (§19.4 case 3), at the cost of one
epoch copy. The abandoned edits stay in their epoch until no cell names it.
Epochs are never truncated, so no cell can name an edit that disappeared.

## 19.8 Sync

A large-capable cell crosses the wire in one of three forms:

| Form    | Contents                                                          |
| ------- | ----------------------------------------------------------------- |
| inline  | the TEXT or BLOB                                                  |
| `Tree`  | the descriptor: root, its lengths and kind, then the edits        |
| `Delta` | a base version's seq, its edit count, then the edits extending it |

A `Delta` names a version of the same row (table, id, branch) that the
receiver holds: its value for the same column, extended by the edits, is the
new value. The sender sends one only when the new value's tree is the base's
and its encoded edits start with the base's. Neither end keeps state for this
beyond what sync already keeps, because each direction knows a version the
other holds.

**Authority to client.** A session already watches every row it shipped,
across all its subscriptions and support rows (§8). The watch also records
the seq of the version last shipped this session, or held for an own commit
(`Fate.held`). A new session starts with none, so its first image of each row
goes whole. A row version with chunked cells goes out as
`Delta` against that seq where the stored base value allows, else as `Tree`.
An `Evict` drops the recorded seq.

**Self-healing.** A client may drop a row without telling the authority
(INV-SYNC-14 evictions), so a `Delta` can name a version it no longer holds.
A client that cannot place a `Delta` (no version at that seq, or an edit
count that differs) skips that image and sends `Hold` for the row with seq 0,
saying it holds no version. The authority then ships the row's current image
whole, or an `Evict` if it is no longer readable. Until that arrives the
client holds a stale version of the row, so it stores no resume point and
forgets the ones it had (a resume from them would count the row as held), and
it sends the `Hold` again on its next connection. A `Delta` is never applied
to any version but the one it names.

**Client to authority.** A client's settled versions all came from the
authority, so it bases a commit's chunked values on the newest settled
version of the row it holds: a value extending that version's value travels
as `Delta` beside the commit's ops, anything else as `Tree`. The authority
keeps every version (§19.11), so it holds the base. It resolves the delta only when the writer may read that version
(`readable_version`): otherwise naming a version would copy content the
writer cannot read into its write. A delta it cannot resolve rejects the
commit. The rebuilt descriptor is admitted as any other (§19.4). A `Tree`
travels with the settled version of its row as its source, and is preceded
by every node of its tree not under that version's trees, pushed under the
writer's receipt (§19.4). Nodes the authority holds for others are pushed
anyway: deduplication is silent. A node the client lacks (a value copied
from a row it holds only partly) is fetched first, and the commit waits.

**Relays.** A relay resolves its sessions' deltas against its own store and
re-encodes against what its upstream link knows, per direction, as above. A
worker is its tabs' authority for pulls.

**Lazy, range-shaped pulls.** Receiving a descriptor never pulls nodes by
itself: a node is pulled when a read needs it. A partial read (§19.5) maps its
range through the edits and needs only the nodes on the paths covering it, so
reading one page of a large document pulls the root, one path of branches and
the leaves of that page.

A pull names the row version it reads through (table, row, branch, version),
the root of the tree it reads there, the hashes of the nodes it lacks, and
optionally a range of that tree's bytes. A read maps its logical range through
the edit tail to the smallest base-tree range covering it; the requester holds
everything above the nodes it lacks, so a range pull is answered with those
nodes and every node beneath them on the paths covering the range, parents
first, in one round trip (up to a bounded number of bytes per answer; a read
past them pulls again). The requester accepts a node it asked for or one a
parent it holds names. The authority answers only when the requester may read
that version and the version holds that root, and only with nodes reachable
from it. Anything else is answered as for an absent node. A replica records,
for each root it receives, the version it arrived in, and pulls through it. A
relay answers from its own store when it holds the nodes and forwards upstream
otherwise.

`INV-CONTENT-7`: epoch ids, cells and materializations never cross the wire;
a value crosses as inline bytes, as a tree reference whose nodes are pulled
through a readable version, or as edits extending a version of the same row
that the receiver holds, named by its seq.

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
An update retains every new cell's epoch before it releases any old one, so an
epoch that moves between columns of a row is never freed in between. An epoch
interned for a write that then stored another cell is named by no cell; the
next reclamation deletes it.

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

The engine reads a partial selection only in part: the query carries each
part (`Query.partial`, and `ArraySubquery.partial` for the Rust include API),
and the replica reads, and pulls (§19.8), only the chunks that part's byte
span covers, through every relay on the way. A text range in UTF-16 units
reads at most the bytes `[from, 3 * to)`; a JSON pointer reads the document
only as far as the pointer needs, unless the document's shape requires all
of it. Current limits: a pointer read re-parses the prefix it has read for
each further window, so its cost grows quadratically with how far into the
document the pointer reaches, and only a top-level array index is read in
part (a pointer into a nested array reads the document up to its end). Edits to the value reach the replica as deltas (§19.6) the part reads
through, so a subscribed part follows them without fetching anything more.
Two subscriptions over the same rows in different parts are distinct shapes.
Partial selections in aggregate, union or joined queries fail explicitly. A
column that a computed column or transform also depends on is read whole and
its selection is taken from the whole value.

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
changes before commit; in a mergeable transaction the diffed column is authored
like any patched column: last-writer-wins at commit, or merged with concurrent
edits when the column is text-merging (ch. 4 §4.3, `INV-HIST-24`). Branch views
reject `applyDiffs` explicitly, as do React Native transactions until its
foreground codec carries the descriptor.

`Db` exposes `value_delta(table, row, column, from_version, to_version)`,
returning `delta` (§19.6) as splices for callers allowed to read both
versions: each replaces `delete` bytes at `at` with `insert`, applied in
order, and text splices carry the same edit in UTF-16 units (`at_utf16`,
`delete_utf16`). It reads only the changed regions; a version the replica no
longer holds, or one the reader may not read, fails. Subscriptions opt into
receiving a large-capable column's changes as splices from the row's previous
result (`ReadOpts::splices`), so an editor can apply remote changes as
transactions: each updated row carries the column's splices from the version
the result showed. When they cannot be computed locally (that version is no
longer held, or chunks are still to arrive) the row carries none for the
column, and the reader takes the value whole. A column the update left
unchanged carries an empty list.

On a range-partial column the splices are page-relative: they turn the page
the result showed into the new page, computed from the two page cells, so an
edit before the window that shifts its contents is a splice of the window
too. Splices on a JSON-pointer-partial column are refused explicitly as not
supported yet, and so, in TypeScript, are splices on a range-selected column
that a computed column or transform makes the engine read whole (its rows
show the window, its splices would be over the whole value).

`JazzClient` subscriptions take the same option (`subscribe_with_opts` with
`ReadOpts::with_splices`) and carry the splices on each `OrderedUpdated`.
TypeScript takes `splices: [column, ...]` in the query options of
`Db.subscribe`; the update callback's second argument maps each updated row's
id to its columns' splices, UTF-16 with a string insert for text and bytes
with a `Uint8Array` insert for bytes. TypeScript accepts only TEXT and BYTEA
columns without a column transform and rejects others when subscribing, as
not supported yet.

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
- Exact equality, ranges and `contains` on values over 64 KiB (19.5): a
  consolidating column flag, and a write-time FTS5 trigram index.
- Retention and history thinning: removal of old versions must be ordinary
  refcounted row deletions, never direct node deletion.
