# jazz — Specification · 20. Specialised indexes

## Overview

A table can declare **specialised indexes** next to its B-tree indexes: a
**text** index for word search, a **vector** index for nearest-neighbour
search over embeddings, and a **geo** index for points on the Earth. They are
one framework with one lifecycle; each kind contributes storage, maintenance,
a query operator and a score.

Specialised indexes are Jazz's own: ordinary SQLite tables (_shadow tables_)
maintained by Jazz in the same transaction as the rows they index, queried by
ordinary SQL plus, where SQL cannot express a computation, a Rust step whose
result is bound back into that SQL. They never use SQLite's FTS5, R*Tree,
loadable extensions, custom SQL functions or virtual tables. Those are
missing on some targets (FTS5 and R*Tree are compiled out of the browser
WASM; the Turso engine has neither and no `create_function`; Durable Objects
allow FTS5 and R\*Tree but no custom functions or modules), and an engine
feature on some nodes and a fallback on others would match and rank
differently. Every node runs the same code, so the same rows and the same
query give the same matches in the same order everywhere. That is required,
not nice to have: a client re-evaluates every query over the rows it holds
(6.5), and fan-out rechecks changed rows with the query's own SQL (13.13).

Indexes are derived state. They are never synced, never part of a row's
history, and never authorize anything. Each node indexes the rows it holds:
the authority all of them, a client its replica (20.9).

Invariant digest:

- `INV-IX-1`: a specialised index has one implementation, and its matches,
  scores and order are a pure function of the indexed row's current values,
  the query and the index declaration, identical on every node and engine
  (20.6).
- `INV-IX-2`: an index's shadow rows change in the same SQLite transaction as
  the indexed row, whatever wrote the row (local write, sync, rebase,
  eviction, pruning, migration); no read observes a row and its index out of
  step (20.3).
- `INV-IX-3`: a search returns only rows the reader may read. The read policy
  is applied to the final row set exactly as for any query, and nothing about
  unreadable rows reaches the result: not their ids, counts, scores or
  statistics (20.7).
- `INV-IX-4`: every query using an index costs work proportional to the
  shadow rows and rows it touches; a shape that cannot be answered so fails
  with a typed error naming it (20.11).
- `INV-IX-5`: shadow tables store only built-in SQLite values in named,
  versioned encodings; a change to an encoding or analyzer is a new version
  and rebuilds the index, never a silent reinterpretation (20.2).
- `INV-IX-7`: index maintenance is total: no value of an indexed column makes
  draining fail. A value a kind cannot index (null, a zero vector under
  `cosine`, a point out of range) leaves the row unindexed, so it never
  matches; it never fails the write or a synced version (20.3).
- `INV-IX-6`: a live query using an index updates exactly as the same query
  re-run would, rechecking only changed rows (20.8).

## 20.1 Declaration

In `jazz.app.json` (10.10), a table's `indexes` list keeps its string (one
column) and array (composite) forms and accepts an object form for
specialised indexes. As with relations and columns, the object names its
kind by key: it has `name` (unique within the table, `[a-z][a-z0-9_]*`, at
most 32 bytes) and exactly one of `text`, `vector` or `geo`; any other
combination is refused. New keys are additive in format 1, and readers
refuse unknown keys, so an older Jazz refuses a file using them with a
named error.

```json
"indexes": [
  "owner",
  ["project", "$createdAt"],
  { "name": "search",
    "text": { "title": 4, "body": { "weight": 1, "length": 256 } },
    "analyzer": "jazz-text/1:en", "prefix": true },
  { "name": "similar", "vector": "embedding", "metric": "cosine" },
  { "name": "place", "geo": { "lat": "lat", "lng": "lng" } }
]
```

| kind key | value                                                                  | other keys                                                                                                    |
| -------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `text`   | string column → weight 1–15, or `{ "weight": 1–15, "length": 1–4096 }` | `analyzer` (20.10.1, required), `prefix` (default `false`), `serverOnly`                                      |
| `vector` | a `vector` column                                                      | `metric`: `cosine`, `l2` or `dot` (required); `ann`: `exact` (default, only value accepted now); `serverOnly` |
| `geo`    | `{ "lat": <f64 column>, "lng": <f64 column> }` (degrees, WGS84)        | `serverOnly`                                                                                                  |

A text field's **weight** multiplies its terms' impacts, and its **length**
is the reference length in terms that the impact formula compares the
field's actual length with (20.10.1). The weight-only form has length 32,
which suits titles, names and other short strings. For long text
(descriptions, bodies, documents), declare a length of about 256; with 32, a
word appearing once in a body of a few hundred terms scores almost nothing.
Both are baked into each posting's impact when a row is indexed, so changing
a field's weight or length, like any change to the declaration, changes the
declaration's canonical bytes and rebuilds the index (20.2).

The artifact is the only place indexes are declared; generated bindings
for each language read them from it. The JSON Schema in
`docs/public/schemas/` lists the same keys as the reader.

Validation, at schema construction, with a typed error naming the table,
index and option:

- `"serverOnly": true`: "serverOnly indexes are not supported yet" (the key
  is read so that this error, not an unknown-key one, names it; see 20.9);

- no kind key or more than one, an unknown key, or a value outside the table above
  ("`ann: \"ivf\"` is not supported yet");
- an indexed column that is missing, of the wrong type, or encrypted (E2EE
  columns hold ciphertext; indexing them is not supported);
- a `text` field whose column has a text-merge strategy and a value over the
  text-merge inline threshold is accepted (see 20.12 for its cost);
- two specialised indexes with the same name.

A new column type, written `{ "vector": n }` (`1 ≤ n ≤ 4096`) with the usual
column keys beside it, holds `n` IEEE-754 binary32 values. Generated
bindings map it to `Float32Array` in TypeScript and `Vec<f32>` in Rust. A
column changing to or from `vector` is refused by the migration diff (no lens
converts it). It is a `ValueType` appended last (`INV-STORAGE` rules on variant
order apply), stored in its cell as a BLOB of `4n` bytes, little-endian,
encoding `f32le/1`, and carried on the wire as the same bytes. A write whose
value has the wrong length or holds a NaN or infinity is refused at
admission. Jazz never computes embeddings: they are ordinary values the
application or a backend writes.

In the engine a vector value is a byte value (`Value::Bytes`, public
`Value::Bytea`) of exactly `4n` bytes; there is no vector value variant, so
value encodings are unchanged and only types are new. A record field of type
`vector(n)` is variable-width and its payload is the `f32le/1` bytes with no
stored-scalar header (a vector is never a large value, 19); the persisted
record descriptor names it with node 27 (`Vector`, field `dimensions: u32`),
and the binding descriptor with type tag 19 and the dimensions. A vector is
not a tuple member, an array element, a payload-enum field or a B-tree index
column. As a filter operand it compares as a whole value, bit for bit (6.1.1);
nearness is only a search (20.4). The dimensions are part of the type, so
changing them is a type change, refused like any other to or from `vector`.

Specialised indexes are part of a table's version id (a new schema-id
domain, `SchemaHash` only; the ids of schemas without specialised indexes are
unchanged), not of its compatibility id, as composite indexes are; the
migration diff treats them as an index-only change: adding, changing or dropping one
does not change how rows are stored or synced. A store opening a schema
that adds an index builds it (20.4); one that drops an index drops its
shadow tables.

## 20.2 Storage

Each index owns shadow tables named `_jazz_ix_<stored>__<name>_<part>` on
the current table only (never on `__h`), plus one row in `_jazz_ix`. The
`_jazz_` prefix keeps them out of what a store lists as its tables, so a
build of Jazz without specialised indexes opening the file leaves them
alone (its triggers keep queueing rows, which a later build drains):

```sql
CREATE TABLE _jazz_ix (
  tbl TEXT NOT NULL, name TEXT NOT NULL,
  kind TEXT NOT NULL,        -- 'text' | 'vector' | 'geo'
  encoding TEXT NOT NULL,    -- e.g. 'jazz-ix-text/1 jazz-text/1:en'
  decl BLOB NOT NULL,        -- canonical declaration bytes
  state INTEGER NOT NULL,    -- 0 building, 1 ready
  built INTEGER NOT NULL,    -- while building: the highest rid indexed so far
  PRIMARY KEY (tbl, name));
```

`decl` is the declaration's `jazz-ix-decl/1` bytes with each column under
its stored name, so a column rename that keeps the stored column keeps the
index.

`encoding` names every byte-level choice the shadow rows depend on: the
kind's row layout and, for text, the analyzer. A store opening with a
declaration whose canonical bytes or encoding differ from the recorded ones
drops and rebuilds that index. Shadow tables are rowid tables with secondary
indexes (no `WITHOUT ROWID`), so they run on every target, the Turso engine
included.

Shadow rows reference indexed rows by the current table's `rid`. Per kind:

| kind   | tables                                                                                                                                |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| text   | `__terms(tid INTEGER PRIMARY KEY, term TEXT UNIQUE, n INTEGER)`; `__post(tid, rid, impact INTEGER)`, indexed `(tid, rid)` and `(rid)` |
| vector | `__vec(rid INTEGER PRIMARY KEY, v BLOB)` (normalized for `cosine`)                                                                    |
| geo    | `__geo(rid INTEGER PRIMARY KEY, cell INTEGER, lat REAL, lng REAL, x REAL, y REAL, z REAL)`, indexed `(cell)`                          |

`__terms.n` counts postings and exists to drop dead terms; it is never read by
a query (20.7). Fixtures in `SPEC/fixtures/ix/` pin each encoding's bytes
for given inputs.

## 20.3 Maintenance

For each table with a specialised index, Jazz creates three triggers on the
current table, `AFTER INSERT`, `AFTER DELETE` and `AFTER UPDATE OF` the
indexed columns and `_state` (a soft delete or restore changes only
`_state`). They only record the row's `rid` in

```sql
CREATE TABLE _jazz_ix_dirty (tbl TEXT NOT NULL, rid INTEGER NOT NULL, PRIMARY KEY (tbl, rid));
```

with `INSERT OR IGNORE`. Triggers are plain SQL, which every target runs,
and they see every write to the current table, so no write site needs to
know about indexes: local writes, synced versions, rebases, pending-version
removal, eviction to cold copies and migrations all reach them.

Rust **drains** the queue inside the same transaction: before the
transaction commits (as the outermost savepoint is released; a drain that
fails rolls the transaction back), and before any statement that reads a
shadow table. Draining a `rid` reads the row's current indexed values (a
missing or deleted row means delete: a deleted row is never indexed), computes its new shadow rows, and replaces the old ones. For text
the old postings are diffed against the new so an edit touching one word
writes one posting. Large values (19) are read as their logical value. A
replica that holds a row before its large value's chunks indexes the row
without that value and records it in `_jazz_ix_incomplete(tbl, rid)`;
when chunks arrive, those rows are queued again, so the replica's index
ends up equal to the authority's. Draining never fails
because of a value (`INV-IX-7`): a write admitted under the table's own
rules is always indexable, or else unindexed, so the same row can never be
accepted by one node and wedge another's sync.

Building an index (a new declaration, or a rebuilt one) indexes every
current row in `rid` order, in batches of bounded size (512 rows, one per
tick), each its own transaction, with `_jazz_ix.state = 0` and `built`
advancing meanwhile (an index on an empty table is built at once); a query using an index that is still
building waits for the build on a client and is refused with a typed retryable
error on the authority.

## 20.4 Queries and the wire

A search is one new predicate, appended last to `Predicate` so every
existing predicate keeps its encoding:

```rust
pub enum Predicate { /* … existing variants … */ Search(IndexSearch) }

pub struct IndexSearch { pub index: String, pub op: IndexOp }

pub enum IndexOp {
    /// Rows whose analyzed fields contain any or all query terms.
    Match { text: String, mode: MatchMode },
    /// The k rows nearest to a vector, by the index metric.
    Nearest { vector: Vec<f32>, k: u32 },
    /// Rows within a radius of a point.
    WithinRadius { lat: f64, lng: f64, meters: f64 },
    /// Rows inside a lat/lng rectangle (may cross the antimeridian).
    WithinBox { south: f64, west: f64, north: f64, east: f64 },
}

/// How many of a text search's terms a row needs; the prefix term counts as one.
pub enum MatchMode { #[default] Any, All }
```

A text search's `mode` is per query, `Any` by default: a row matches when it
holds at least one query term (`Any`) or every one (`All`). Either way it
ranks by the summed impacts of the terms it holds (20.6), so under `Any` a
row holding more of the terms ranks higher. `MatchMode` encodes as one byte
after the text (`Any` 0, `All` 1); other values are refused on decode.

`Predicate::Search` is valid only as a top-level element of a query's
`filters`, at most once per query, and never in a policy, a join, an include
or under `Any`/`Not`. It combines by AND with the other filters, joins,
includes and the read policy, and composes with `order_by`, `limit` and
`offset` like any filter. The reserved order column `$score` (like
`$createdAt`) orders by the search's score (20.6), best first, and is valid
only with a search; without an explicit `order_by`, a query with a search
orders by `$score`. Ties are broken by `id`, as for every order. `Query` and
`OrderBy` keep their shapes.

**Wire.** `Query` travels in `SyncMessage`, so a peer that does not know
`Predicate::Search` cannot decode a query using it. Support is the wire
feature `FEATURE_INDEX_SEARCH` (the next free `WireFeatures` bit), not a
protocol version: the wire version stays 6 and queries without a search are
byte-identical. A node sends a query with a search upstream only if the
upstream offered the feature; otherwise that query is answered from the
replica alone and, at `Remote` tier, refused with "the server does not
support index search". A transport reports what its link negotiated; one
that cannot say (an in-process link aside) offers no optional feature. The
new predicate's JSON and postcard bytes are pinned per op.

Validation refuses, with typed errors: an unknown index; an op not of the
index's kind; an empty text after analysis; a vector of the wrong
dimension or with non-finite values; `k` outside 1–1000; coordinates out of
range; a radius over 1000 km; `search` on a history read, an as-of cut or a
branch view with a base (not supported yet).

## 20.5 Compilation

A query with a `search` clause still compiles to one statement over the
current table. The kind supplies a candidate relation `ix(rid, score)` as a
common table expression; the root is `t JOIN ix USING (rid)`, and every other
clause, the read policy included, applies to `t` unchanged.

**Text** is pure SQL. Rust analyzes the query text into terms (20.10.1),
looks up their `tid`s and binds them as a JSON array. With `prefix`, the
last word is one slot matching its whole-word term or every term in its
range (`term = ?exact OR (term >= ?lo AND term < ?hi)`, 20.10.1).

```sql
WITH q(tid, slot) AS (SELECT ... FROM json_each(?)),
ix(rid, score) AS (
  SELECT p.rid, sum(p.impact) FROM q JOIN post p ON p.tid = q.tid
  GROUP BY p.rid HAVING count(DISTINCT q.slot) >= ?needed)
```

`?needed` is 1 under `Any` and the number of slots under `All`; nothing else
differs between the modes.

**Geo** is pure SQL. Rust covers the query region with at most 16 ranges of
cells (20.10.3) and computes a chord threshold; SQL reads those ranges and
keeps rows within the exact threshold:

```sql
ix(rid, score) AS (
  SELECT rid, (x-?qx)*(x-?qx)+(y-?qy)*(y-?qy)+(z-?qz)*(z-?qz) AS d
  FROM geo WHERE (cell BETWEEN ?c0 AND ?c1 OR …) AND d <= ?d2)
```

**Vector** needs a distance SQL cannot compute on every target, so it runs
in two phases:

1. **Candidates.** On every node the candidates are the rows the query's
   other clauses and read policy match (_filter-first_): SQL returns their
   `(rid, id, v)` (the same statement without the `search` clause, joined to
   `__vec`, ordered by `rid`, capped at `VECTOR_EXACT_MAX` = 50 000 + 1 rows;
   exceeding the cap is a typed error). Rust scores them (20.10.2) and keeps
   the best `k` by `(score, id)`, `id` being the row's object id (a `rid` is
   local to a node). Scoring the whole replica and filtering afterwards
   would keep fewer than `k` rows whenever the other clauses drop some of
   the nearest, and so differ from the authority. On the authority the
   clauses must also narrow the table, as decided from the statement's
   `EXPLAIN QUERY PLAN`: its root table must be read by
   `SEARCH … USING INDEX`, not `SCAN`.
2. **Rows.** The `k` pairs `(rid, score key)` are bound as one JSON array,
   and the statement runs with `ix` as `json_each` over it, so filters,
   policy, includes and order apply as above. The score key is the score's
   order-preserving `i64` (the `f64` bits, negatives' magnitude flipped), so
   SQL compares it exactly.

On the authority, a vector search whose other clauses do not narrow the
table through an index is refused: "unfiltered vector search on the server
needs an ANN index (`ann: \"ivf\"`), not supported yet". This keeps
`INV-IX-4` until an ANN kind exists.

## 20.6 Scores and determinism

Every score is a pure function of one row's indexed values and the query
(`INV-IX-1`). No score reads corpus statistics (document counts, average
lengths, term frequencies across rows), so a client and the authority, and
two clients holding different subsets, rank the rows they share identically,
and a change to one row cannot move another.

Scores are computed so that every node gets the same bits:

In `ix(rid, score)` every kind's score is smaller-better, so `$score`
ascending is best first: text binds the negated sum.

- **text:** an integer. Each posting's `impact` (0–255) is computed in Rust
  at index time from the term's frequency in the field, the field's length in
  terms and the field weight (20.10.1); the query score is the integer sum of
  the impacts of the matched postings, which SQL adds exactly in any order.
  Larger is better.
- **geo:** the squared chord distance between unit vectors, `x, y, z` computed
  by Rust at index time and the query point at query time, combined in SQL
  by one expression of `+`, `-` and `*` in a fixed order (IEEE-754 binary64,
  correctly rounded on every target). Smaller is better. Chord distance is
  monotonic in great-circle distance.
- **vector:** computed in Rust in index order with f32 products accumulated
  in f64 sequentially, never fused (no FMA) and never reassociated (SIMD
  implementations must reproduce the sequential result exactly or not be
  used). `cosine` and `dot` are larger-better similarities stored negated
  so that every vector score is smaller-better; `l2` is the squared distance.

Score values are not part of query results in this version; they only order
rows.

## 20.7 Authorization

The read policy applies to the final statement exactly as for any query (7.3),
so a search cannot return a row its reader may not read (`INV-IX-3`). Beyond
rows, nothing about unreadable rows may reach a reader:

- No score reads corpus statistics (20.6), so ranking does not depend on
  hidden rows.
- `__terms.n` and every other count are never read by queries.
- Vector candidates on the authority are chosen by the same filters and
  policy as the final rows (filter-first), and the policy is applied again in
  phase 2.
- Typed errors name the query's shape, never whether rows exist.
- An empty result and a result of hidden rows are indistinguishable.

A client indexes only rows the server let it hold, and eviction removes their
shadow rows in the same transaction (20.3).

## 20.8 Live queries

A subscription with a `search` clause watches its table (`Watch::Table`),
narrowed by any key its other filters or policy pin, as today. Text and geo
queries are recheckable under the existing rules (13.13): their CTE reads
only the changed row's shadow rows, so re-running the statement narrowed to
`id IN (changed)` gives each changed row's exact new score, and the row is
spliced into the window by `(score, id)`. Because scores depend on the row
alone, unchanged rows keep their scores and positions (`INV-IX-6`).

A live search's sort keys read each row's score through the search's own
candidate relation. A vector subscription re-runs whole instead: `Nearest`
keeps only its `k` best, so a changed row can push out a row the recheck
never read. A full page whose last row is removed refetches, as today.

## 20.9 Replicas and tiers

Every node that holds a table maintains its indexes over the rows it holds.
A query using an index runs at its requested tier:

- `LocalFirst` searches the replica at once and the authority in parallel;
  rows the authority sends are indexed on arrival and appear in the local
  result by their score, which equals the authority's.
- `Remote` searches settled rows on the authority only.

**Server-only indexes** are not supported yet: a declaration with
`"serverOnly": true` is refused (20.1). A client re-evaluates every query
over the rows it holds (6.5), and a search through an index it does not
maintain is a result it cannot recompute, like a count over rows it does not
hold. Such results get a design of their own, covering counts first and
searches second; this chapter promises no behaviour for them until it
lands.

A local result covers only the rows the replica holds: it is complete for
data the client's subscriptions cover, and otherwise grows as the authority's
answer arrives.

## 20.10 Kinds

### 20.10.1 Text

An **analyzer** turns a field's string into terms. `jazz-text/1:<lang>`,
`lang` one of `en`, `simple`, is defined by its steps and by tables fixed in
Jazz at Unicode 16.0.0, never the toolchain's or the platform's Unicode
version:

1. Split into words with `jazz-text`'s plain-text unit segmentation
   (`plain_units`). Keep the word runs, and the units that are one
   standalone letter or digit: an ideograph, a kana or a Thai, Lao, Myanmar
   or Khmer letter, which `jazz-text` cuts one per unit, so these scripts are
   searched by character. Punctuation, symbols, emoji and spaces are dropped.
   Two word runs with a single `'` or `’` between them are one word (`don't`,
   `O’Neill`). Other separators split, so `e-mail`, `3.14` and `snake_case`
   are two words each.
2. Lowercase with Unicode simple case folding (CaseFolding.txt, statuses C
   and S). `ẞ` folds to `ß`, every sigma to `σ`, and `İ`, which has no simple
   folding, is left to step 3.
3. Fold with the table `jazz-text/fold/1`. A Latin, Greek or Cyrillic letter
   becomes its canonical decomposition without combining diacritical marks
   (U+0300–U+036F), case-folded (`é` → `e`, `ά` → `α`, `ё` → `е`, `й` → `и`,
   `İ` → `i`). A few letters fold by hand: `ß` → `ss`, `æ` → `ae`, `œ` →
   `oe`, `ĳ` → `ij`, `ø` → `o`, `ł` → `l`, `đ` → `d`, `ħ` → `h`, `ŀ` → `l`,
   `ı` → `i`, `ŧ` → `t`, `ƀ` → `b`. The Latin ligatures (U+FB00–U+FB06) and
   the fullwidth Latin letters and digits fold to ASCII. Combining marks
   (U+0300–U+036F, U+1AB0–U+1AFF, U+1DC0–U+1DFF, U+20D0–U+20FF,
   U+FE20–U+FE2F) and variation selectors are dropped, so precomposed and
   decomposed spellings meet. `’` becomes `'`. Other characters are
   unchanged.
4. For `en`, stem with the Snowball English (Porter 2) algorithm of Snowball
   v3.2.0 (`algorithms/english.sbl`), which also strips possessives (`'s`,
   `s'`).
5. Drop terms longer than 64 bytes of UTF-8.

There are no stop words: dropping them breaks queries made only of them.
Under `All` a common word costs nothing in what matches; under `Any` it
adds the rows holding it, ranked low by their impacts.

Query text is analyzed the same way; a query matches a row when every term
(`All`) or at least one term (`Any`, the default, 20.4) occurs in at least
one field. Repeated query terms count once. With
`prefix`, the last query word is not a term but a prefix slot: it matches an
indexed term equal to the word analyzed as a whole word (so a fully typed
`running` meets the stem `run`), or any indexed term starting with the word
folded but unstemmed (steps 1–3), compared as UTF-8 bytes. A partly typed
word whose stem is shorter than what has been typed (`runn`) matches only
once completed. A query with no terms and no prefix after analysis is
refused (20.4).

The impact of a term in a field with declared weight `w` and reference
length `R` (20.1: 1–4096, default 32), frequency `f` and actual length `L`
terms (after step 5) is

```text
impact = min(255, floor(w * 16 * (f * 2.2) / (f + 1.2 * (0.25 + 0.75 * L / R))))
```

computed in binary64 in that order, with `w`, `R`, `f` and `L` exact
integers; per row a term's impact is the sum over its fields, capped at 255.
It is BM25's term-frequency part with a declared reference length per field
in place of the corpus's average length, and no corpus term weight. A term
occurring once in a field as long as its reference length has impact
`16 * w` (239 at `w = 15`, by rounding). One occurring once in a field of more than about `37 * R` terms has
impact 0 at weight 1: it still matches, but adds nothing to the score.

The fixtures in `SPEC/fixtures/ix/text/` pin the terms, postings (with
default and declared lengths) and query terms of both analyzers, and the
impact formula over a grid of `w`, `R`, `f` and `L`, byte for byte.

### 20.10.2 Vector

`__vec.v` holds the column's `f32le/1` bytes; for `cosine`, each vector
divided by its L2 norm in f64 and rounded to f32. A row whose vector is null,
or zero under `cosine`, is not indexed and never matches. Scores:

| metric   | score (smaller is better)          |
| -------- | ---------------------------------- |
| `l2`     | `Σ (aᵢ − bᵢ)²`                     |
| `cosine` | `−Σ aᵢ bᵢ` over normalized vectors |
| `dot`    | `−Σ aᵢ bᵢ`                         |

`Nearest { k }` keeps the `k` best candidates by `(score, id)`. Ann kinds
(`ivf`, then graph-based) are later versions of this kind; they choose
candidates but never change scores.

### 20.10.3 Geo

A point is `(lat, lng)` from the declared columns, in degrees; a row with
either null, non-finite or out of range is not indexed and never matches. `x, y, z` is the point on the unit sphere.
Trigonometry is fdlibm's `sin`/`cos` kernels written out in basic IEEE-754
binary64 operations, with exact argument reduction in degrees, never the
platform's `sin`/`cos` (which may differ in the last bit between targets).

`cell` (encoding `jazz-ix-geo-z62/1`) interleaves
`u = floor((lng + 180) / 360 · 2³¹)` and `v = floor((lat + 90) / 180 · 2³¹)`,
each clamped to `2³¹ − 1`: `u` in the even bits, `v` in the odd bits.

**Cover.** A region is covered by aligned quadtree blocks: snap to the finest
level at which the region spans at most 64 blocks per side, keep the blocks
that meet the region (for a radius, those that meet its spherical cap; a box
is split at the antimeridian, and a radius whose cap reaches a pole widens to
all longitudes), sort their cell ranges, and merge the smallest gaps until at
most 16 ranges remain. The cover only chooses which cells are read; it is
always a superset of the region.

**Exact test and score.**

- `WithinRadius`: rows with `chord2 ≤ (2 sin(meters / (2R)))²`, `R = 6 371 008.8 m`,
  the threshold computed in Rust; score `chord2` (20.6).
- `WithinBox`: rows with `lat BETWEEN south AND north` and
  `lng BETWEEN west AND east` (`lng >= west OR lng <= east` when the box
  crosses the antimeridian), on the stored degrees; score the constant 0, so
  `$score` order is `id` order.

Measured read amplification (cells read ÷ cells in a disc, 16 ranges) with
the cap-shaped cover is about 1.25–1.33× from 100 m to 50 km and from
latitude 0 to 70; a bounding-box cover reads 1.56–1.63×, and a Hilbert curve
over the bounding box 1.42–1.45×.

## 20.11 Errors

All are typed, name the table and index, and carry no data about rows:

- declaration errors (20.1);
- query validation errors (20.4);
- `IndexServerOnly` (20.9): reserved; unreachable while server-only
  indexes are refused at declaration;
- `IndexSearchUnsupportedUpstream`: a `Remote` search to an upstream without
  `FEATURE_INDEX_SEARCH` (20.4);
- `IndexBuilding` (retryable, authority only) (20.3);
- `VectorSearchUnbounded`: unfiltered vector search on the authority (20.5);
- `VectorCandidatesOverCap`: more than `VECTOR_EXACT_MAX` candidates (20.5).

The last five are `Reason`s (`index_server_only`,
`index_search_unsupported_upstream`, `index_building`,
`vector_search_unbounded`, `vector_candidates_over_cap`) on codes `query`,
except `index_building`, which is `backpressure` (retry later).

## 20.12 Costs

- **Write:** one trigger insert per row change, then at drain: text, one
  posting row per distinct term changed; vector, one row; geo, one row. A
  text-merge edit re-analyzes its field (bounded by the value size; large
  values make each keystroke cost their length).
- **Storage:** text roughly 1.5 times the analyzed text (about three times
  FTS5's compressed postings); vector `4n` bytes per row; geo about 40 bytes
  per row.
- **Query:** text, the postings of the query terms; geo, the cells of the
  cover; vector, the candidates read and scored (at most `VECTOR_EXACT_MAX`).
  Scoring is latency-bound by the sequential `f64` sum (about 1.5 ns per
  element on a 2.9 GHz x86 core, natively and in V8's wasm32 alike), so a
  full `VECTOR_EXACT_MAX` scan of 384 dimensions costs about 30 ms in Rust,
  plus reading the 77 MB of shadow rows (about 60 ms warm natively; more in
  a browser's OPFS-backed SQLite), and 1536 dimensions about four times that.
- **Bundle:** each kind is a cargo feature of `jazz-index`, on by default in
  every build, so each one's size is measured and can be dropped.

## Open questions

- Results a client cannot recompute (counts over rows it does not hold,
  searches through server-only indexes): a design of their own, after which
  20.9 says what a server-only index does.
- An ANN vector kind (`ann: "ivf"`) in shadow tables, so the authority can
  answer unfiltered vector search.
- Corpus-weighted text ranking (BM25 with term weights computed by the
  authority per subscription and returned with the result), if fixed-length
  ranking proves too weak.
- Hybrid search (several clauses fused by reciprocal rank), typo tolerance,
  trigram substring search (answering 19.5's large-value `contains`), and
  returning scores or highlights.
- A typed `point` column (asked for in issue 3991) instead of two `f64`
  columns for geo indexes.
- Searching E2EE columns, which would need client-side encrypted indexes.
