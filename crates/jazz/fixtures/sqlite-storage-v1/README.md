# SQLite storage format 1 corpus

What an alpha.60 user has on disk: SQLite files written by Jazz storage
format 1 (`jazz::format::STORAGE_FORMAT`), gzip-compressed and base64-encoded.
Once alpha.60 ships they are immutable: a change that a format-1 file would
not read bumps `STORAGE_FORMAT` and adds a new corpus beside this one, and
never rewrites these. Until then format 1 is still moving, so such a change
re-runs the producer and re-pins the hashes here and in the test instead
(for example: enum columns stored as tags, per-write timestamps on the wire changed how pending uploads
are stored, a rejection's reason name changed how fates are recorded,
pending uploads moved from one encoded message to a row per op, and
watermarks became per-sequencer rows).

- `server.sqlite.gz.base64`: an authority (WAL, default page size). SHA-256 of
  the SQLite file `5c478e21f7447123bc3c4cf486bc49c6977117683e7084103860d69c847c2f4a`.
- `client.sqlite.gz.base64`: a client replica that synced with it. SHA-256
  `cc0a6e7e98a0efe8189138f40e17fe48cb7bb503472c4f47abd0bc3e2c4e5a26`.

Producer: `produce_sqlite_storage_corpus_v1` in `crates/jazz/tests/storage_corpus.rs`
(ignored; `JAZZ_WRITE_SQLITE_STORAGE_CORPUS=1`). It wrote, with fixed row ids,
nodes and write times:

- `projects`, `documents` (reference, indexed title, a 300 KiB chunked large
  value, bytes, a defaulted column, a defaulted enum stored as tags in
  `_jazz_enum_tags`) and `drafts` (branched by `branch_id`);
- two interned authors, updates (history versions), a soft delete and a row
  on a branch;
- a client text edit to `documents.body` (text-merging) that the server
  merged with its own concurrent edit, so its history keeps the client's
  write as sent (`_state` bit 2, at `_clock` = minus the commit's position);
- a client that synced `projects` and `documents`, wrote one row that
  settled, one stamped far ahead of the server's clock that the server
  rejected (so `_jazz_transactions` keeps a rejected fate with its reason name),
  then two writes (an insert and an update) and a serializable transaction
  that read one project and inserted two, none of which reached the
  server: its pending uploads, the last one stored as two op rows and a
  read row;
- a row of `drafts` on a base branch that the draft branch overrides with a
  version the client may not read: the client's view of the draft over that
  base hides the row, so the client keeps a marker of it in `_jazz_withheld`;
- then the server activated a second schema version that renames
  `documents.title` to `heading` (a stored-names lens) and wrote under it.

The receipts that read it back run by exact name in `dev/gates/storage-compat.sh`.

The greenfield removal of `_jazz_shadow` regenerates this format-1 corpus
in place, without a migration or format bump. Pending writes recover their
upstream baseline from the current row or the newest history version below
the replica's local sequence range. Eviction removes current and historical
versions so reopening cannot restore an evicted baseline.

Dropping the large-value key columns (`$k:<column>`) and `_jazz_lv_scratch`,
when queries stopped reading chunked values' content, regenerated it the same
way.

Replacing the authority's `_jazz_text_in` side table with as-sent history
versions, when `documents.body` became text-merging, regenerated it the same
way.

Keeping the deploy catalogue as typed rows (`_jazz_versions` numbering the
admitted versions, `_jazz_names` keyed by that number, `_jazz_stored_columns`,
and `_jazz_schemas`, `_jazz_lenses` and `_jazz_permissions`) regenerated it
the same way. A development store with the earlier layout (`_jazz_names`
keyed by version id) is refused at open with "delete the local store and
resync".

Moving a commit's bases, access proofs (formerly sources) and deltas onto its
ops, so each pending op is stored with them and `_jazz_pending_items` keeps
only ops and serializable reads, regenerated it the same way.

Replacing a pending commit's `author` and `session` fields with one `acting`
enum (`Own`, `Attributed`, `Session`), which changes the encoded
`_jazz_pending` header, regenerated it the same way. A pending row in the
earlier layout is refused as an unsupported storage format, never misread.

Merging `_jazz_applied` and `_jazz_settled` into one `_jazz_transactions`
table keyed by an interned node code (`_jazz_nodes.n`) regenerated it the
same way.

Deleting a settled commit's pending rows in one statement (the
`_jazz_pending_forget` trigger on `_jazz_pending`, with
`_jazz_pending_versions` now created on open) regenerated it the same way,
adding the serializable pending commit above so the per-op layout is
pinned with more than one op and a read.

Hiding branch-view rows whose head version the reader may not read added the
client's `_jazz_withheld` table (created on the first hidden eviction) and
gave `drafts` an `owner` column and a read policy, which regenerated it the
same way, and re-pinned the browser corpus (whose producer hides a note the
same way). A storage change re-pins this corpus and the browser corpus;
`server-data-dir-v1` is re-pinned only when an authority's bytes change (an
authority never writes `_jazz_withheld`, and that producer is not
byte-deterministic).

Storing every version at a position `(_n, _seq)`, `_n` being the code of the
node that numbered it (0 for the authority; a replica's own writes at its
node's code instead of a reserved local seq range), regenerated it the same
way. It adds `_n` to every row and history table (history keyed by
`(id, _b, _n, _seq)`), `sn` to `_jazz_transactions` and `node` to
`_jazz_lv_via`.

Naming a pending commit's tables and columns by stored names, so a commit no
longer depends on the schema version that wrote it, dropped
`_jazz_pending_versions` and re-encoded the pending ops and serializable reads
(a read now names the version its query is in), which regenerated it the same
way.

Giving a replicated insert the backwards defaults of the columns its writer's
lineage dropped, and a replicated upsert the cells it writes only if it
creates its row (`created`), changed the encoded pending ops, which
regenerated it the same way.

Ordering unsettled versions last-writer-wins by their commits' clocks gave
each pending commit's header a `clock` (its writer's commit clock), which
regenerated it the same way.

Replacing the `(_n, _seq)` pair with one scalar position `_t` (below
2^53) regenerated it the same way. `_t` has two kinds: accepted (the
authority's position, below 2^52) and pending (`1<<52 |` the commit's HLC
as this store took it; a relay bounds a writer's clock to its own plus
500 ms). Every row and history table has `_t` in place of `_n` and `_seq`,
with index `__by_t`, and `_w`: a pending version's writer (its 16-byte node
and 8-byte big-endian tx), `X''` for an accepted version. History is keyed
by `(id, _b, _t, _w)`, so two writers' versions at one `_t` both stay.
`_jazz_transactions` is `(n, tx, t, result)` (index
`_jazz_transactions_by_t`), `_jazz_lv_via` has `t` in place of
`node`/`seq`, `_jazz_watermarks` is one `through` per key, the sequencer
tables are gone, and a pending commit's `clock` is its commit HLC.

Giving the reserved row columns descriptive names regenerated it the same
way: `_b` is `_branch`, `_t` is `_clock` (index `__by_clock`), `_w` is
`_writer`, `_st` is `_state` and `_cols` is `_touched`; `_base` keeps its
name. History is keyed by `(id, _branch, _clock, _writer)`.
