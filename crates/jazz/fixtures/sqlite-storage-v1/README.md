# SQLite storage format 1 corpus

What an alpha.60 user has on disk: SQLite files written by Jazz storage
format 1 (`jazz::format::STORAGE_FORMAT`), gzip-compressed and base64-encoded.
Once alpha.60 ships they are immutable: a change that a format-1 file would
not read bumps `STORAGE_FORMAT` and adds a new corpus beside this one, and
never rewrites these. Until then format 1 is still moving, so such a change
re-runs the producer and re-pins the hashes here and in the test instead
(for example: per-write timestamps on the wire changed how pending uploads
are stored, a rejection's reason name changed how fates are recorded, and
pending uploads moved from one encoded message to a row per op).

- `server.sqlite.gz.base64`: an authority (WAL, default page size). SHA-256 of
  the SQLite file `ee21141c812da22c74ceb53b8f15da26885b5b33d0dbef9880528b180a463a09`.
- `client.sqlite.gz.base64`: a client replica that synced with it. SHA-256
  `94ab9a72d943afdccc33f3ef3dfd1e898a812da55b19fc2bad976f019109b747`.

Producer: `produce_sqlite_storage_corpus_v1` in `crates/jazz/tests/storage_corpus.rs`
(ignored; `JAZZ_WRITE_SQLITE_STORAGE_CORPUS=1`). It wrote, with fixed row ids,
nodes and write times:

- `projects`, `documents` (reference, indexed title, a 300 KiB chunked large
  value, bytes, a defaulted column) and `drafts` (branched by `branch_id`);
- two interned authors, updates (history versions), a soft delete and a row
  on a branch;
- a client text edit to `documents.body` (text-merging) that the server
  merged with its own concurrent edit, so its history keeps the client's
  write as sent (`_st` bit 2, at `_seq` = minus the commit's seq);
- a client that synced `projects` and `documents`, wrote one row that
  settled, one stamped far ahead of the server's clock that the server
  rejected (so `_jazz_transactions` keeps a rejected fate with its reason name),
  then two writes (an insert and an update) that never reached the
  server: its pending uploads;
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
