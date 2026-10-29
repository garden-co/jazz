# SQLite storage format 1 corpus

What an alpha.60 user has on disk: SQLite files written by Jazz storage
format 1 (`jazz::format::STORAGE_FORMAT`), gzip-compressed and base64-encoded.
Once alpha.60 ships they are immutable: a change that a format-1 file would
not read bumps `STORAGE_FORMAT` and adds a new corpus beside this one, and
never rewrites these. Until then format 1 is still moving, so such a change
re-runs the producer and re-pins the hashes here and in the test instead
(for example: enum columns stored as tags, per-write timestamps on the wire changed how pending uploads
are stored, a rejection's reason name changed how fates are recorded, and
pending uploads moved from one encoded message to a row per op).

- `server.sqlite.gz.base64`: an authority (WAL, default page size). SHA-256 of
  the SQLite file `e4fd47809b657a3f582e8044106a5cfd02fbae80ab4de24da92cd857872562c0`.
- `client.sqlite.gz.base64`: a client replica that synced with it. SHA-256
  `a852eb2bcd97b31b6607ea8d761eb24295af9f2518a5354848f0cc1a94a26d01`.

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
  write as sent (`_st` bit 2, at `_seq` = minus the commit's seq);
- a client that synced `projects` and `documents`, wrote one row that
  settled, one stamped far ahead of the server's clock that the server
  rejected (so `_jazz_applied` keeps a rejected fate with its reason name),
  then two writes (an insert and an update) that never reached the
  server: its pending uploads;
- then the server activated a second schema version that renames
  `documents.title` to `heading` (a stored-names lens) and wrote under it.

The receipts that read it back run by exact name in `dev/gates/storage-compat.sh`.

A format-1 file written by a build between #4409 and #4432 (which records
column types, but enum columns as text) is not read by later builds: its
enum column's recorded type no longer matches, so opening it is refused
with "changing the type of `<table>.<column>`". Format 1 had not shipped;
such a store is recreated, not migrated.

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
