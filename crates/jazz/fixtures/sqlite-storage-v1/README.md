# SQLite storage format 1 corpus

What an alpha.60 user has on disk: SQLite files written by Jazz storage
format 1 (`jazz::format::STORAGE_FORMAT`), gzip-compressed and base64-encoded.
Once alpha.60 ships they are immutable: a change that a format-1 file would
not read bumps `STORAGE_FORMAT` and adds a new corpus beside this one, and
never rewrites these. Until then format 1 is still moving, so such a change
re-runs the producer and re-pins the hashes here and in the test instead
(for example: per-write timestamps on the wire changed how pending uploads
are stored, and a rejection's reason name changed how fates are recorded).

- `server.sqlite.gz.base64`: an authority (WAL, default page size). SHA-256 of
  the SQLite file `b43565e8a6a85d251ab29128c4f52b2c71ff7791f9e5d42f2a1ee5c40d2e92a9`.
- `client.sqlite.gz.base64`: a client replica that synced with it. SHA-256
  `89d5bae39a2d389b4ca190c4209d431708b8768b98b8c5b152945e871f533cbf`.

Producer: `produce_sqlite_storage_corpus_v1` in `crates/jazz/tests/storage_corpus.rs`
(ignored; `JAZZ_WRITE_SQLITE_STORAGE_CORPUS=1`). It wrote, with fixed row ids,
nodes and write times:

- `projects`, `documents` (reference, indexed title, a 300 KiB chunked large
  value, bytes, a defaulted column) and `drafts` (branched by `branch_id`);
- two interned authors, updates (history versions), a soft delete and a row
  on a branch;
- a client that synced `projects` and `documents`, wrote one row that
  settled, one stamped far ahead of the server's clock that the server
  rejected (so `_jazz_applied` keeps a rejected fate with its reason name),
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
