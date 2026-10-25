# SQLite storage format 1 corpus

What an alpha.60 user has on disk: SQLite files written by Jazz storage
format 1 (`jazz::format::STORAGE_FORMAT`), gzip-compressed and base64-encoded.
They are immutable. A change that a format-1 file would not read bumps
`STORAGE_FORMAT` and adds a new corpus beside this one; it never rewrites
these.

- `server.sqlite.gz.base64`: an authority (WAL, default page size). SHA-256 of
  the SQLite file `c6f4a4fcc528b45d7572a319058a347f5def9c0c10a2b1589cf4c179df0451d7`.
- `client.sqlite.gz.base64`: a client replica that synced with it. SHA-256
  `5e43039e4b91099d9e00bd8bd41fe05d2b9d379da567fa9cc0815a5c8c1ae500`.

Producer: `produce_sqlite_storage_corpus_v1` in `crates/jazz/tests/storage_corpus.rs`
(ignored; `JAZZ_WRITE_SQLITE_STORAGE_CORPUS=1`). It wrote, with fixed row ids,
nodes and write times:

- `projects`, `documents` (reference, indexed title, a 300 KiB chunked large
  value, bytes, a defaulted column) and `drafts` (branched by `branch_id`);
- two interned authors, updates (history versions), a soft delete and a row
  on a branch;
- a client that synced `projects` and `documents`, wrote one row that
  settled, then two writes (an insert and an update) that never reached the
  server: its pending uploads;
- then the server activated a second schema version that renames
  `documents.title` to `heading` (a stored-names lens) and wrote under it.

The receipts that read it back run by exact name in `dev/gates/storage-compat.sh`.
