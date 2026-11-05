# SQLite storage format 1 corpus

These historical SQLite files pin the earlier unreleased format-1 layout.
They remain unchanged as refusal fixtures: the current format-2 reader must
reject them without changing their files or sidecars. Current readable stores
are pinned separately in [the format-2 corpus](../sqlite-storage-v2/README.md).

- `server.sqlite.gz.base64`: an authority (WAL, default page size). SHA-256 of
  the SQLite file `a10348d551dcbb5db42f4ff3dbe441aadae69be7e12da573f856a53df80ca532`.
- `client.sqlite.gz.base64`: a client replica that synced with it. SHA-256
  `2770a3d61c5e1123a406992c36b7c8066901128238cf00faf682527e9201cf51`.

The original `produce_sqlite_storage_corpus_v1` scenario wrote, with fixed row
ids, nodes and write times:

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
