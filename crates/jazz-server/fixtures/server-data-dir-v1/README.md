# Server data directory, storage format 1

What an alpha.60 server has in its data directory: one `jazz.sqlite`
(storage format 1, `jazz::format::STORAGE_FORMAT`), gzip-compressed and
base64-encoded. It holds the app's rows, the catalogue tables (schemas,
lenses and permissions revisions) and, as a host table, the account journal
(see `dev/proofs/server-host-tables.md`).

Once alpha.60 ships the file is immutable: a change it would not read bumps
`STORAGE_FORMAT` or the host-table key version and adds a new corpus beside
this one. Until then format 1 is still moving, so such a change re-runs the
producer and re-pins the hash and account here and in the test.

- `jazz.sqlite.gz.base64`: SHA-256 of the SQLite file
  `aba3227d3ef16c85111bc06cb77852d300e881a96aa7774169b482725142a994`.

Producer: `produce_server_data_dir_corpus_v1` in
`crates/jazz-server/src/server/data_dir_corpus.rs` (ignored;
`JAZZ_WRITE_SERVER_DATA_DIR_CORPUS=1`). A catalogue-driven server
(`ServerBuilder` with a data dir, no schema) was:

- deployed `notes(title)` with a select permission, then written one note;
- deployed `notes(title, body)` with a migration introducing `body` (default
  `(none)`), then written a second note with a body;
- asked to register `alice` (issuer `https://issuer.example`), who got account
  `ab1a686c-2c31-488b-b12b-0f456b1b2c49`;
- shut down, which checkpoints the WAL into the file.

Reader: `pinned_server_data_dir_reopens_and_accepts_current_writes`, run by
exact name in `dev/gates/storage-compat.sh`.
