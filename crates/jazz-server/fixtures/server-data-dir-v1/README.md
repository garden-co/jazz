# Server data directory, storage format 1

What an alpha.60 server has in its data directory: one `jazz.sqlite`
(storage format 1, `jazz::format::STORAGE_FORMAT`), gzip-compressed and
base64-encoded. It holds the app's rows and, as host tables, the server
catalogue, the account journal and the runtime catalogue (see
`dev/proofs/server-host-tables.md`).

Once alpha.60 ships the file is immutable: a change it would not read bumps
`STORAGE_FORMAT` or the host-table key versions and adds a new corpus beside
this one. Until then format 1 is still moving, so such a change re-runs the
producer and re-pins the hash and account here and in the test.

- `jazz.sqlite.gz.base64`: SHA-256 of the SQLite file
  `4ecff0458673c7bee316304dc5ca594bd7ddd36ed521b77607af95443e82756b`.

Producer: `produce_server_data_dir_corpus_v1` in
`crates/jazz-server/src/server/data_dir_corpus.rs` (ignored;
`JAZZ_WRITE_SERVER_DATA_DIR_CORPUS=1`). A catalogue-driven server
(`ServerBuilder` with a data dir, no schema) was:

- deployed `notes(title)` with a select permission, then written one note;
- deployed `notes(title, body)` with a migration introducing `body` (default
  `(none)`), then written a second note with a body;
- asked to register `alice` (issuer `https://issuer.example`), who got account
  `81a787c1-761f-4d56-a625-da95e1fbc306`;
- shut down, which checkpoints the WAL into the file.

Reader: `pinned_server_data_dir_reopens_and_accepts_current_writes`, run by
exact name in `dev/gates/storage-compat.sh`.
