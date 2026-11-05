# SQLite storage format 3 corpus

These authority and replica snapshots pin storage format 3. They were generated
once by `produce_sqlite_storage_corpus_v3` using the public database API.
Historical format-1 and format-2 files remain unchanged and are refused without
modification. There is no migration or backward reader for those layouts.

SHA-256 of the decoded SQLite files:

- `server.sqlite.gz.base64`: `93c5ffcaa43d6b0afc89009a918315c2951eba9f710bdcde35c0507fdd578446`
- `client.sqlite.gz.base64`: `4e2f587afb652dc0e1713845c49b3de153e15f9a93175530068683aa2b4f5c1b`

The scenario preserves the schema rename, history, branch, large value,
provenance, accepted client writes and pending uploads. It also records an
authority rejection with reason `client_clock_too_far_ahead`. This is the new
durable layout that requires format 3: rejected results contain an optional
reason name before their display message.

Tests reopen both stores, inspect the recorded rejection, settle pending
uploads, make current writes and reopen again. The independent raw result
fixture is `../sqlite_write_fate_v2.json`.

All connected runtimes must use matching artifacts in the same release.
See [the wire and storage compatibility contract](../sqlite-sync-v3/README.md).
