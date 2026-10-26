# SQLite storage format 2 corpus

These authority and replica snapshots pin storage format 2. They were generated
once by `produce_sqlite_storage_corpus_v2` in `tests/storage_corpus.rs`, using
the upstream public-API scenario. Format-1 files remain unchanged beside them
and are tested for refusal without any file or sidecar changes.

SHA-256 of the decoded SQLite files:

- `server.sqlite.gz.base64`: `072244d4ee6a63d440a2797927b4373d261ee312be71bc2537d2bbe9741dd2fa`
- `client.sqlite.gz.base64`: `994bdd01c34a0dfb8706fe6e47bdb9bc48cd8b93907276ea78d0233161243ec4`

The corpus retains the upstream schema rename, history, branch, large value,
provenance, accepted client writes and pending uploads. The tests reopen both
files, verify those values, settle the pending uploads, make current writes
and reopen again. Damaged current-format stores must fail as storage errors;
other format stamps must be refused without mutation.

Format 2 stores the reconciled per-operation timestamps, echo hints and E2EE
pending-upload layout. There is no migration or backward reader for format 1.
See [the sync/storage contract](../sqlite-sync-v2/README.md) for the version
fences and the requirement to release matching runtimes together.
