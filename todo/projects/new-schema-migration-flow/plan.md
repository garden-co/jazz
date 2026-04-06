# Implementation Plan

1. Extend `schema export`:
   - make `--schema-dir` and `--schema-hash` mutually exclusive
   - load from local snapshots first
   - fetch from server on miss, persist the JSON snapshot, print it

2. Rework `migrations create` default flow:
   - no positional hashes
   - first run writes only the initial snapshot
   - no-op when latest snapshot matches current schema
   - normal run diffs latest snapshot -> current schema and writes migration + snapshot

3. Implement explicit historical flows:
   - `--fromHash <hash>` -> current schema
   - `--fromHash <hash> --toHash <hash>` -> fetched/local target schema
   - clear failures when a hash cannot be resolved locally or from the server

4. Update runtime warning text and CLI help to point to:
   - `schema export --schema-hash <schemaHash>`
   - `migrations create --fromHash <schemaHash>`

5. Add integration coverage for end-to-end recovery:
   - missing server schema -> export by hash -> create recovery migration
   - verify snapshot reuse and generated filenames/content
