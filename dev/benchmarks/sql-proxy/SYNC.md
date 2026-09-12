# Captured synchronization and SQL ingestion

`ingest.py` extends the read-only reference to separate SQL stores representing
Core, Edge and Client. All material comes from the existing synthetic benchmark.
It is a static initial-load experiment, not a replacement sync implementation.

## Actual traffic, not an invented write set

Run the optimized Rust benchmark with `JAZZ_CUSTOMER_PHASES=cold` and
`JAZZ_CUSTOMER_CAPTURE_SYNC=/tmp/new-capture-directory`. Capture refuses to
replace existing files. Discard the capture run's timing: it serializes diagnostic
JSON and writes large files. `sync_capture_enabled` labels new receipts.

The two JSONL files retain full postcard transaction bytes, full postcard/JVRR
version envelopes, scope/fate/global-time/durability state, exact coordinates,
and fields decoded by Jazz itself. Loading the capture checks every field
against the fixture export and rejects other transaction kinds, non-root
branches, missing cells, unaccepted fates or nonempty predecessor lists.

This initial load delivers:

| Link          | Bundle deliveries | Distinct versions | Repeated deliveries |
| ------------- | ----------------: | ----------------: | ------------------: |
| Core → Edge   |            47,437 |            46,740 |                 697 |
| Edge → Client |            28,137 |            27,518 |                 619 |

The native receipt reports **one receiver bulk-ingest commit per node**. SQL
therefore also uses one data-ingest transaction. ANALYZE performs additional
engine bookkeeping/transaction work; its time is included separately. The topology replay reproduces
the captured ordering and duplicates after fetching rows from the source DB.
Source fetches are real SQL reads; no in-memory fixture substitutes for them.

## Receiver responsibilities

Each node has separate persistent storage (a SQLite file, or its own PostgreSQL
database in the isolated test cluster). Static tables/indexes are initialized
before timing, as the native benchmark opens its empty stores before its timer.
The timed receiver operation:

1. Coalesces duplicate immutable versions, checking exact equality.
2. Bulk loads staging tables (`executemany` or PostgreSQL `COPY`).
3. Checks existing transaction/version bytes for conflicts, then inserts missing
   records in one atomic SQL transaction.
4. Maintains transaction and row/version lookup indexes plus the relationship
   indexes used by permission queries.
5. Commits with the configured durability, then refreshes planner statistics.
   Statistics work is measured and reported separately, not free setup.

The store keeps **complete captured wire envelopes verbatim**, plus typed fields
for queries. This is deliberately storage-heavy: it does not intern repeated
wire descriptors as Jazz's native storage does. It provides a conservative SQL
reference, not a storage-layout equivalence claim.

History views check accepted transactions and absence of accepted successor
versions. The permission query is the same bounded recursive query as the first
experiment. Every result field, persisted transaction/envelope byte and duplicate
outcome is verified. Conflicting version bytes must roll back. Fresh connections
verify persisted results after close/reopen; this is not a power-cut test.

## Timed comparisons

- Captured Edge → Client delivery into an empty SQL receiver, followed by reads.
- Identical delivery into the populated receiver (idempotent replay).
- Core → Client, including Core permissions, ingestion and local reads.
- Core → Edge → Client: Core serves the trusted Edge all fixture rows; Edge
  evaluates user permissions; Client persists them and evaluates locally.

The two-hop total is a directly measured serial wall time, including source
query/fetch, packet ordering, each ingestion and Client reads. Assertions,
hashing, fixture parsing, initial Core seeding and node teardown are outside it.
The initial-load stores are fresh, but source data is cache-warm. It is not an
OS-cache-cold read test. Streaming readiness and overlapping node execution are
not modeled; these totals should not be extrapolated to a WAN.

SQLite uses WAL with synchronous NORMAL (relaxed) or FULL (durable), with
checkpointing deferred beyond commit. PostgreSQL uses synchronous_commit off
or on with the cluster's fsync enabled. Commit latency is included. Relaxed
means recent ingest is recoverable by resynchronization; durable means waiting
for the engine's synchronous commit, not a tested hardware survival guarantee.

Per-ingest counters include client CPU, backend CPU (Linux scheduler ticks),
backend physical write/read bytes and PostgreSQL WAL insertion bytes. PostgreSQL
background writers are not included in backend `/proc` I/O. SQLite file + WAL
size and PostgreSQL database size have different meanings; do not equate them.

## Codec and semantic boundary

SQL receives predecoded typed fields as well as full wire bytes. Native transport
also passes logical SyncMessages in-process, but its receiver's field access and
transport accounting differ. To bound that omission separately, run the same
Rust executable with `JAZZ_CUSTOMER_DECODE_CAPTURE=<capture JSONL>`: it times the
real postcard/JVRR decoder and field extraction while retaining the decoded
batch. JSON/hex capture parsing is excluded. Do not silently add these diagnostic
codec timings to a SQL wall-time receipt and call it a measured end-to-end run.
The SQL topology does not serialize a real Jazz network connection.

The experiment preserves the data and permission outcomes of this initial
shallow-history fixture. It omits ongoing IVM maintenance, authority receipt
bookkeeping, cancellation, reconnect reconciliation, branching, exclusive
transactions and other behaviors not exercised by the captured load. A ratio
to Jazz is evidence of remaining work to explain, not proof that all omitted
machinery is removable overhead.

## Run

```sh
python3 dev/benchmarks/sql-proxy/ingest.py /tmp/sql-fixture.json /tmp/capture \
  --rounds 3 --durability relaxed --out /tmp/sqlite-sync.json
# psycopg 3 required; use an isolated cluster. The harness creates and deletes
# only uniquely named sql_sync_* databases that it owns.
python3 dev/benchmarks/sql-proxy/ingest.py /tmp/sql-fixture.json /tmp/capture \
  --postgres 'host=/tmp port=55439 dbname=postgres user=ubuntu' \
  --rounds 3 --durability durable --out /tmp/postgres-sync.json
```

Temporary SQL databases are removed after successful validation (or on error);
keep JSON receipts. The captured data is public synthetic material, but is large
and should remain a local evidence artifact rather than enter Git history.
