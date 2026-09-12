# Shallow-history SQL reference

This research harness asks how much the anonymized native cold-load fixture's
relationships, recursive permissions, shallow version selection, and readable
supporting rows cost in conventional SQL engines. It changes no runtime code.

The exporter calls the benchmark's actual `build_seed_plan`, rather than
reimplementing its generator. Default scale produces 46,740 application rows,
with one accepted mergeable transaction/version per row and no previous version
of any application row. The same 39 table queries return 27,518 permitted rows.
Each resource's permission follows group membership through at most eight steps;
children inherit their parent's permission. The dominant child query returns
23,831 rows.

## Variants

- `current`: ordinary typed SQL tables, recursive permission evaluation, full
  user-field results. No version metadata.
- `history`: typed version tables, accepted transaction lookup, and an indexed
  anti-join against accepted successors before the same permission query.
  The parent relation is empty in this fixture, but these lookups still execute.
- `witness`: the history query plus deduplicated supporting version coordinates,
  then full payload fetch. Support includes result rows, their parents, matching
  grants, and the reachable permission graph. This deliberately conservative
  graph is independently checked in Python. It is **not** asserted to be exactly
  Jazz's provenance selection, nor is this a model of opaque denied evidence.

Versions/transactions use synthetic integer identifiers. UUID row IDs and field
values come from the Rust export. Payload JSON is solely a research interchange
and bundle representation, not a proposed storage/wire format. It omits Jazz's
complete author, schema, and transaction envelopes. No compressed payload or
count-only query substitutes for fetching fields.

The history representation is sufficient for this fixture and the successor
sensitivity checks. It is not an implementation of arbitrary branch conflict
resolution, exclusive transactions, distributed acceptance, or schema migration.
In particular, a PostgreSQL transaction is not a substitute for Jazz's distributed
transaction protocol.

## Run

Build the existing native benchmark using the normal optimized perf profile:

```sh
cargo build -p jazz-sim --bench customer_cold_start --profile perf \
  --features jazz/testing,jazz/transport-compression-zstd,jazz-benchmark-guard/mimalloc
JAZZ_CUSTOMER_EXPORT_SQL=/tmp/sql-fixture.json \
  target/perf/deps/customer_cold_start-<build hash>
python3 dev/benchmarks/sql-proxy/run.py /tmp/sql-fixture.json \
  --sqlite /tmp/fresh-reference.sqlite --out /tmp/sqlite-receipt.json
# Requires psycopg 3. Use a dedicated disposable PostgreSQL database.
python3 dev/benchmarks/sql-proxy/run.py /tmp/sql-fixture.json \
  --postgres 'host=/tmp port=55439 dbname=reference user=ubuntu' \
  --out /tmp/postgres-receipt.json
```

Use the executable emitted by Cargo, not a stale hash. PostgreSQL creates a new
`sql_proxy` schema and refuses to overwrite one. SQLite refuses an existing file.
The harness does not modify any existing server configuration or delete databases.

## Measurement boundaries and verification

Five rounds rotate variant order. Each number sums execute + complete fetch for
39 independent queries. Python validation, hashing, fixture loading, index
construction, and ANALYZE are excluded. Plans are captured outside timings for
the dominant query. Both engines use the same queries and equivalent indexes;
PostgreSQL fetch includes its local Unix-socket client transport. SQLite fetch
is in-process. Results are fully consumed into Python tuples.

These are **seeded/cache-warm database** timings, including the first query pass,
not OS-cache-cold disk reads. They are not directly comparable to Jazz's full
Core → relay → empty client synchronization: there is no downstream durable
replay, graph initialization, ongoing IVM maintenance, subscription protocol,
reconciliation, or serialization of the full Jazz transaction representation.
Do not divide Jazz's end-to-end duration by these values and call it an engine
speedup. The meaningful comparison here is the incremental cost of adding
history and supporting-set construction to the same relational workload.

Every result ID and user field is checked against the exported fixture oracle.
Every conservative support ID and payload is checked against an independent
Python graph traversal. Untimed rollback-scoped mutations check unaccepted and
accepted successors and withdrawal of all permission seeds. Per-query hashes
permit cross-engine equality checks. Timing is evidence, not a CI assertion.

See [the Feldera IVM comparison](FELDERA.md) for the same fixture evaluated by
an incremental SQL engine, both on one node and through Core → Edge → Client.
