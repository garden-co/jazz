# Feldera permissioned IVM reference

This experiment uses the existing anonymized fixture: 46,740 source rows,
24,102 permitted user resources, and 27,518 total permitted rows across the
39 table outputs. It does not increase fixture size or change Jazz runtime code.

Feldera OSS 0.349.0, revision `6d62383f00038f56c7aa87a6ba8badc1db36dbbb`, runs
locally with one worker per pipeline and optimized SQL compilation. The image
is pinned to
`images.feldera.com/feldera/pipeline-manager@sha256:721bb36715e34b0361495e86d6aae6971661e8d8654c44ff9610aae767036143`.

## What the engine computes

The SQL program maintains accepted current versions, a bounded depth-eight
recursive group-membership relation, resource grants, inherited child
permissions, and a materialized output containing complete user payloads.
A request input activates an identity. Changing that input causes actual IVM
updates; the Python runner does not compute permission results for Feldera.

The version model joins accepted transaction IDs and excludes versions with
accepted successors. The fixture itself has one accepted version per row and
no parents. This is a shallow-history reference, not full Jazz merge semantics.

`scope_rows` is computed incrementally. Its subsequent `SELECT *` fetch uses
Feldera's ad-hoc API solely to read that materialized result. No permission SQL
is executed in the ad-hoc engine. Fetching, JSON serialization, HTTP transfer,
and Python decoding are included in end-to-end timings.

## Single node and topology

Single node: source rows and their accepted transactions are already installed,
the compiled pipeline is running, and no request is active. The timer covers
activating the ordinary identity, waiting for its transaction to finish, and
fetching all output payloads. Removing the request between rounds is untimed.
These are data-ready reads, not empty-node ingestion or pipeline compilation.

Topology: all three independent pipelines are started before the timer. Core
is populated; Edge and Client are empty but have an active ordinary-identity
request. Core is activated in trusted mode and its 46,740 source rows are
fetched from its maintained output. Those actual returned rows and their
accepted-version metadata are fed into Edge in one bulk transaction. Edge
produces 27,518 permissioned rows, which are fed into Client in another bulk
transaction. Client evaluates permissions locally and all its rows are fetched.

These unique input sets match the previous captured Jazz topology: trusted
Core→Edge 46,740; untrusted Edge→Client 27,518. The fixture's requested auxiliary
tables include the membership/grant rows; permitted resources include parents
of permitted children. Consequently their union is sufficient readable support
for downstream evaluation. This does not establish support completeness for
arbitrary queries or policies with hidden evidence.

The `trusted` parameter is controlled by the benchmark to model the trusted
Core→Edge link; it is not an authentication API or production security design.
Each stage has its own retained input and IVM state. It never receives its
result directly from the fixture. All stages execute serially on one host;
this is not a WAN or browser test.

## Measurements

Five rounds, no concurrent compilation or other owned benchmark. All times
below are milliseconds; component medians need not sum to the median total.

| Measurement                                                       |     Median |
| ----------------------------------------------------------------- | ---------: |
| Single-node activation, completion wait                           |       85.0 |
| Single-node complete result fetch                                 |      142.8 |
| **Single-node total**                                             |  **227.8** |
| Core activation and complete trusted fetch                        |      324.4 |
| Edge ingest, maintained permission calculation, completion wait   |      564.3 |
| Edge complete permissioned fetch                                  |      138.1 |
| Client ingest, maintained permission calculation, completion wait |      358.8 |
| Client complete permissioned fetch                                |      165.4 |
| **Core→Edge→Client total**                                        | **1559.1** |

Single totals: 221.5, 229.7, 233.8, 227.8, 225.2 ms.
Topology totals: 1509.7, 1554.6, 1599.1, 1561.7, 1559.1 ms.

Feldera's single-node `runtime_elapsed_msecs` increased by median 36 ms,
while process CPU increased by 57 ms. Empty Edge/Client runtime totals after
installation were median 290/187 ms; these include their small initial request
setup. End-state RSS was about 234/201 MB. Reported storage was 119 bytes per
receiver: this workload remained effectively memory-resident and fault tolerance
was not enabled. These counters are not a durable-ingest or peak-memory claim.

The first disposable trial installed Core from empty in about 588 ms, outside
its read/topology timers. The repeated receipt's setup replays existing Core
inputs and must not be described as another fresh installation measurement.

## What is and is not comparable

The earlier native Jazz all-memory topology was roughly 11 seconds. This
reference is substantially faster, but the ratio is not a drop-in engine gain:

- This is one ahead-of-time compiled SQL program with shared permission
  relations across the 39 outputs. Jazz dynamically lowers and maintains its
  subscription programs. Compilation and process startup are excluded here.
- Relationship fields are typed SQL columns. Complete remaining user fields
  travel in an opaque canonical JSON payload; they are not separately decoded
  into SQL columns. Every field is checked after fetching.
- Transaction IDs are synthetic ordinals. Full Jazz authorship, schema and
  transaction/wire envelopes, receipts, reconnect reconciliation, opaque denied
  evidence, and arbitrary history/branch/exclusive-transaction semantics are
  omitted.
- Unique versions are forwarded once. The extra 697/619 captured duplicate
  deliveries are not replayed. SQL primary keys provide ordinary replacement
  behavior, not Jazz's complete immutable-conflict admission checks.
- Request activation is followed by complete maintained-view snapshots over
  local HTTP JSON. The harness does not implement streaming transport overlap
  or an ongoing distributed subscription protocol.

The evidence does establish that maintaining this recursive permission workload
with a general IVM engine need not take the current Jazz topology's duration.
It does not say which omitted or differently represented responsibilities
account for the remaining difference.

## Validation

Every measured stage checks the complete ID set, absence of duplicates, each
user payload, each extracted relationship field, and the version ID.
Untimed incremental checks revoke all membership seeds (output drops to 3,382
auxiliary rows), restore them, add an unaccepted successor, accept it, and reject
it again. Output restores correctly at each step. A deliberate trusted-mode
permission bypass makes the independent expected-ID oracle fail. No existing
Jazz tests were altered.

## Reproduce

Use the existing Rust fixture export from README.md at default scale.
Start the pinned image on loopback (requires Docker and Python `requests`):

```sh
docker run -d --name jazz-feldera-proxy -p 127.0.0.1:18080:8080 \
  images.feldera.com/feldera/pipeline-manager@sha256:721bb36715e34b0361495e86d6aae6971661e8d8654c44ff9610aae767036143
python3 dev/benchmarks/sql-proxy/feldera.py /tmp/sql-fixture.json --out /tmp/feldera
python3 dev/benchmarks/sql-proxy/feldera.py /tmp/sql-fixture.json --out /tmp/feldera \
  --run --rounds 5
```

The create command refuses an existing pipeline name; choose `--name` for a
fresh Core. Receiver names are unique per round and are stopped after use.
The runner keeps the Core and local container for inspection; stop the container
when finished with `docker stop jazz-feldera-proxy`. Full local receipts are in
`/home/ubuntu/jazz-debug-evidence/permissioned-profile/feldera/repeated/`.

Implementation references: [recursive SQL](https://docs.feldera.com/sql/recursion/),
[bulk transactions](https://docs.feldera.com/pipelines/transactions/),
[materialized views](https://docs.feldera.com/sql/materialized/), and
[latency counters](https://docs.feldera.com/pipelines/latency/).

Tooling friction: the local Docker runtime needed installation; the recursive
forward declaration needed to retain SQL-inferred nullability even after an
`IS NOT NULL` predicate. REST table names use uppercase for unquoted SQL names.
The pinned image supplied precompiled dependencies, so subsequent identical
pipelines reused the compiled program. No larger fixture was run.
