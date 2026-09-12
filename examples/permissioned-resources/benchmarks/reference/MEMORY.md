# Memory-only topology checkpoint

This experiment changes only the native cold-load benchmark's storage adapter.
The same Jazz/Groove query, authorization, synchronization and ingestion logic
runs in all configurations; no production durability semantics change.

## Results

Three optimized native runs per configuration, rotated in execution order.
Same synthetic seed, 39 subscriptions, 27,518 materialized rows. Setup and Core
seed copying are outside the measured interval. These are cold receiver loads,
not cold operating-system page-cache measurements.

| Storage (Core / Edge / Client) | Median full load |           Range | Core tick time | Edge tick time | Client tick time |
| ------------------------------ | ---------------: | --------------: | -------------: | -------------: | ---------------: |
| RocksDB / RocksDB / RocksDB    |         12.652 s | 12.642–12.882 s |        2.766 s |        5.538 s |          2.646 s |
| RocksDB / Memory / Memory      |         11.509 s | 11.496–11.643 s |        2.725 s |        4.775 s |          2.296 s |
| Memory / Memory / Memory       |         11.213 s | 11.207–11.497 s |        2.613 s |        4.725 s |          2.212 s |

Node columns are independently computed medians of time inside node ticks;
they omit driver work and should not be added to reconstruct full load time.
Memory receivers improve wall time by 9.0%; all-memory improves it by 11.4%.

Every run ingested 46,740 unique bundles at Edge and 27,518 at Client, with
68,015,375 raw payload bytes delivered to Client. An additional untimed
all-memory capture matched the prior RocksDB capture's complete bundle
multisets, including transaction bytes, version bytes, decoded fields and
duplicate multiplicities: 47,437 deliveries Core→Edge and 28,137 Edge→Client.
All existing expected-result and permission assertions passed.

## Meaning and limits

Most elapsed time survives removal of every persistent backend from the timed
load. A recoverable receiver cache cannot achieve a large improvement merely
by swapping RocksDB for the current MemoryStorage. It would need to reduce
encoding, indexing, publication or other ingestion work that remains in this
experiment. This is evidence for investigating those costs, not a measurement
that assigns the entire remainder to any one of them.

MemoryStorage itself uses mutex-protected ordered maps and owned byte buffers;
it is not an ideal bulk in-memory representation. This comparison therefore
is not an exact isolation of disk I/O cost. RocksDB already batches receiver
bulk ingestion, and its existing WAL policy is unchanged. Browser IndexedDB
may have a different storage cost; these are native results only.

No checkpoint/restart performance, durable local outbox, crash recovery or
production acknowledgment changes are evaluated here. Memory configurations
reject phases other than `cold` rather than pretending to reopen persisted data.

## Reproduction and provenance

Build the `customer_cold_start` bench with profile `perf` and features
`jazz/testing,jazz/transport-compression-zstd,jazz-benchmark-guard/mimalloc`.
Run the same executable with `JAZZ_CUSTOMER_PHASES=cold` and
`JAZZ_CUSTOMER_STORAGE=rocks`, `memory-receivers`, or `all-memory`.
All configurations use BoxedStorage so the adapter boundary is shared.
The all-memory Core is initialized from the same cached physical RocksDB seed;
every column family's keys and values are compared exactly after copying.

Measured source: parent `876fdd92d50d286d03630c6203348a3cee26e556` plus the
benchmark-only change accompanying this report. Receipts report the parent
SHA and dirty working tree because compilation preceded the experiment commit.
Local raw receipts and executable path are preserved under
`/home/ubuntu/jazz-debug-evidence/permissioned-profile/memory-topology/`.
Capture timings are excluded from the table.
