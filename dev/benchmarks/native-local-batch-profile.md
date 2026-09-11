# Native local batch: phase and CPU attribution

Measured on the indexed-selection implementation `a7c85d2e7b` (PR #2787).
This is a diagnostic receipt, not a timing gate or a predicted browser result.
No production runtime code was changed for this investigation.

## Workload and boundaries

`crates/jazz/benches/local_batch_phases.rs` creates a synthetic `tasks` table
with `title` and `done`. It seeds one transaction, drops/reopens the worker,
sends a Local query snapshot to a fresh in-memory foreground, updates 90% of
rows in one foreground transaction, uploads the commit to the worker, returns
the updated supporting rows, then drops/reopens both runtimes and repeats the
read. Exact row-ID sets and the exact set of completed rows are asserted.
A deterministic small version is in `legacy_benchmark_smoke`.

The worker backend is either shared MemoryStorage or RocksDB `WalNoSync`;
the foreground is MemoryStorage. Both use real Jazz/Groove code, wire codecs,
Local relay ingestion, and maintained peer publication. Seeding directly on the
worker is fixture setup. The test excludes JS bindings, browser scheduling,
IndexedDB page storage, external authentication, network latency, remote fate
assignment, and the facade's open-transaction staging. It provides complete
replacement cells and explicit parent IDs to the low-level authoring call.
RocksDB closes/reopens here; this does not evict OS disk caches. Raw storage
measurements include an explicit durability flush and 128-byte values, but
exclude construction of the input vector. They are a substrate baseline, not
semantically equivalent Jazz operations.

The initial attempt at a 10,000-row single transaction exceeded the existing
4,096-version wire limit; that is not a valid timing result. The harness now
rejects that size up front. Larger logical workloads require multiple batches.

## Reproduce

```sh
cargo build -p jazz --bench local_batch_phases --profile perf \
  --features testing,transport-compression-zstd
# Cargo prints the executable path; use that exact binary below.
JAZZ_BATCH_ROWS=150,750,1500,3000 target/perf/deps/local_batch_phases-<hash>
JAZZ_BATCH_ROWS=1500 JAZZ_BATCH_UPDATE_PERCENT=50 \
  target/perf/deps/local_batch_phases-<hash>
dev/t --test legacy_benchmark_smoke local_batch_phases_correctness_smoke
```

Build once, run repeatedly. The first native dependency build is a cold cost;
subsequent harness-only optimized builds took about 29 seconds. No WASM build
is needed. Use fully optimized browser builds at checkpoints, not an
unoptimized WASM proxy for release performance.

On Linux, collect attribution separately from ordinary timings:

```sh
perf record --clockid mono -e task-clock -F 999 \
  --call-graph dwarf,16384 -o /tmp/local-batch.perf -- \
  env JAZZ_BATCH_ROWS=1500 target/perf/deps/local_batch_phases-<hash>
perf script --no-inline --ns -i /tmp/local-batch.perf \
  -F comm,pid,tid,time,event,ip,sym,dso
```

The JSONL phase start/end timestamps use CLOCK_MONOTONIC on Unix. Explicitly
select that clock in perf before assigning samples to phases. The first
capture used perf's default clock and was discarded for phase attribution.
The valid capture contains five runs at 1,500 rows. Kernel symbols are not
available; some outer async stacks are truncated. Inclusive function sample
fractions overlap and must not be summed. `--no-inline` avoids slow system
library addr2line expansion; Rust function symbols remain available.

## Uninstrumented timing

Representative sweep, milliseconds at 1,500 rows / 1,350 updates:

| Phase                                                     | Memory worker | RocksDB worker |
| --------------------------------------------------------- | ------------: | -------------: |
| Raw 1,500-value batch                                     |          0.34 |           0.49 |
| Raw durability flush                                      |         <0.01 |           0.89 |
| Raw scan                                                  |          0.13 |           0.25 |
| Initial supporting-row publication                        |         209.4 |          229.5 |
| Initial foreground ingestion                              |         124.3 |          126.8 |
| Initial foreground query                                  |          33.3 |           34.8 |
| Foreground authoring                                      |          86.7 |           85.7 |
| Foreground persistence                                    |           5.4 |            5.6 |
| Build upload payload                                      |          17.9 |           18.5 |
| Worker ingestion, persistence and maintained-query update |         247.1 |          261.4 |
| Updated supporting-row publication                        |         174.7 |          180.1 |
| Already-authoring foreground ingestion                    |         113.8 |          120.5 |
| Foreground query after update                             |          41.6 |           44.0 |
| Fresh foreground ingestion after reopen                   |         886.6 |          886.1 |

Wire encoding/decoding usually takes a few milliseconds per message; checked
upload decoding took about 11–12 ms. These costs are recorded separately.
Another uninstrumented 90%-update run showed the same ordering (fresh receiver
870 ms with memory worker, 859 ms with RocksDB worker). These are observations,
not confidence intervals. Native authoring/persistence alone is roughly 15k
row updates/s; the full roundtrip is much slower because of subsequent work.

## Finding 1: partial-parent misses repeatedly read siblings

Memory-worker fresh foreground ingestion scales as follows at 90% updated:

|  Rows | Time (ms) |
| ----: | --------: |
|   150 |      22.3 |
|   750 |     256.4 |
| 1,500 |     886.6 |
| 3,000 |   3,376.4 |

`validate_known_parent_coordinate` represents 3,584 of 4,449 sampled stacks
in the 1,500-row post-update receiver phase (~81%).
`query_versions_for_tx` appears in 3,506 (~79%). These overlap.

Mechanics: the fresh scope includes 150 unchanged rows from transaction A and
1,350 current rows from transaction B, each naming a parent in A. A is therefore
known but partial. An exact parent lookup misses for a B row. Validation calls
`query_versions_for_tx_physical_coordinate`, whose cold fallback reads A's
entire retained fragment. It then calls `query_versions_for_tx` again to assess
completeness. The cold read caches table names, not the materialized versions,
so the same sibling fragment is repeatedly read, decoded and sorted.

Varying only the updated percentage at 1,500 rows confirms the cross-product:

| Updated | Fresh receiver ms | Total read counter |
| ------: | ----------------: | -----------------: |
|     10% |             844.4 |            816,793 |
|     50% |           2,151.1 |          2,259,793 |
|     90% |             869.9 |            822,793 |
|    100% |             134.7 |             13,530 |

At 50%, history-index reads alone are 1,126,500: approximately two scans of
750 siblings for each of 750 missing parents, plus ordinary work. Read counters
include point probes and returned index/row entries; these are not disk I/Os.
At 100%, A is wholly absent, so the missing-transaction branch returns without
loading siblings. A smaller update can therefore be dramatically slower.

Next design target: batch parent-coordinate evidence and completeness metadata.
An incomplete parent transaction must remain inconclusive when the exact
witness is unavailable; a complete transaction with a wrong coordinate must
still be rejected. Do not retrieve old parent bodies merely to improve timing.
This is the cold-fallback case already tracked in #2784.

## Finding 2: trusted internal rows still get round-trip validation

`node/descriptor_roles.rs::encode_current_payload_record` builds a new row,
serializes per-field type descriptors to compare their trees, decodes the row,
re-encodes it to compare bytes, and emits a result descriptor. This is local
trusted output, not untrusted wire admission. Its inclusive stack fraction is
151/842 (~18%) of memory-worker batch publication samples.

`maintained_subscription_view::decode_typed_terminal_record` covers 318/842
(~38%) of that phase. These overlap: result-payload encoding is one of its
children. `decode_typed_version_witness` separately decodes cells and authors
into values, reconstructs history values, and creates another version record.
Schema-level checks and plans should be shared; trusted records should not
make repeated encode/decode validation round trips.

## Finding 3: conversion and work amplification remain after batching

For 1,350 updates, both foreground authoring and worker ingestion each emit
8,102 physical writes (~six per row plus transaction metadata), about 1.70 MB.
The authoring phase records 8,100 point/range operations; the worker ingest
records 9,467 ranges/point probes and 12,165 returned/probed entries. Its last
IVM tick encodes about 1.02 MB of notifications. These metrics describe that
commit/tick, not every operation over the entire scenario.

The raw substrate remains fast while Jazz timings are similar with either
backend. Multiple logical records have semantic purposes, but batching storage
writes does not yet make the surrounding per-row transformations cheap.

CPU evidence and corresponding code paths:

- Worker ingest: IVM evaluator update frames occur in ~36% of samples;
  `BorrowedRecord::to_values` in ~28%. Large-value materialization appears in
  ~14%, despite this fixture containing only small inline values. It decodes
  a whole row to inspect values and returns a copied byte vector when nothing
  changes (`groove/large_values.rs::materialize_record_attempt`).
- Batch publication: deletion-winner fallback occupies ~21%, predominantly
  per-row query work. This is separate from result-terminal conversion.
- Known foreground receipt: `preflight_view_bundle_conflicts` occupies ~37%,
  with stored-row to wire-record reconstruction underneath. The later
  `ingest_known_transaction` occupies ~32%; the earlier ensure_exact change
  did not remove this preflight's full stored-version reconstruction.
- Even the plain receiver query rebuilds public row representations:
  `CurrentRow::project` appears in ~50% of initial receiver-query samples.
  `normalize_public_current_rows` invokes projection for every result row,
  rebuilding fields, values and metadata even for this simple all-rows query.
- Allocation/free routines plus byte-copy routines account for at least
  33–39% of self samples in the major measured phases. This is disjoint leaf
  attribution, not the sum of overlapping inclusive frames.

## Performance thesis

The next gains should come from reducing work above storage, not switching
backends or only making the next tiny helper faster. First remove the
partial-parent cross-product. Then remove trusted round-trip checks and move
schema/descriptor work out of row loops. Finally retain owned immutable row
bytes plus typed identity/provenance across query maintenance and publication,
so unchanged rows and known receipts do not repeatedly become value trees and
new encoded records. Preserve permission selection and authored-row fidelity;
those semantics do not require every current implementation conversion.

This investigation proposes those changes; it does not implement them or
claim that 100k semantic end-to-end writes/s is already achievable.
