# Persistent server-storage core hydration benchmark receipt

Date: 2026-07-29

Status: directional developer-machine evidence, not a performance promise.

## Why this lane exists

The earlier integrated SaaS fan-out receipt used `MemoryStorage`, a local
pending fixture, and Local reads. That is useful for query-engine attribution,
but it is not the default server configuration.

The new `saas_persistent_hydration` harness uses the serving database's durable
path:

- `RocksDbStorage::open`, whose default is WAL enabled without per-commit
  `fsync` (`WalNoSync`);
- `Db::open_history_complete`;
- settled Global fixture rows and Global subscriptions;
- the server identity and deterministic row-ID seed;
- by default, a clean close/drop/reopen between seeding and measurement.

The reopen creates a fresh 256 MiB RocksDB block cache. It does **not** evict
the operating-system page cache, so these results are described as
“reopened/fresh Rocks cache,” not cold disk.

## Reproduction

The exact 529,900-document run retained ten subscriptions:

```sh
env \
  JAZZ_SAAS_PROFILE=baseline \
  JAZZ_SAAS_ACTIVE_SUBSCRIPTIONS=10 \
  cargo bench --profile perf -p jazz-tools \
    --features saas-permission-bench,rocksdb \
    --bench saas_persistent_hydration --quiet
```

The same-open control retained one subscription:

```sh
env \
  JAZZ_SAAS_PROFILE=baseline \
  JAZZ_SAAS_ACTIVE_SUBSCRIPTIONS=1 \
  JAZZ_SAAS_PERSIST_REOPEN=false \
  cargo bench --profile perf -p jazz-tools \
    --features saas-permission-bench,rocksdb \
    --bench saas_persistent_hydration --quiet
```

The 100-route persisted fan-out lane used:

```sh
env \
  JAZZ_SAAS_PROFILE=baseline \
  JAZZ_SAAS_DOCUMENTS=10000 \
  JAZZ_SAAS_ORGANIZATIONS=100 \
  JAZZ_SAAS_TEAMS=1001 \
  JAZZ_SAAS_HOT_DOCUMENTS=5000 \
  JAZZ_SAAS_TEAM_MEMBERS_PER_TEAM=5 \
  JAZZ_SAAS_ACTIVE_SUBSCRIPTIONS=100 \
  cargo bench --profile perf -p jazz-tools \
    --features saas-permission-bench,rocksdb \
    --bench saas_persistent_hydration --quiet
```

The fixture has 5,000 organizations, 5,000 teams, 50,000 active team
memberships, and 529,900 documents. Team 0 owns 30,000 documents; every other
team owns 100. The query is the same filtered, ordered Top-100 document list as
the fan-out benchmark. Every initial Reset is checked against the independent
fixture oracle. These runs use the baseline team-membership authorization
branch, not the five-branch real-world permission profile.

The machine and toolchain are the same Apple M1 Max / 32 GiB developer machine
used for the earlier SaaS receipts: macOS 15.6.1 on arm64, Rust/Cargo 1.93.1.
The source baseline was `90c7795a77f02bcb2676b4b650061b6d3d71703f`
plus the benchmark changes in this receipt. Each number below is one optimized
run; the commands assume no additional `JAZZ_SAAS_*` variables are present.

## Reopened server-storage result

Settled fixture creation took 99.23 s and is excluded from every open and
subscription timing. The harness emits the temporary directory size after
dropping the measured database, but persisted size is not treated as a result
in this receipt because the timed run predated that measurement-boundary fix.

| Phase                                 | Wall time | Logical records read |          Rocks block bytes read |
| ------------------------------------- | --------: | -------------------: | ------------------------------: |
| RocksDB open                          |   1.093 s |                    — |                               — |
| Jazz history-complete recovery        |   0.461 s |              590,491 |                        27.18 MB |
| First subscription, 30k-document team |   6.489 s |            2,929,500 |                        40.16 MB |
| Median of ten subscriptions           |   6.024 s |       2,929,500 each | 47.15 MB for subscriptions 2–10 |
| Ten retained subscriptions, total     |  60.862 s |           29,295,000 |                               — |

All ten subscriptions returned the exact expected initial membership. The hot
team returned 100 rows. Each 100-document team returned 75 rows after the
archived/status filters, yet it performed exactly the same 2,929,500 logical
record reads as the hot team.

The first subscription observed 13,032 Rocks block-cache hits and 32,334 block
reads. Subscriptions 2–10 each observed 7,408 hits and 37,956 block reads. The
logical work therefore remains broad after reopening and remains broad for
later bindings; this is not only first-open recovery.

A repeat with destination-level counters attributed all 2,929,500 hydration
reads to `global_current_rows` across 16 ranges. It read **zero**
`global_current_indexes` records. Recovery separately read 589,900
`global_current_rows`, which is one pass over the fixture's live rows, plus 589
transaction/history records and two other records. This strongly localizes the
hydration bottleneck: the current Global plan reads row sources without using
the declared team indexes. The counters do not by themselves identify which
list or policy operator causes each repeated scan.

## Same-open control

The same-open hot-team subscription took 6.149 s and read the same 2,929,500
logical records. The independent reopened sample was 0.341 s (5.5%) slower and
read 40.16 MB rather than 25.40 MB of Rocks blocks.

The observed difference is compatible with reopen/cache effects, but two
separately seeded single samples do not isolate causality. The invariant record
count is the more durable finding: cache warmth cannot repair a non-selective
plan.

## One hundred retained customer subscription hydrations

A second reopened lane used 10,000 documents, 1,001 teams, 5,005 memberships,
and 100 sequentially opened, retained team subscriptions. Team 0 held 5,000
documents; every other team held five.

- RocksDB open took 259.0 ms and Jazz recovery took 15.8 ms (274.8 ms total);
- the first subscription took 258 ms;
- the next 99 had a 76.3 ms median (73.6–79.0 ms through p95);
- total subscription time was 7.832 s;
- every subscription read exactly 78,328 `global_current_rows` and zero
  `global_current_indexes`;
- all 100 initial memberships were exact.

Those subscriptions incurred zero Rocks block reads after recovery on this
small fixture: all relevant blocks were cache hits. Hydration still scaled
with 7,832,800 logical row reads. This isolates the active-customer effect from
physical disk and again shows that later retained bindings do not become
selective. It does not measure concurrent clients, writes, or update delivery.

## Conclusion

The MemoryStorage result was not an artifact of benchmarking the wrong
backend. The server-default RocksDB/Global/history-complete path still performs
millions of logical reads for a Top-100 team page, and sequential initial
hydration work scales approximately linearly with retained bindings.

The immediate priority remains selective Global hydration:

1. route the bound team predicate into the declared Global team index;
2. use an ordered composite source for the filters and descending order;
3. stop after 100 authorized matches;
4. share only genuinely reusable state without turning each binding into
   another full scan.

RocksDB tuning is not the first lever. The same-open control still took roughly
6.15 s and performed the identical broad logical scan.

## Qualifications

- This calls the core `Db::subscribe_for_identity` path directly over the
  serving database. It excludes accepted-subscriber peer result
  bundling/encoding, session/connection work, WebSocket transport, catalogue
  lookup, and client decoding.
- Same-process seeding leaves the OS page cache uncontrolled. A true cold-disk
  receipt needs a dedicated host with controlled cache state or a dataset
  larger than available memory.
- Global fixture setup batches rows and then invokes the same authority
  self-finalization used by the serving path through a `jazz/testing`-only
  helper. Setup latency is not a production ingestion benchmark.
- Rocks performance counters are diagnostic single-run values. Logical Jazz
  storage-read counts and exact result validation are the mechanism evidence.

Tooling friction: a reusable pre-seeded fixture/manifest and a dedicated
cache-controlled host would make repeated cold-start sampling substantially
cheaper.
