# StagePlan benchmarks

StagePlan is a task board for putting on a show. Its benchmarks own the
**task list** (authoring, bulk updates, reopen), the **show board** (bounded
board, task-detail and activity reads, live filtered views), the **crew
dashboard** (many small permissioned subscriptions) and **row-dependent
policies** (update, subscribe, archive, restore, offline resume).
[metadata.ts](metadata.ts) is the structured source for each case's
description, timing boundary and throughput unit; update it with the harness.

The package merges two former suites: the todo example's native workload
(`jazz-example-todo-benchmark`, now `src/tasks/`) and the W1 team task board
(`jazz-example-benchmark-w1`, now `src/board/`). Renamed cases start a new
CodSpeed history; the workloads and their correctness checks are unchanged.

## CodSpeed cases (`benches/walltime.rs`)

| Case                                                   | Former case                                                |
| ------------------------------------------------------ | ---------------------------------------------------------- |
| `stage_plan_add_task_1350`                             | `sequential_insert_1350_rocksdb`                           |
| `stage_plan_check_off_task_1350`                       | `sequential_update_1350_rocksdb`                           |
| `stage_plan_bulk_complete_1350`                        | `batch_update_1350_rocksdb`                                |
| `stage_plan_reopen_1500`                               | `reopen_1500_rocksdb`                                      |
| `stage_plan_open_board`                                | `query_board_profile_s_rocksdb`                            |
| `stage_plan_open_task_detail`                          | `query_task_detail_profile_s_rocksdb`                      |
| `stage_plan_activity_page[30000]`                      | `query_bounded_activity_page_scaling_rocksdb[30000]`       |
| `stage_plan_move_card_to_done`                         | `subscribe_activity_intersection_delta_rocksdb`            |
| `stage_plan_crew_dashboard[(600, 60)]`, `[(6000, 60)]` | `subscription_fanout_memory[...]`                          |
| `stage_plan_update_under_policy[9000]`                 | `update_activity_policy_scaling_memory[9000]`              |
| `stage_plan_subscribe_under_policy[9000]`              | `subscribe_activity_policy_point_scaling_memory[9000]`     |
| `stage_plan_archive_task[9000]`                        | `delete_task_point_scaling_memory[9000]`                   |
| `stage_plan_restore_task[9000]`                        | `restore_task_point_scaling_memory[9000]`                  |
| `stage_plan_resume_after_offline[(500, 2000, 1500)]`   | `resume_one_task_update_scaling_memory[(500, 2000, 1500)]` |

## Nightly cases (`benches/nightly.rs`)

Scaling sweeps that explain a cost but do not need a receipt on every PR keep
their former W1 names: bounded activity page (profile S, 9,000 RocksDB, and the
memory sweep), comments scaling, indexed update without a subscription, point
subscribe without a policy, the 900-row policy attach and resubscribe. The
nightly CodSpeed run measures them on main; the benchmark smoke gate compiles
them on every PR; run them locally with
`cargo bench -p jazz-example-stage-plan-benchmark --bench nightly`.

Dropped because a CodSpeed case above measures the same operation: the
in-memory twins of the board, task-detail, profile-S activity page and
intersection-delta reads; `update_activity_policy_no_subscription_memory` and
the 900-row policy update, archive and restore points; the smaller resume
point; and the 0- and 10-list crew dashboards.

## Task list

`src/tasks/` owns the former native batch-phase workload of the local-first
todo example: its schema, deterministic rows, driver, assertions, and profiling
support. The four wall-time cases finish with 1,500 tasks:

- Reopen (`stage_plan_reopen_1500`): open an already seeded RocksDB worker, publish to an empty memory
  foreground, ingest, and query the tasks.
- Bulk complete (`stage_plan_bulk_complete_1350`): mark 1,350 tasks done in one transaction, including foreground
  authoring, upload encoding, worker ingest and delivery back to the foreground.
- Check off one at a time (`stage_plan_check_off_task_1350`): do the same as 1,350 separate transactions.
- Add one at a time (`stage_plan_add_task_1350`): start with 150 seeded, subscribed tasks and insert 1,350
  new unfinished tasks in separate transactions, growing to 1,500. Each insert
  completes the same foreground author/persist, upload, worker ingest/publication,
  encode/decode and foreground application path before the next insert. IDs are
  deterministic caller-supplied IDs; new rows have no predecessor version.

Setup is outside each measured closure. RocksDB uses WAL without fsync. This is
an in-process native worker/foreground workload, not browser or IndexedDB timing;
it excludes JS scheduling and authentication bootstrap. Each measured operation
ends with one read of all tasks, including after the entire sequential loop.
Exact row-ID/completed-ID verification happens when Divan drops the fixture,
outside timing. The diagnostic profile retains its per-delivery verification.

```sh
cargo test -p jazz-example-stage-plan-benchmark --lib tasks::
cargo bench -p jazz-example-stage-plan-benchmark --bench walltime --features jazz-benchmark-guard/mimalloc
cargo run -p jazz-example-stage-plan-benchmark --bin stage-plan-tasks-profile --profile perf --features jazz-benchmark-guard/mimalloc
JAZZ_TODO_WORKLOAD=sequential-insert cargo run -p jazz-example-stage-plan-benchmark --bin stage-plan-tasks-profile --profile perf --features jazz-benchmark-guard/mimalloc
JAZZ_TODO_WORKLOAD=sequential-update cargo run -p jazz-example-stage-plan-benchmark --bin stage-plan-tasks-profile --profile perf --features jazz-benchmark-guard/mimalloc
```

The profile binary reports the existing per-phase counters plus a raw storage
baseline. `JAZZ_BATCH_ROWS` and `JAZZ_BATCH_UPDATE_PERCENT` configure it; the
CodSpeed cases have fixed sizes. Keep allocator, optimization profile and machine
architecture equal when comparing numbers. CodSpeed ARM64 results establish a
separate baseline from local x86 measurements.

`JAZZ_TODO_WORKLOAD` defaults to `batch` (the original memory/RocksDB diagnostic
and raw-storage controls). Sequential diagnostics use RocksDB and retain the
existing `batch_*` phase labels for each one-row transaction. Their `rows` field
is the current table size, allowing phase costs to be grouped by growing table
size. `JAZZ_BATCH_ROWS` is the final size for inserts (20..=4096); setup seeds
one tenth of it. Sequential updates honor `JAZZ_BATCH_UPDATE_PERCENT` (default
90). Diagnostic per-delivery verification and printing add work and must not
be compared to clean Divan latencies. Both timed sequential cases do 1,350
transactions, but inserts grow the scope whereas updates keep 1,500 rows
throughout; their timings are not a pure insert-vs-update primitive comparison.

## Hot-row history diagnostic (#2981)

`stage-plan-tasks-history-depth` keeps the same todo schema, RocksDB WAL worker, memory
foreground and complete wire roundtrip. It compares `spread` (round-robin
updates) with `hot` (one repeatedly edited row). Both explicitly name the last
authored version of the target row as parent. Optional `stale-parent` instead
names the seed every time: this intentionally creates concurrent siblings and
must not be confused with sequential history depth. Every edit changes the
title and toggles `done`; exact IDs and values are checked after each reporting
window and after reopening the worker and rehydrating a fresh foreground.

```sh
cargo build -p jazz-example-stage-plan-benchmark --bin stage-plan-tasks-history-depth --profile perf
JAZZ_HISTORY_ROWS=1500 JAZZ_HISTORY_UPDATES=2000 JAZZ_HISTORY_WINDOW=500 \
  JAZZ_HISTORY_ARMS=spread,hot target/perf/stage-plan-tasks-history-depth
```

JSONL windows report phase medians and average logical storage/history reads
per update, plus source and process provenance. Authoring and author persistence
are memory-backed; worker ingest/publication is RocksDB-backed. `upload` and
`publish` include their wire encode/decode. `roundtrip` includes all phases and
measurement bookkeeping, not window validation/printing or fixture setup.
These are diagnostic timings, not CodSpeed cases or browser throughput claims.
The existing four walltime cases and their assertions are unchanged.

## Show board

The executable query shapes and workload methods live in `src/board/mod.rs`;
the measured closures live in `benches/walltime.rs` and `benches/nightly.rs`.

These are individual W1-derived operations, not the complete historical mixed
scenario. In particular, task detail performs two reads but is one task-detail
operation, activity table size is not query output size, and a reconnect after
one change is not necessarily a delta-sized response. Keep metadata and the
timer boundary in agreement when changing the harness.

### Crew dashboard: permissioned subscription fan-out (#3231)

`src/board/crew_dashboard.rs` generalizes a dashboard startup pattern: one broad
overview and many independently mounted keyed lists over the same table. Public
builders define teams and tasks with inherited SELECT permission. The overview
has no tenant predicate; the second team's equally sized dataset must be excluded
by authorization. Keyed lists use 60 fixed boards independently of row count.

The topology is three real Jazz runtimes: history-complete Core, scope-isolated
relay and non-durable foreground. Logical in-process transport isolates engine
cost; it is **not** a wire, browser, IndexedDB, React or disk-reload benchmark.
All subscriptions open before pumping, remain live together and consume reset
and delta events until settled. Seeding, runtime creation, connections and teardown
are outside timing. Query preparation, admission and hydration are inside.

CodSpeed measures 600 and 6,000 rows/team with 60 keyed lists
(`stage_plan_crew_dashboard`); the standalone runner below takes any list count. Row count and binding
count are independent axes. Total stored tasks are twice the rows/team argument.

Standalone attributed receipts (prepare, subscribe, Core/relay/foreground ticks,
total and pump turns) run with:

```sh
cargo run --profile perf -p jazz-example-stage-plan-benchmark --bin stage-plan-crew-dashboard -- 600 60 3
cargo test -p jazz-example-stage-plan-benchmark --lib board::
```

The standalone runner validates exact membership and payloads after timing.
The correctness test also verifies live edits and inherited permission revocation.
The timed benchmark does not replace those tests. Preserve exact source/binary
hashes for comparisons; busy-host local timings are diagnostic, not stable claims.

Experiment premise: previous #2853/#2861 compiler-memo trials did not establish
useful endpoint gains on their cold-load workloads. This workload instead exposes
many independently admitted bindings and measures the complete three-runtime
opening. Any new reuse must preserve identity, claims, read view, catalogue and
receiver scope; disappearance of a compiler frame alone is not acceptance.

The common [metadata contract](../../../dev/benchmarks/metadata/README.md) explains
revision provenance, legacy names, work units and validation.
