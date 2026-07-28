# Benchmarks: what we have, what they measure, what's missing

An orientation for anyone picking up performance work. Compiled 2026-07-28 from a
read of the bench sources — not from their names, several of which mislead.

Full detail, with `file:line` for every claim, is in the working notes
(`benchmark-inventory.md`). This is the map.

## The short version

We have plenty of benchmarks. What we don't have is **an answer to "where do we
stand"** — because results land in three unrelated places, and only one of them
keeps history.

| harness                            | output                                                 | survives?                                 |
| ---------------------------------- | ------------------------------------------------------ | ----------------------------------------- |
| `dev/benchmarks/smoke.sh`          | JSONL receipts + `SMOKE_LEDGER.md` with previous/delta | **yes, committed**                        |
| SaaS document/permission harnesses | dated Markdown receipts                                | one committed snapshot, no delta series   |
| Criterion (`cargo bench`)          | `target/criterion`                                     | no — machine-local, dies on `cargo clean` |
| realistic native/browser           | per-scenario JSON, CI artifacts                        | no — artifacts expire                     |

So Groove, Criterion, browser, storage/OPFS and WASM-ingest numbers exist only as
whatever the last person happened to run.

## What each thing actually measures

### Core `jazz` benches — the ones in the smoke gate

| bench                       | measures                                                                                                                     | sensitive to                                                   |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `cold_subscription`         | seeds one row at history depths (1k/5k/10k), times global `current_rows_update` and local `current_rows`                     | historical depth, pending local state, current-row hydration   |
| `validation`                | multi-client exclusive transactions, 1–3 reads + 1–2 writes each; compares core ingest acceptance against a simplified model | client/row/commit counts, hot-row rate, OCC and predicate sets |
| `sync`                      | mixed mergeable/exclusive/skew/delete commits UI→worker→edge→core, with periodic current-row updates back                    | commit mix, view cadence, topology, bundle/reference reuse     |
| `large_value_checkpointing` | one-byte text edits to depth ~300, then materialization with interval checkpoints vs full replay                             | text history depth, checkpoint interval, body size             |
| `merge_back_cost`           | branch creation, ~1k mergeable writes, merge-back, current-row count                                                         | branch write count. **Not** complete offline sync              |

Run: `cargo bench -p jazz --bench <name>`. All five are in `smoke.sh`, so they
have committed history and delta tracking.

### `jazz-sim` scenarios — S1–S7, S9

Named workload simulations (`s1_saas`, `s2_canvas`, `s3_permissions`,
`s4_order_processing`, `s5_durable_stream`, `s6_text_traces`, `s7_migrations`,
`s9_durable_execution`) plus `micro`. In the smoke gate, so also committed.

**S8 does not exist.** The spec reserves it for branch/merge/offline edits;
there's no Cargo target. `merge_back_cost` is a smaller piece of that, and R8 is
a declarative JSON scenario with no runner.

### Groove benches — not in any gate

`scenario` (social feed, ACL, one-shot) and `micro`. Driven by env vars:

```
GROOVE_SCENARIO=social_feed GROOVE_ENGINE=groove cargo bench -p groove --bench scenario --quiet
```

Emit JSON on stdout. Nothing captures it. The Groove spec references a
`scripts/bench_run.py` retention tree that **doesn't exist in this checkout** —
treat that documentation as stale.

### Legacy `jazz-tools` Criterion suite — manual only

`observer_write_path`, `db_benchmark`, `authorization_scope_benchmark`,
`insert`/`update`, `subscription`, and `realistic_phase1` (R1–R13).

`saas_documents` and `saas_permission_fanout` are public-API, JSON-emitting
scale harnesses in the same crate. They cover latest-100 team document reads,
RBAC/direct-ACL policy branches, active-subscription fan-out, write batching,
and approximate maintained-state sizing. Their dated receipts live beside
this overview. The evidence-backed implementation sequence is in
[`plans/PLAN.md`](plans/PLAN.md), with separate plans for correctness, hydration,
write refresh, maintained Top-K, claim rebinding, and lifecycle cleanup. The
follow-up probe methodology and caveats are recorded in
[`SAAS_DEEP_DIVE_RECEIPT_20260729.md`](SAAS_DEEP_DIVE_RECEIPT_20260729.md).

Two things to know before trusting these:

- **They're all `MemoryStorage`** unless noted, so they don't measure the
  persisted read path at all.
- The "batch" cases in `insert`/`update` are _serial insert-and-wait calls_, not
  atomic transactions. Only `subscription` has a genuinely staged 100-insert
  mergeable transaction.

`realistic_phase1` needs `--features rocksdb`. CI runs R1/R2/R3/R4/R9–R12, not R13.

### Browser, storage, WASM

- **Browser realistic** (W1/W4, B1–B6): `pnpm --dir packages/jazz-tools run bench:realistic:browser`. CI scales the workload to **3%**, so these are shape checks, not scale numbers.
- **Native realistic** (W1/W3/W4): `cargo run -p jazz-tools --example realistic_bench -- <scenario>`.
- **Native storage engines** (SQLite/RocksDB/redb raw KV): `cargo bench -p jazz-storage-native-bench`.
- **OPFS B-tree hot paths**: `cargo bench -p opfs-btree --bench hot_paths` — note this uses an **in-memory file, not actual OPFS**. The real one is the WASM worker bench (`run-opfs-bench.cjs`).
- **Jazz WASM probes**: arithmetic, trait dispatch, RefCell, allocation. Runtime characteristics, _not_ Jazz data paths.

## The gaps that matter

Ranked by how much they'd change a decision:

1. **`INV-INC-1` has no performance receipt.** The mechanism law — maintained delivery work bounded by the change, never by view size — is enforced by an exact functional canary (one parent, 20k children, insert one, allocations within 3×). That proves the property at _one point_. No slope, no curve, no number to set a target against. Given how much recent work is justified against this invariant, it's the first gap to close.
2. **No end-to-end persisted realistic-scale read receipt.** Legacy suites are in-memory, browser CI runs at 3%, and native R3 reopens its own temp RocksDB without controlling OS cache. We have no honest cold-read number.
3. **S8 missing** (branch/merge/offline).
4. **Policy cost now has only a local in-memory snapshot.** The SaaS fan-out
   receipt measures ordinary writes reaching authorized readers through team,
   organization-admin, and direct-ACL paths, but not persisted storage, sync,
   reconnect, or policy churn.
5. **S5–S7 missing promised dimensions**: remote resume / evicted prefix (S5), full-history memory (S6), native-vs-lens tax and migration waves (S7).
6. **No peer-known-state / payload-dedup scale sweep** for normal sync.

## Where targets should come from

The spec states performance properties that nothing currently enforces. These
are the natural source of acceptance targets:

| property                                   | what exists                                      | what's missing                                                         |
| ------------------------------------------ | ------------------------------------------------ | ---------------------------------------------------------------------- |
| `INV-INC-1` bounded incremental delivery   | landing canary, S1 fanout counters               | no scale curve, numerical bound, or gate                               |
| PERF-4 known-state payload dedup           | S1/sync emit bundle and reference counters       | no controlled known-state cardinality sweep                            |
| PERF-5 maintained converges to rehydrate   | correctness/oracle work                          | no maintained-vs-rehydrate cost/bytes comparison over view scale       |
| PERF-7/8 current reads are O(current rows) | `cold_subscription`, `large_value_checkpointing` | no retained baseline or slope threshold; filtered/indexed reads absent |
| S4 post-acceptance propagation is O(delta) | S4 emits settlement/propagation phases           | no fixed-delta / varying-view regression rule                          |

## Known documentation rot

- The bench README says R13 is unregistered; the source registers it.
- `C_performance.md` says S4's phase split is incomplete; the source emits both phases.
- Groove's spec references a retention script tree that isn't in this checkout.
- `opfs-btree/BENCHMARK_OVERVIEW.md` maps historical figures to paths that have moved.

## Suggested order of work

1. **Unify receipts before running anything.** Have Criterion, browser, storage and Groove targets emit a smoke-shaped receipt alongside their native output — name, metric, value, unit, workload params, commit, dirty flag, timestamp. The `SMOKE_LEDGER.md` schema already does previous/delta; reuse it rather than inventing one. Small adapter per harness, not a rewrite.
2. **Then run everything once** to establish a baseline that's actually comparable run-to-run.
3. **Then set targets**, taking them from the stated-properties table above rather than from whatever the current numbers happen to be.

Doing 2 before 1 produces three incomparable snapshots and no baseline.
