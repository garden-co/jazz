# jazz — Specification · Appendix C. Performance

## Overview

_Non-normative (guidance)._ This appendix defines the discipline for making
performance claims, the properties that keep steady-state work bounded, and the
optimization levers that matter most. `INV-PERF-*` are measurement-discipline
anchors. Detailed scenario specs live in appendix B; this appendix is about _how
to reason about the numbers_ and what to optimize.

Invariant digest:

- `INV-PERF-1`: A performance claim MUST identify its workload, source state, and before/after measurements.
- `INV-PERF-2`: Correctness is part of benchmark validity: deterministic counters or oracle checks are hard gates, and a benchmark that gets faster by changing results MUST fail.
- `INV-PERF-3`: Benchmark numbers MUST be interpreted against declared comparison anchors.
- `INV-PERF-4`: Steady-state peer updates MUST preserve payload deduplication and incremental result delivery.
- `INV-PERF-5`: Incremental subscription state MUST converge to the equivalent full-rehydrate state.
- `INV-PERF-6`: Current-row optimizations MUST preserve deletion and restoration visibility, including the evidence needed to establish it.
- `INV-PERF-7`: Optimized current-row reads and query evaluation MUST remain semantically equivalent to visible current rows.
- `INV-PERF-8`: Cold current-only hydration and current-view witness sourcing MUST scale with visible current rows rather than history depth.
- `INV-SYNC-24`: Known-state payload deduplication MUST omit only version bodies and MUST preserve membership, program facts, and inventory references.

## Details

### C.1 Discipline

Performance numbers are only useful when the workload, validity checks, and
comparison points are explicit. A performance claim cites a scenario or
microbench, its workload parameters, the dirty-tree status, and before/after
numbers (`INV-PERF-1`). Correctness is part of benchmark validity:
deterministic counters and oracle checks are hard gates, and a run that gets
fast by changing results fails (`INV-PERF-2`, appendix B). Numbers are read
against declared anchors: latency floor, bytes floor, naive ceiling or reference
implementation, topology/profile, and durability (`INV-PERF-3`).

### C.2 Steady-state sync

Steady-state sync is designed to avoid resending or recomputing state that has
not changed, while preserving the same visible results as a full rehydrate. The
hot path therefore holds three properties that are both correctness and
performance requirements: per-peer complete-tx payload dedup with result-set
deltas (`PeerState.shipped_complete_tx_payloads`, `PeerMetrics`) (`INV-PERF-4`);
incremental subscription state converging to a full rehydrate for both filtered
query bindings and whole-table current-row views (`INV-PERF-5`); and current-row
optimizations preserving deletion/restore visibility including register
witnesses (`INV-PERF-6`). The committed successor to complete-tx dedup is the
known-state design (ch. 8 §8.11, `INV-SYNC-24..27`, target): version-granular,
declaration-seeded, ack-free — it retires `shipped_complete_tx_payloads` when
it lands.

**Implementation status (verified).**
`incremental_query_result_sets_match_full_rehydrate_after_seeded_commits`
covers incremental/full-rehydrate equivalence. The current payload-inventory
representation is `PeerState::shipped_complete_tx_payloads`; the known-state
successor remains an implementation transition, not an invariant condition.

A **full-diff full recompute is sometimes correctness-preserving, not a failure**. For
example, a permission change can make an old exclusive transaction newly
visible, and the test expects exactly one `full_diff_recomputes_out`. Large reset
rehydrates deliberately avoid a duplicate groove hydration and full-diff from
stored peer state thereafter. There is no `LARGE_REHYDRATE_RESULT_ROWS` constant.

### C.3 Current-row reads

Global current-row reads are served from the compact representation of visible
current rows, rather than by replaying history. `DurabilityTier::Global`
current-row reads and global current-row query graphs read the **overwrite
global-current tables**, not a history argmax graph, while remaining
semantically equivalent to visible current rows (`INV-PERF-7`,
`visible_current_graph` / `write_global_current_update`, ch. 14). This makes
cold current-only hydration **O(current rows), not O(history depth)** for
degenerate whole-table global shapes (`INV-PERF-8`) by routing global
current-row subscriptions through the global-current tables (receipt:
`benches/cold_subscription.rs`).

**Implementation status (verified).**
`denormalized_current_content_witness_projects_history_provenance_to_unix_milliseconds` verifies
the current-sourced witness bytes, and
`maintained_relation_include_single_row_changes_are_scale_independent` is the
canonical scale-independence canary.

Winner metadata needed for sync payload witnesses and known-state dedup is also
part of the compact current representation: current content/register rows carry
the canonical version payload metadata (`schema_version`, `parents`, provenance)
and the settle position (`global_time` where fated). Version witnesses are sourced
from current rows, not a current-key → history join, and the current-sourced
payload is byte-equivalent to the canonical history projection.

Receipt, dirty-tree Plan 3 denormalization run (`JAZZ_DEPTHS=100,1000
JAZZ_PENDING_SIZES=0 cargo bench -p jazz --bench cold_subscription`): global
current-row update wall time changed from 1.589 ms at depth 100 and 7.481 ms at
depth 1000 to 0.986 ms and 0.945 ms. This receipt measures the current-row path,
not historical history-read removal directly; both runs already reported zero
history row reads in this phase.

### C.4 Levers and hot spots

The main performance levers are the places where repeated work still scales with
the table, the shape, or a per-call derivation instead of the actual change.

- **S4 post-accept propagation.** The harness emits _two_ measurements:
  settlement throughput and propagation-inclusive throughput. Per-commit fan-out is
  intended to be O(delta), not O(table). The relay whole-table full recompute case has
  been fixed (degenerate system whole-table views stay incremental);
  filtered/join/edge-client views still take the conservative full-diff full recompute
  on exclusive-sibling drains — measure whether that path needs delta-sizing
  too.
- **Cross-scenario levers** (from profiling): avoid per-call
  `prepared_query_plan` lookup/deep hashing; intern result-set table keys (~25%
  of the S1 profile); cache alias handles per peer/ingest scope; batch closure
  expansion.
- **S4 ledger levers:** cache draft row winners for `tx_write`, reduce the
  client double-pass at pending ingest and global finalize, re-measure
  `tx_read`.

### C.5 Measurement honesty

Measurement categories stay separate so that a slow propagation path is not
mistaken for a slow settlement path. The S4 "throughput regression" was a
measurement conflation: retained baselines included per-commit peer refresh (~23
tx/s) while refresh-suppressed engine throughput was much higher; the real issue
is propagation fan-out, not settlement. Gates (`[needs: payload-inventory]`)
stay _visibly_ gated, never silently
counted as measured.

### C.6 Developer feedback throughput (implementation guidance)

Compile, lint, and test latency is an implementation constraint: slow feedback
reduces the number of correctness experiments and adversarial checks that fit in
an engineering cycle. This section is non-normative and does not weaken any
runtime, compatibility, or merge invariant. Changes to the feedback pipeline
must retain an equivalent complete merge gate and must be justified with
before/after GitHub job and step timings under `INV-PERF-1`.

The optimization order is:

1. remove duplicated or release-only work from correctness jobs;
2. execute independent artifact builds and test partitions concurrently;
3. make artifacts and compiler results safely reusable by source fingerprint;
4. reduce compilation and test invalidation domains with deliberate boundaries;
5. consider process or language boundaries only after measured pipeline waste is
   removed.

The August 2026 #1348 receipts identify two critical paths. `test-ts` took
10m46s: 6m52s in `build:ci`, 42s rebuilding NAPI after it had already been
built, and 1m57s in Node/browser tests. Its release WASM task took 3m37s, of
which 1m23s was Rust compilation and the remainder was binding/`wasm-opt`
work. NAPI compilation then took about 2m07s. `test-rust` reached its 15-minute
job limit after spending about 2m40s compiling `cargo-nextest`, 1m55s compiling
workspace tests, and roughly nine minutes executing a timeout-heavy suite.
These receipts make orchestration and test completion the first targets, not a
core-language rewrite.

The near-term implementation plan is:

- install pinned prebuilt test/build tools instead of compiling them per job;
- remove duplicate Clippy and duplicate named-test execution;
- give setup, compilation, and execution coherent separate budgets and retain
  structured partial receipts on failure or cancellation;
- use the existing fast, unoptimized WASM artifact for correctness tests while
  retaining optimized WASM validation in package/publish workflows;
- build NAPI once, verifying it before any conditional repair;
- run independent WASM and NAPI builds concurrently;
- shard the complete Rust inventory after removing per-shard setup taxes, then
  evolve from deterministic hash partitions to receipt-driven balanced shards;
- preserve fast PR diagnosis and a complete merge gate rather than silently
  skipping coverage.

**Implemented first step (2026-08-11).** `test-rust` installs the pinned
prebuilt `cargo-nextest` release instead of compiling it from source; repeated
#1348 GitHub runs measured that source build at **115–169 seconds**. In the
main CI workflow, the `wasm-pack` installer now runs only in `test-ts`, the
sole job which invokes the WASM packager (the previous source install there was
about **20 seconds**), and the lint job relies on the one workspace Clippy
invocation already contained in `pnpm lint` rather than running it again. These
changes retain the same test selection and lint command; expected savings are
roughly **two minutes** from the Rust test job plus one complete duplicate
workspace-Clippy pass and the unneeded installer work in the Rust-only jobs.
Follow-up receipts must replace these estimates with before/after step timings.

Cache correctness precedes cache scale. Turbo task inputs should describe each
artifact's actual dependency closure rather than all `crates/**/*.rs`; native
artifacts remain content-addressed and provenance-checked. CI reports Turbo and
`sccache` hit rates so a cache claim is observable. Active worktrees keep
separate Cargo target directories to avoid lock contention and share compiled
work through `sccache`. Optional fast linkers and local caches are detected
accelerators, never prerequisites for colleagues outside the devbox setup.

If those changes leave Rust invalidation as a major local bottleneck, prefer a
small number of acyclic capability boundaries over microcrates. The first
candidate is a lean Jazz engine (model, protocol, query, node, database, peer,
and codecs) beneath native-only server, CLI, admin, and storage shells, with the
existing `jazz` crate retaining a compatibility facade. A boundary is accepted
only when measurements show that representative edits avoid rebuilding an
unrelated outer layer.

The binding architecture should also be tested against a thinner endpoint. A
portable WASM runtime may serve browsers, ordinary Node consumers, and most
cross-platform tests, while an optional thin NAPI host supplies only capabilities
that require native storage, server integration, threading, or demonstrated
performance. This is a measured architecture spike, not a commitment to remove
NAPI: the test must compare capability coverage, artifact/build latency, runtime
performance, and packaging complexity. Rewriting the engine in another language,
introducing an unstable Rust dynamic-library boundary, or migrating build systems
does not precede these lower-risk measurements.

### In flight & measured receipts (non-normative)

_C.1–C.6 above are the durable performance discipline. The following is the live
performance backlog with measured receipts, profile shares, and accepted
residuals, from the former `PERF.md`._

Measured optimization opportunities, each with evidence and expected scope.
Convention: nothing lands here without a profile or bench line behind it;
nothing leaves without a receipt (before/after bench lines in the commit).
Companion to [B_benchmarks.md](B_benchmarks.md); retained baselines under
`benchmarks/results/jazz/`.

### P0: S4 post-accept propagation fan-out is O(table) (resolved diagnosis 2026-06-12 late)

Triage verdict: **no regression existed.** The historical 268 tx/s was
measured by the original S4a bench, which had no per-commit peer
refresh; retained baselines from the re-baseline era already show
~23 tx/s with refresh present. Per-iteration breakdown on HEAD:
settlement 2.5ms, subscription view-update fan-out **50.6ms** (2 peers
× 8 tables of `current_rows_update` per accepted commit, growing with
ORDERS/ORDER_LINES size). Refresh-suppressed engine throughput:
**391 tx/s** (current mix) / **497 tx/s** (old mix) — the engine got
faster, the wall number measures propagation.

Two work items:

1. **Bench split (honesty)**: S4 reports settlement throughput and
   propagation-inclusive throughput as separate lines; the conflated
   number retires.
2. **The real engine P0**: per-commit propagation must be O(delta),
   not O(table). Determine whether S4's peers ride the incremental
   receiver path at all, and why incremental cost grows with table
   size (candidates: closure expansion per output row — item 4 below;
   exclusive-finalize forcing full evaluation; whole-table degenerate
   shape lacking delta translation). This is now the top engine lever
   for the S4-vs-SQLite gap.

### Cross-scenario (profiled 2026-06-12, post argmax/append-only arc)

1. **Plan handles instead of per-call plan lookup.** `prepared_query_plan`
   is 12.6% (S4) / 11.3% (S2) / 6.6% (S1) self-time — plans are re-derived
   or deep-hashed per call (corroborated by `SipHasher::write` ~1.3% in all
   four scenarios). Shapes are already content-addressed: cache by the
   precomputed Copy `ShapeId`, or hand callers a plan handle once at
   prepare/subscribe time. Single biggest common win; S4's read path is the
   main beneficiary.
2. **Interned table keys in result set entries.** S1 spends 18% dropping
   `(String, RowUuid, TxId)` tuples plus ~12.6% BTree node churn — result set
   entries clone table-name Strings per entry; the interning pass never
   reached them. Switch to the `Intern<String>` Copy handle. ~25% of S1's
   profile.
3. **Alias handles.** `ensure_node_alias` at 8.2% (S4) / 5.0% (S1) — alias
   interning probed per version encode/decode; cache the handle per
   peer/ingest scope.
4. **Closure expansion batching.** `expand_join_closure_for_output` 7.4%
   (S3) / 2.7% (S2) / 2.0% (S1): per-output-row queries; batch per delta
   group.

### S4 ledger levers (from the per-phase instrumentation)

5. **Draft parent-cache**: `tx_write` re-derives the parent the app just
   read (~39µs/write); cache row→winner in the open draft. ~250µs/tx.
6. **Client double-pass**: pending-ingest at commit + global finalize at
   fate are two full per-row passes at the client; batch or fold. ~1.5–2×
   on client-side work.
7. **`tx_read` floor**: 51µs each vs ~5µs achievable (post plan-handle fix,
   re-measure; covered reads are now two seeks + compare).

### Residuals / accepted-for-now

- Historical Plan-1 receipts: tick runtime stats
  were split into cheap always-on counters plus explicit expensive arrangement
  walks; the S3 permissions smoke receipt moved from **12.597s** before the split
  to **0.893s** in the first post-cleanup smoke run (`20260702T000844Z`, dirty
  git `18e31f13a`). The historical execution-only run measured S3 smoke at
  **1.262s** after a **280.686s** prebuild; that mismatch is why timing is now
  owned by CodSpeed rather than a local omnibus script.
- RocksDB baseline configuration landed in groove: the groove crate now declares
  its own `lz4` and `zstd` RocksDB features; the adapter configures block-based
  bloom filters (10 bits/key), a shared 256 MiB LRU block cache, a shared 256 MiB
  write-buffer manager, LZ4 for upper levels, and zstd for the bottommost level.
  This was feature alignment, not a new workspace artifact feature set.
- The staged storage overlay now has a one-seek `last_with_prefix` fast path when
  no staged delete exists under the prefix; reverse prefix scans stream/merge
  staged and base entries rather than materializing the whole base prefix first.
- The smoke CPU profile baseline (`20260702T005457Z`) is intentionally
  smoke-sized. Its top self-time tables are useful for spotting fixed costs
  (notably RocksDB write/open costs) but are not medium-scale hotspot claims;
  hotspot claims still require medium-size runs under `INV-PERF-1`.
- S2 receipt: ~8.5ms p95 vs 1ms floor (re-measured 2026-06-12 after the
  delta-fold fix d942e57; originally 12ms) — itemized in per-stage
  histograms (link timer-worker granularity ~5ms is harness; rest is
  real service).
- Threaded-driver scheduling overhead ~1ms (noted in BENCHMARKS.md; revisit
  when canvas targets sub-ms floors).
- Whole-table refresh recompute on requested refreshes (delta-suppressed in
  steady state; recompute only on reset/rehydrate).
- micro `domination_winner_probe` p99 ~160µs (reverse-seek tail; watch).
- S3 `query_rows` profile share is inflated by bench-oracle evaluation;
  trust the cross-scenario items, not S3's absolute shares.

## Open Questions

- 🔶 [#1787](https://github.com/garden-co/jazz/issues/1787) — Performance measurement and benchmark migration.
- 🔶 [#1776](https://github.com/garden-co/jazz/issues/1776) — Propagation/access-path implementation and query performance.
