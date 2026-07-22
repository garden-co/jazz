# Commit Superlinearity Attribution

Date: 2026-07-21

## Native Repro

Harness: `crates/jazz/examples/commit_superlinearity_native.rs`

Run:

```sh
cargo run -p jazz --example commit_superlinearity_native --release -- 10000
```

The harness uses `Db<MemoryStorage>` directly, stages 500 todo-like inserts per mergeable transaction, and records staging, `MergeableTx::commit()`, and post-commit subscription drain time. It runs two scenarios:

- `unsub`: no live subscriptions.
- `sub`: one live unbounded local subscription over `Query::from("todos")`.

Important verdict: the JS/WASM unsubscribed superlinear curve does **not** reproduce natively. Native unsubscribed commit time stays roughly flat/noisy through 10k rows. That places the large JS/WASM unsubscribed slope outside the generic Rust `Db` mergeable commit path measured here, most likely in wasm/runtime binding, serialization/copying, or JS-facing finalization around the Rust call.

The subscribed native path does grow with table size, but far below the JS subscribed curve. At 5k rows, native subscribed commit was ~166 ms in the 10k run, versus ~1046 ms in the JS/WASM run cited in `/tmp/STRESS_TODO_FINDINGS.md`.

## Repro Curve

Release run on this worktree, max rows 10k:

| batch | rows | native commit ms unsub | native commit ms subbed |
|---:|---:|---:|---:|
| 0 | 500 | 15.785 | 84.044 |
| 1 | 1000 | 12.814 | 111.956 |
| 2 | 1500 | 16.926 | 113.410 |
| 3 | 2000 | 13.619 | 116.932 |
| 4 | 2500 | 15.296 | 124.641 |
| 5 | 3000 | 14.885 | 145.609 |
| 6 | 3500 | 14.290 | 163.613 |
| 7 | 4000 | 14.572 | 179.560 |
| 8 | 4500 | 18.962 | 144.857 |
| 9 | 5000 | 24.676 | 165.581 |
| 10 | 5500 | 16.996 | 205.338 |
| 11 | 6000 | 24.469 | 161.997 |
| 12 | 6500 | 20.709 | 181.047 |
| 13 | 7000 | 25.522 | 176.044 |
| 14 | 7500 | 26.464 | 155.530 |
| 15 | 8000 | 23.699 | 202.431 |
| 16 | 8500 | 16.992 | 243.913 |
| 17 | 9000 | 23.095 | 297.703 |
| 18 | 9500 | 15.634 | 323.508 |
| 19 | 10000 | 21.265 | 319.581 |

## Stage Attribution

Temporary env-gated instrumentation was added to `crates/jazz/src/db.rs` around `MergeableTx::commit()` and then reverted. The split was:

- commit record build
- `NodeState::commit_mergeable_many`
- `Db::finalize_local_commit`
- `Db::refresh_subscriptions`

Additional temporary tracing split local subscription refresh into:

- `NodeState::drain_local_maintained_view_subscription`
- `apply_maintained_update_to_snapshot`
- `subscription_is_settled`

5k instrumented run, selected points:

| rows | scenario | build ms | node commit ms | finalize ms | refresh subscriptions ms | local maintained drain ms | snapshot/delta apply ms |
|---:|---|---:|---:|---:|---:|---:|---:|
| 500 | unsub | 0.451 | 12.515 | 0.000 | 0.000 | n/a | n/a |
| 2500 | unsub | 0.108 | 12.568 | 0.001 | 0.000 | n/a | n/a |
| 5000 | unsub | 0.094 | 12.949 | 0.000 | 0.000 | n/a | n/a |
| 500 | sub | 0.080 | 21.284 | 0.001 | 44.889 | 44.787 | 0.006 |
| 2500 | sub | 0.106 | 22.282 | 0.002 | 74.602 | 53.895 | 20.573 |
| 5000 | sub | 0.092 | 21.562 | 0.001 | 103.317 | 58.376 | 44.796 |

The temporary instrumentation was reverted after measurement.

## Named Mechanisms

Always-there/native unsubscribed slope:

- Not reproduced in native `Db<MemoryStorage>` through 10k rows.
- `MergeableTx::commit()` stayed roughly flat, with `NodeState::commit_mergeable_many` owning almost all of the ~13-25 ms baseline.
- `finalize_local_commit` and `refresh_subscriptions` were effectively zero without subscriptions.
- Therefore the JS/WASM unsubscribed slope is not explained by `crates/jazz/src/node/ingest.rs` or the ordinary native commit/finalize path in this harness.

Subscription-added/native slope:

- Owning public stage: `Db::refresh_subscriptions` in `crates/jazz/src/db.rs`.
- Owning local maintained stage: `NodeState::drain_local_maintained_view_subscription` in `crates/jazz/src/node/query_eval.rs`.
- Owning snapshot/materialization stage: `apply_maintained_update_to_snapshot` in `crates/jazz/src/db.rs`.
- `drain_local_maintained_view_subscription` is already non-flat in the measured local maintained graph path, rising from ~45 ms at 500 rows to ~58 ms at 5k.
- `apply_maintained_update_to_snapshot` grows more directly with accumulated result size, from ~0 ms at 500 rows to ~45 ms at 5k. This is O(current result size + delta) per commit for an unbounded subscription because each 500-row delta is applied against an ever larger materialized snapshot.

Peer maintained path:

- The existing `JAZZ_REHYDRATE_TRACE` in `PeerState::query_update_maintained_subscription_view` did not fire for this local `Db::subscribe` repro.
- This run therefore attributes the native local subscription slope, not the sync peer maintained-view bundling path. Existing trace hooks remain available for a connected peer/native-runtime repro.

## Fix List

1. CLEARLY-GOOD: Add first-class, checked-in phase counters around JS/WASM commit boundaries: JS staged inserts, wasm call entry/exit, Rust `MergeableTx::commit`, native/wasm serialization, subscription refresh/materialization, and callback publish. The largest observed unsubscribed slope did not reproduce in native Rust, so the next fix needs runtime-boundary attribution.

2. CLEARLY-GOOD: Keep the native harness as bench-only coverage for regressions. It makes the Rust baseline visible and prevents conflating Rust core regressions with wasm/runtime regressions.

3. CLEARLY-GOOD: Flatten local subscription snapshot application for unbounded subscriptions. `apply_maintained_update_to_snapshot` has O(current result size + delta) behavior per commit in this harness; applying row-keyed deltas without cloning/rebuilding the whole snapshot would remove the measured native subscription slope.

4. CLEARLY-GOOD: Inspect `NodeState::drain_local_maintained_view_subscription` for per-delta scans over arrangements/result facts. It owns a smaller but real N-dependent subscription cost even before snapshot materialization.

5. SPECULATIVE: Port the same phase counters into the wasm build behind a feature or env flag. This is useful if the JS/native boundary counters still cannot split host-side cost from compiled Rust cost cleanly.

6. SPECULATIVE: Investigate storage-specific behavior with `RocksDbStorage`. The harness used `MemoryStorage` to isolate native core behavior; storage writes were not the reported JS/WASM mechanism, but RocksDB can be checked separately if a native storage-backed repro is needed.

## Tooling Friction

Tooling-friction: a built-in `Db` commit phase receipt, emitted consistently from native and wasm builds, would have avoided temporary source instrumentation and made the native-vs-wasm boundary obvious in one run.
