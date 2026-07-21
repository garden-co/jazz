# TS dedup phase 2

## Scope

Phase 2 moves root subscription delta ordering out of the TypeScript adapter and
into the core subscription stream. The peer `SyncMessage::ViewUpdate` wire shape
remains member-grained in this slice; the changed byte shape is the wasm/native
subscription row payload consumed by `packages/jazz-tools`.

## Implementation notes

- `crates/jazz/src/db.rs` now attaches `PositionedSubscriptionDelta` metadata to
  `SubscriptionEvent::Delta` when root positions are available. Added/updated
  rows carry post-delta indexes; removed rows carry pre-delta indexes.
- Relation/include deltas intentionally do not get positioned metadata yet. A
  first attempt cloned the full relation snapshot before applying every
  maintained update and failed `INV-INC-1`; the final code scopes positional
  snapshot comparison to root-only updates with no relation edge payload.
- `crates/jazz-wasm/src/lib.rs` encodes native subscription rows as
  `{ row_id, index, deleted, raw }` and removed rows as
  `{ table, row_id, index }`. When core positioned metadata is present, wasm uses
  it; otherwise reset/snapshot-style payloads fall back to ordinal indexes.
- `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts`
  applies native row positions directly in `applySubscriptionDeltaWithWireDelta`
  and disables the plain-subscription snapshot refresh path. This removes the
  per-commit `db.all` refresh loop for native/root deltas.
- The TS semantic delta functions remain for reset/snapshot/relation paths in
  this slice, but the native root delta path no longer computes indexes or
  add/update/remove classification from row-array diffs.

## Tests added

- `NativeRuntimeAdapter server transport > applies core-provided native subscription positions without JS sorting`
  proves native subscription deltas reduce according to core-provided row indexes
  and asserts the legacy TS semantic diff helper is not invoked for the
  maintained-capable path.
- `NativeRuntimeAdapter server transport > does not run db.all snapshot refresh cycles for maintained native subscriptions`
  keeps the disabled snapshot-refresh path covered: one live maintained
  subscription plus five public inserts produces zero `db.all` refresh reads.
- `NativeRuntimeAdapter TS adapter perf canary > measures mergeable commit slope with and without one maintained subscription`
  is skipped by default behind `JAZZ_TS_ADAPTER_PERF=1`. It drives 500-row
  mergeable transaction batches through the wasm runtime to 5k rows, once with no
  subscription and once with one live unbounded subscription.

## Commit-Slope Curves

The exclusive-transaction version of this harness did not produce a usable
receipt: the first current-tree run timed out at 60s, then at 300s, before
printing a complete curve. The checked-in canary uses mergeable transactions,
which match the adapter's public `beginTransaction`/`commitTransaction` path and
avoid exclusive staging reads dominating the receipt.

Phase-1 parent: detached throwaway worktree at
`codex/ts-dedup-query-semantics` / `2a126fe54`, with the same skipped canary
patched into that worktree only. Parent setup required `wasm-pack build --target
web --release` in `/tmp/jazz_phase1_perf/crates/jazz-wasm` (exit code 0).

| rows | parent no sub ms | phase 2 no sub ms | parent one sub ms | phase 2 one sub ms |
| ---: | ---------------: | ----------------: | ----------------: | -----------------: |
|  500 |           29.325 |            31.322 |           120.855 |            108.959 |
| 1000 |           86.075 |            89.367 |           208.448 |            183.375 |
| 1500 |           80.654 |            82.535 |           227.679 |            188.871 |
| 2000 |           79.333 |            81.125 |           247.550 |            202.617 |
| 2500 |           76.059 |            75.033 |           269.130 |            210.315 |
| 3000 |           73.055 |            74.627 |           298.444 |            228.060 |
| 3500 |           70.700 |            74.655 |           312.274 |            234.672 |
| 4000 |           70.851 |            71.486 |           348.077 |            254.708 |
| 4500 |           67.731 |            67.019 |           355.225 |            235.705 |
| 5000 |           63.854 |            63.434 |           367.894 |            275.095 |

Receipt interpretation: this mergeable transaction harness does not reproduce
the historical no-subscription 20ms-to-695ms slope on either branch; both
branches stay roughly flat after the first 1k rows. With one live unbounded
subscription, phase 2 materially lowers the 5k-row commit batch from 367.894ms
to 275.095ms, about 25% lower in this run, but the subscribed curve still rises
with result size.

## Receipts

- `cargo check -p jazz -j 2`: exit code 0.
- `cargo check -p jazz-wasm -j 2`: exit code 0.
- `cargo test -p jazz -j 2`: final exit code 0. Earlier iterations caught a
  missing enum field in `warm_reopen_differential` and the relation/include
  snapshot clone described above.
- `cargo test -p groove -j 2`: exit code 0.
- `cargo test -p jazz-tools --features test -j 2`: exit code 0.
- `cargo test -p jazz-server -j 2`: exit code 0.
- `cargo check -p jazz-sim --benches`: exit code 0.
- `cargo test -p jazz --test incremental_delivery_canary -j 2`: exit code 0
  for all three canaries.
- `cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact`: exit code 0 after scoping positions to root-only updates.
- `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle -j 2`: exit code 0.
- `pnpm --dir packages/jazz-tools exec vitest run --config vitest.config.ts src/runtime/native-runtime/runtime.test.ts`: first runnable iteration exit code 1 due one old handwritten row-batch fixture; second iteration exit code 0.
- `pnpm --dir packages/jazz-tools exec tsc --noEmit`: exit code 0.
- `dev/gates/ts-wire-codec.sh`: first iteration exit code 2 due stricter nullable test assertion; second iteration exit code 0.
- `cargo fmt --check -p jazz -p jazz-wasm`: final exit code 0 after running
  `cargo fmt -p jazz -p jazz-wasm`.
- `pnpm exec oxfmt crates/jazz/SPEC/8_sync_protocol.md crates/jazz/SPEC/16_maintained_subscription_views.md dev/TS_DEDUP_PHASE2.md packages/jazz-tools/src/runtime/native-runtime/native-row-codec.ts packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts packages/jazz-tools/src/runtime/native-runtime/runtime.test.ts`: exit code 0.
- `wasm-pack build --target web --release` in `crates/jazz-wasm`: exit code 0.
- `dev/benchmarks/smoke.sh`: exit code 0. Ledger appended at
  `dev/benchmarks/SMOKE_LEDGER.md`; result directory
  `dev/benchmarks/results/20260721T224804Z`.
- `JAZZ_TS_ADAPTER_PERF=1 pnpm --dir packages/jazz-tools exec vitest run --reporter verbose --config vitest.config.ts src/runtime/native-runtime/runtime.test.ts -t "measures mergeable commit slope"` on phase 2: exit code 0.
- `JAZZ_TS_ADAPTER_PERF=1 ... vitest ... -t "measures mergeable commit slope"`
  in `/tmp/jazz_phase1_perf/packages/jazz-tools`: exit code 0 after building
  parent wasm artifacts.

## Receipts-Only Follow-Up

- `pnpm --dir packages/jazz-tools exec vitest run --config vitest.config.ts src/runtime/native-runtime/runtime.test.ts`: exit code 0 after adding the permanent wire-count and position-authority tests.
- `pnpm --dir packages/jazz-tools exec tsc --noEmit`: final exit code 0. An
  intermediate run exited 2 because the skipped perf canary used the public
  `TestRuntime` type for concrete adapter transaction methods.
- `dev/gates/ts-wire-codec.sh`: exit code 0 after the receipts-only changes.
- `pnpm exec oxfmt dev/TS_DEDUP_PHASE2.md packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts packages/jazz-tools/src/runtime/native-runtime/runtime.test.ts`: exit code 0.
- Parent-curve setup commands in `/tmp/jazz_phase1_perf`: `git worktree add
--detach /tmp/jazz_phase1_perf codex/ts-dedup-query-semantics` exit code 0;
  `wasm-pack build --target web --release` in `crates/jazz-wasm` exit code 0.
  A direct parent `pnpm --dir packages/jazz-tools exec vitest ...` attempt exited
  254 before dependencies were installed in that throwaway worktree.
