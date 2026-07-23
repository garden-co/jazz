# Limit Subscription Fallback Investigation

Date: 2026-07-21
Branch/worktree: `track/limit-subscription-fallback` in `/Users/anselm/jazz_core-perf-dropdown`

## Summary

Guido's stress-app report is confirmed at the mechanism level.

`useAll(app.todos.limit(100))` installs an unordered `limit(100)` live subscription. Jazz does not support that as a maintained subscription today, so the serving peer rejects the maintained subscription capability. The native runtime then relies on snapshot refresh paths that call `db.all(...)` for opened subscriptions after write progression. For edge/global one-shot coverage, that same usage pattern is `attach_query`/read/`detach_query`, which sends a `Subscribe -> ViewUpdate -> Unsubscribe` lifecycle after the shape has been registered.

The fallback read does honor the limit in returned payload size: the probe returned exactly 100 rows at 10k, 50k, and 100k stored rows. The cost still scales linearly with stored rows, so the O(stored) work is before or during result limiting in core query evaluation/materialization, not wire payload transfer.

## Hypothesis Verification

### 1. Maintained `limit(100)` is rejected

Confirmed.

Stress app query:

- `dev/stress-tests/todo-react/src/TodoList.tsx:9` builds `app.todos.limit(100)`.
- `dev/stress-tests/todo-react/src/TodoList.tsx:19` passes it to `useAll(...)`.

The facade does not reject the shape early:

- `crates/jazz/src/db.rs:872-888` `attach_query_with_opts` calls `ensure_supported_subscription_shape`.
- `crates/jazz/src/db.rs:6126-6132` currently makes `ensure_supported_maintained_coverage_query_shape` a no-op.

The serving peer rejects when maintained lowering cannot provide result membership:

- `crates/jazz/src/db.rs:4738-4757` maps `PeerState::rehydrate_query_for_subscription_with_opts` `QueryCapability` errors to `SyncMessage::SubscribeRejected { reason: UnsupportedShapeCapability { detail } }`.
- `crates/jazz/src/node/query_engine/lowering.rs:805-836` validates maintained result-membership output and emits `"maintained subscription view window shape is not lowered yet"` when unsupported.
- `crates/jazz/src/node/query_engine/lowering.rs:867-875` rejects unordered slices unless they are `limit(1)` with `offset == 0` or unbounded.
- `crates/jazz/src/peer.rs:3855-3888` has an explicit regression test: unordered `limit(2)` and `limit(1).offset(1)` must error loudly and must not install a maintained subscription.

Spec alignment:

- `crates/jazz/SPEC/16_maintained_subscription_views.md:213-217` documents unordered `limit > 1` and unordered nonzero offset as unsupported.
- `crates/jazz/SPEC/14_lowering_to_groove.md:234-245` says unordered `limit(1)` lowers through `ArgMinBy`; ordered windows lower through `TopBy`; unordered `limit > 1` remains unsupported.

Probe evidence: `unsupported_limit100_subscribe_error=true`.

### 2. Client fallback re-queries and creates phantom wire cycles

Confirmed for the fallback re-query mechanism; the wire lifecycle follows from the query attachment path.

Native-runtime snapshot refresh callers:

- `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts:681-705` calls `refreshOpenedPlainSubscriptions()` after `waitForTransaction(...)` observes the write.
- `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts:991-1044` `observeWriteForBoundaryEffects` has two async waiters. The local-visible waiter calls `refreshOpenedPlainSubscriptions()` at `:1000-1003`; the settled waiter calls it again while pumping at `:1037-1039`.
- `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts:1213-1234` iterates opened subscriptions and computes a row diff from refreshed rows.
- `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts:1237-1242` `refreshPlainSubscriptionRows` calls `this.db.all(...)` or `allForIdentity(...)` using `subscription.query` and `subscription.opts`.
- `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts:842-856` stores the prepared query from the original subscription, so the fallback re-query uses the original limited query object.
- `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts:1737-1774` also uses `refreshPlainSubscriptionRows` when subscription chunks require snapshot refresh.

One-shot coverage wire lifecycle:

- `crates/jazz/src/db.rs:918-943` `attach_query_shape_binding_with_opts` queues a pending upstream `Subscribe`.
- `crates/jazz/src/db.rs:4356-4412` sends `RegisterShape` once per shape/read-view and sends `Subscribe`.
- `crates/jazz/src/db.rs:951-957` considers an attachment covered after a settled result set arrives, i.e. after `ViewUpdate`.
- `crates/jazz/src/db.rs:960-974` `detach_query` applies local unsubscribe and queues upstream `Unsubscribe`.
- `crates/jazz/src/db.rs:4423-4427` sends the upstream `Unsubscribe`.

Per one-shot attachment after shape registration is warm: `Subscribe=1`, `ViewUpdate=1`, `Unsubscribe=1`. The first attachment for a shape additionally sends `RegisterShape=1`.

The likely "two cycles per toggle" callers are:

- `waitForTransaction` refresh after wait completion: `native-runtime-adapter.ts:681-705`.
- `observeWriteForBoundaryEffects` refreshes during write-state pumping: `native-runtime-adapter.ts:991-1044`, especially `:1000-1003` and `:1037-1039`.

### 3. Re-query cost is O(stored rows) despite `limit(100)`

Confirmed.

Temporary probe: `crates/jazz-tools/examples/limit_subscription_fallback_probe.rs` (uncommitted). Command:

`cargo run -p jazz-tools --example limit_subscription_fallback_probe -j 2`

Exit code: `0`.

The probe seeds N todos, prepares `Query::from("todos").limit(100)`, toggles one row, waits local, then executes the same limited read that `refreshPlainSubscriptionRows` uses conceptually. It asserts the initial and refreshed reads both return 100 rows.

| Stored rows | Returned rows | Initial limited read | Toggle wait | Fallback refresh read |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 100 | 306.638 ms | 940.947 ms | 299.447 ms |
| 50,000 | 100 | 1,578.163 ms | 4,665.224 ms | 1,532.143 ms |
| 100,000 | 100 | 3,175.569 ms | 9,593.762 ms | 3,162.260 ms |

Attribution:

- Wire payload is bounded by the visible limit in this probe: 100 rows returned at every stored size.
- The read path is `Db::all` -> `all_for_identity` -> `NodeState::query_rows_for_link_with_prepared_plan` (`crates/jazz/src/db.rs:679-728`).
- Query normalization adds a `RowSetExpr::Slice` for `limit` after the root source/order operators (`crates/jazz/src/node/query_eval.rs:5559-5575`).
- Ordinary read tests confirm order/limit semantics are post-root materialization (`crates/jazz/src/node/query_eval.rs:12137-12171`).

The degradation curve is almost perfectly linear in stored rows. That means the fallback is not fetching an unbounded payload, but the core one-shot evaluator still pays O(stored rows) before it can return the 100-row result.

## Wire Count Evidence

Observed/derived count per fallback one-shot attachment after shape registration is warm:

| Message | Count per one-shot refresh | Evidence |
| --- | ---: | --- |
| `RegisterShape` | 0 warm, 1 cold | `db.rs:4356-4383` sends it only when `announced_shapes.insert(...)` succeeds. |
| `Subscribe` | 1 | `db.rs:918-943` queues subscribe; `db.rs:4396-4412` sends it. |
| `ViewUpdate` | 1 | `db.rs:951-957` requires settled result set coverage; server rehydrate returns a `ViewUpdate` or rejection at `db.rs:4738-4768`. |
| `Unsubscribe` | 1 | `db.rs:960-974` queues detach unsubscribe; `db.rs:4423-4427` sends it. |

For a toggle that hits both refresh callers, expected warm-cycle counts are `Subscribe=2`, `ViewUpdate=2`, `Unsubscribe=2`.

## Fix Analysis

### CLEARLY-GOOD: add explicit order in the stress app query

Estimate: small app-side change, low engine risk.

`app.todos.orderBy(...).limit(100)` should route to ordered-window support instead of the unsupported unordered `limit > 1` path. The engine/spec already target ordered windows through `TopBy`:

- `crates/jazz/SPEC/16_maintained_subscription_views.md:240-242`
- `crates/jazz/src/node/query_engine/lowering.rs:4036-4049`

This is the cleanest local mitigation for the stress app if product semantics accept a deterministic visible order.

### CLEARLY-GOOD: avoid duplicate fallback refreshes per write

Estimate: small TypeScript adapter change, moderate correctness scrutiny.

The two likely refresh callers are independent and can both run for one write. A coalescing guard for opened plain subscription refreshes per write/progression tick would reduce the phantom one-shot cycles without changing engine semantics. This does not solve O(stored) per remaining refresh, but it can halve the observed churn in Guido's "two cycles per toggle" case.

This is a fallback fix candidate, but I did not implement it in this lane because the exact transaction/progression ordering needs a focused adapter test to avoid regressing local-visible delivery.

### SPECULATIVE: support unordered `limit > 1` maintained subscriptions

Estimate: medium to large, semantic decision required.

There is a tempting implementation path because `lower_window` can construct `TopBy` with empty user order plus row-uuid tie-breaker (`crates/jazz/src/node/query_engine/lowering.rs:4017-4033`). But the capability validator deliberately blocks unordered `limit > 1` (`lowering.rs:867-875`) and the spec says it is unsupported. Enabling it would define application-visible behavior for "unordered but deterministic" windows, not just plumbing.

If accepted as semantics, the likely work is:

- update ch. 14/ch. 16 semantics for unordered finite windows;
- relax `linear_window_supported`;
- adjust/replace `maintained_subscription_view_unsupported_limited_variants_error_loudly`;
- add black-box maintained-vs-one-shot coverage for row insert/delete/update boundary churn.

### SPECULATIVE: make one-shot evaluator stop after `limit`

Estimate: medium/high; engine optimization with policy/order caveats.

The measured fallback read returns only 100 rows but scans proportional to all stored rows. An unordered no-filter `limit(100)` could theoretically stop early, but policy composition, filters, order-by, branches, and deterministic row choice make this nontrivial. It is still worth considering as a general one-shot optimization, but it is not the mechanism-first fix for maintained subscriptions.

### NOT USEFUL: make fallback re-query "honor the limit"

The fallback already appears to honor the limit in returned rows. The problem is not unbounded ViewUpdate payload size; it is O(stored) evaluation before returning the bounded result and repeated attach/read/detach churn.

## Verification

Commands run:

- `cargo run -p jazz-tools --example limit_subscription_fallback_probe -j 2`
- Exit code: `0`
- `ps -axo pid,stat,command | rg ...limit_subscription...`
- Exit code: `1` from `rg` because there were no matching live probe/cargo processes.

No commits made.

Tooling-friction: a built-in stress-app headless probe that emits native adapter refresh counts, wire counts, row payload counts, and core read timings would have saved the temporary example and manual source attribution.
