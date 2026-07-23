# Default Ordering / Maintained Limit Implementation

Date: 2026-07-21
Branch/worktree: `codex/default-ordering-maintained-limit` in `/Users/anselm/jazz_core-perf-dropdown`

## Decisions

- Lowering scope: implemented for root `Linear` relation windows that have
  `limit` and/or `offset` and no explicit `order_by`. Lowering injects
  ascending `RowUuid` order immediately before the `Slice`, so maintained
  capability validation and `TopBy` window lowering see the query as ordered.
- Subtree scope: staged. Applying default order to every linear fragment
  perturbed recursive/policy maintained graphs and produced `UnsupportedOperator`
  plus missed RLS deltas in existing maintained-view harness tests. SPEC 6 now
  marks unbounded roots and `array_subqueries`/nested relation subtrees as the
  remaining plan-injection work.
- ArgMinBy fate: retained only for genuinely unordered internal shapes where no
  default result order is injected. Root windowed `limit(1)` queries now travel
  through the injected-order `TopBy` path with the rest of root windows.
- Remaining rejections: aggregate maintained lowering, maintained relation-edge
  deltas for `array_subqueries`, incomplete predicate-policy lowering, and
  default-order injection for unbounded/subtree relation boundaries remain
  documented gaps. Root unordered `limit > 1` and nonzero `offset` are no longer
  rejected by the core maintained lowering.
- Schema boundary: no schema/catalogue conversion change was made. The change is
  lowering/runtime only.
- Subscriber delta boundary: a public `jazz-tools` black-box tracking test shows
  the subscriber can still emit reset/removal churn before fine-grained
  default-order window positions. It is ignored and documented because the core
  maintained serving path accepts the window, but the client-facing pass-through
  delta story is not complete.

## Intentional Test Updates

- `crates/jazz/src/peer.rs::maintained_subscription_view_default_order_limited_variants_are_supported` replaces the old loud rejection regression for unordered `limit(2)` and `offset(1).limit(1)` with supported maintained hydration assertions.
- `packages/jazz-tools/tests/ts-dsl/query-api.test.ts` unskips the two order-sensitive DSL tests now that root window default ordering is lowered.
- `crates/jazz-tools/tests/edge_server_mode.rs::default_order_limit_subscription_emits_ordered_window_indices` adds a public API tracking test for exposed ordered delta indices, currently ignored because subscriber reset/removal churn remains.
- `crates/jazz-tools/src/client.rs` computes removal/update old indices from the previous subscription row order rather than enumerating removed rows. This is an adapter correctness improvement exposed by the tracking test, but it does not solve reset churn by itself.

## Stress Probe

Command: `cargo run -p jazz-tools --example limit_subscription_fallback_probe -j 2`
Exit code: `0`

Before values are from `dev/LIMIT_SUBSCRIPTION_FALLBACK.md`; after values are
from this run. The committed probe still measures one-shot refresh read cost, so
the O(stored) refresh curve remains even though the maintained subscribe is now
accepted.

| Stored rows | Before subscribe accepted | After subscribe accepted | Before refresh read | After refresh read |
| ---: | :---: | :---: | ---: | ---: |
| 10,000 | no | yes | 299.447 ms | 304.915 ms |
| 50,000 | no | yes | 1,532.143 ms | 1,595.255 ms |
| 100,000 | no | yes | 3,162.260 ms | 3,238.588 ms |

Probe after-run detail:

| Stored rows | Seed | Initial limited read | Returned rows | Toggle wait | Refresh read |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 6,877.973 ms | 311.239 ms | 100 | 916.686 ms | 304.915 ms |
| 50,000 | 34,819.161 ms | 1,563.676 ms | 100 | 4,662.272 ms | 1,595.255 ms |
| 100,000 | 70,094.707 ms | 3,273.375 ms | 100 | 9,494.823 ms | 3,238.588 ms |

`unsupported_limit100_subscribe_error=false`

## Gate Table

| Command | Exit | Notes |
| --- | ---: | --- |
| `cargo test -p jazz maintained_subscription_view_limit_one_installs_subscription -j 2` | 0 | focused compile/behavior check after lowering edit |
| `cargo test -p jazz maintained_subscription_view_default_order_limited_variants_are_supported -j 2` | 0 | updated former rejection regression |
| `cargo test -p jazz-tools --features test default_order_limit_subscription_emits_ordered_window_indices -j 2` | 0 | no test ran because `clients_sync.rs` is not a registered target; moved test |
| `cargo test -p jazz-tools --features test --test edge_server_mode default_order_limit_subscription_emits_ordered_window_indices -j 2` | 101 | exposed subscriber reset/removal churn and wrong removal index before fix/ignore |
| `cargo test -p jazz-tools --features test --test edge_server_mode default_order_limit_subscription_emits_ordered_window_indices -j 2` | 0 | ignored tracking test compiles |
| `cargo test -p jazz maintained_subscription_view_ -j 2` | 101 | broad injection into internal fragments broke recursive/RLS maintained tests |
| `cargo test -p jazz maintained_subscription_view_ -j 2` | 0 | root-window-only injection passes maintained-view focused suite |
| `cargo run -p jazz-tools --example limit_subscription_fallback_probe -j 2` | 0 | maintained limit subscribe accepted; probe still measures one-shot refresh curve |
| `cargo fmt --check -p jazz -p jazz-tools` | 1 | formatting diffs before applying fmt |
| `cargo fmt -p jazz -p jazz-tools` | 0 | formatted affected Rust files |
| `cargo fmt --check -p jazz -p jazz-tools` | 0 | formatting gate passes |
| `pnpm --dir packages/jazz-tools vitest run --config vitest.config.ts tests/ts-dsl/query-api.test.ts` | 254 | local pnpm interpreted path as command; no test ran |
| `pnpm -C packages/jazz-tools vitest run --config vitest.config.ts tests/ts-dsl/query-api.test.ts` | 254 | local pnpm interpreted path as command; no test ran |
| `pnpm vitest run --config vitest.config.ts tests/ts-dsl/query-api.test.ts` from `packages/jazz-tools` | 254 | Node 20.13.1 warns against required >=22.12; `vitest` command unavailable |
| `pnpm exec vitest run --config vitest.config.ts tests/ts-dsl/query-api.test.ts` from `packages/jazz-tools` | 254 | `vitest` command unavailable |

Full canonical gates were not completed in this pass.

Tooling-friction: a public subscription probe that separates core maintained
acceptance, reset-result-set churn, ordered delta indices, and fallback one-shot
refresh counts would have avoided mixing the old fallback read benchmark with
the new maintained acceptance check.

## Slice 2

Scoping rule implemented: default row-id ordering is only injected for simple
root app/result plans with no auxiliary sources, closure paths, or join
contributions, and only at the root window boundary already handled by slice 1;
recursive, RLS/policy, relation-subgraph, auxiliary, and coverage terminals are
not rewritten.

Unbounded root result outcome: the public `jazz-tools` subscription adapter now
derives ordered delta indices from the prepared query's ordered `all()` snapshot
after each core subscription event, so unbounded todo-style subscriptions expose
ascending row-id order for one-shot reads, resets, and delta positions. This is
intentionally scoped at the app subscription boundary after a naive unbounded
core `TopBy(usize::MAX)` injection reproduced the slice-1 RLS miss pattern in
`maintained_subscription_view_emits_expected_owner_policy_updates` and
`maintained_subscription_view_real_peer_path_emits_expected_view_updates`.

Cost note: the current app-boundary unbounded position fix is O(result size) per
public subscription event because it re-reads the ordered prepared query result
before translating core rows into `OrderedRowDelta` indices. The core maintained
delivery canaries still pass, but this is not the desired final mechanism for
large unbounded app subscriptions; a core event shape that carries ordered
positions, or a maintained ordered-membership index exposed to the adapter, would
avoid the extra full-result read.

Churn-test outcome: `crates/jazz-tools/tests/edge_server_mode.rs::
default_order_limit_subscription_emits_ordered_window_indices` is unignored and
passes. The new unbounded black-box test
`default_order_unbounded_subscription_keeps_row_id_order_across_deltas` inserts
rows across ticks and verifies a later lower id lands at index `0`, with expected
ids sorted to avoid same-millisecond UUIDv7 assumptions.

Customer cold-start receipt:

| Metric | Before reference | Slice 2 after |
| --- | ---: | ---: |
| Cold wall_ms | 13,461 | 12,320 |
| Cold dominant_child_opened_ms | not recorded in the reference table | 466 |
| Cold dominant_child_materialized_ms | 12,765 | 11,675 |
| Cold dominant_child_rows | 23,831 | 23,831 |
| Warm wall_ms | 7,229 | 7,239 |
| Warm dominant_child_opened_ms | not recorded in the reference table | 537 |
| Warm dominant_child_materialized_ms | 5,960 | 5,993 |
| Warm dominant_child_rows | 23,831 | 23,831 |

Before reference is the prior same-branch native receipt in
`dev/PERF_FINDINGS_DROPDOWN.md` Phase 7. After command:
`JAZZ_REHYDRATE_TRACE=1 JAZZ_CUSTOMER_PHASES=cold,warm cargo bench -p jazz-sim --bench customer_cold_start -j 2`
with exit code `0`.

Gate table:

| Command | Exit | Notes |
| --- | ---: | --- |
| `cargo test -p jazz maintained_subscription_view_ -j 2` | 0 | RLS/recursive maintained focused suite passed after backing out unbounded core injection |
| `cargo test -p jazz --test incremental_delivery_canary -j 2` | 0 | all three canaries passed |
| `cargo test -p jazz-tools --features test --test edge_server_mode default_order_ -j 2` | 0 | limited churn test active; unbounded todo-example test passed |
| `cargo test -p jazz-tools --features test --test edge_server_mode default_order_limit_subscription_emits_ordered_window_indices -j 2 -- --ignored` | 0 | pre-unignore focused confirmation |
| `cargo test -p jazz -j 2` | 1 | first run overlapped other cargo work and failed only in doctest crate resolution |
| `cargo test -p jazz -j 2` | 0 | rerun alone passed, including doctests |
| `cargo test -p jazz-tools --features test -j 2` | 0 | full jazz-tools gate passed |
| `cargo test -p jazz-server -j 2` | 0 | passed |
| `cargo test -p groove -j 2` | 101 | first run overlapped other cargo work and failed only in doctest crate-version skew |
| `cargo test -p groove -j 2` | 0 | rerun alone passed, including doctests |
| `cargo check -p jazz-sim --benches` | 0 | passed |
| `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle -j 2` | 0 | passed in 131.24s |
| `dev/gates/ts-wire-codec.sh` | 0 | 84 passed, 1 skipped |
| `cargo fmt --check -p jazz -p jazz-tools` | 0 | passed |
| `dev/benchmarks/smoke.sh` | 0 | passed; ledger appended at `dev/benchmarks/SMOKE_LEDGER.md`, results in `dev/benchmarks/results/20260721T215603Z` |

Tooling-friction: core subscription events do not expose ordered positions, so
the public adapter had to choose between append-biased deltas and an O(result)
ordered snapshot refresh; a position-bearing core delta would have made this
slice both cleaner and scale-proof.
