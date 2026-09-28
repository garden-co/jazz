# Routed subscription CPU investigation (2026-09-23)

Measured on the native `perf` profile, stacked on `5a19dc918` and draft PR #3262. The fixture uses the public Jazz query `documents WHERE team = $team ORDER BY updated_at DESC LIMIT 100`, 1,000 distinct bindings, 2,000 documents, and exact event checks. All values are synthetic.

## Production Jazz result

| 1,000 routes              | Before this change | Cache + collector fix |
| ------------------------- | -----------------: | --------------------: |
| Initial hydration         |             1.60 s |                1.60 s |
| Matching write            |            40.2 ms |          27.2–28.0 ms |
| Unrelated, quiet write    |            29.5 ms |          15.0–15.5 ms |
| Below-window, quiet write |            29.5 ms |          14.9–16.6 ms |

The route receipt passed its exact initial-window, matching-delta, quiet-write, and witness contracts. It reports 11,069 graph nodes, 1,009 arrangements, zero retained source-version identities, and 1,099 replacement entries. The older `ROUTE_SUBSCRIPTION_CURVE.md` source-witness memory diagnosis no longer describes this code path.

The first profile showed repeat activation-plan compilation and a DFS from every output to classify structured `CollectBy`. The activation cache's 65,536-weight limit excluded this repeated plan. Raising its bounded budget to 1,000,000 reduced a quiet write to 23–24 ms. Graph validation rejects every consumer of `CollectBy`, so classifying only the output node removes the DFS and lowers quiet writes to 15.0–15.5 ms. A ten-second sample after both fixes still spent its largest share inside Groove `apply_batch` and `IncrementalEvaluation::poll`; `poll_ready_node`, register resolution, memo insertion, node metadata cloning, and hash allocation recur across the large graph. Repeated quiet writes averaged 18.1 ms each over 1,000 inserts into an unobserved team.

## What still scales with binding count

`Db::subscribe` uses `ClientLocal`. Current-query lowering inlines bound values for that mode, producing a separate physical graph per binding. The receipt has 1,000 subscriptions and **zero** active prepared shapes. A temporary trusted-serving switch was not a fix: it produced **1,000** prepared shapes and 11,094 graph nodes, because the retained subscription path calls Groove `prepare` for each binding.

Even one actual Groove prepared shape still installs a literal filter and projection per binding in `bound_routed_multisink_graph`. The reproducible public-API probe is `crates/groove/examples/route_shared_probe.rs`:

```sh
cargo build --profile perf -p groove --example route_shared_probe
target/perf/examples/route_shared_probe direct 1000
target/perf/examples/route_shared_probe shared 1000
target/perf/examples/route_shared_probe single 1000
```

| Groove probe, 1,000 route keys   | Direct graphs | Prepared shape + bound outputs | One grouped output |
| -------------------------------- | ------------: | -----------------------------: | -----------------: |
| IVM graph nodes                  |         2,008 |                          2,008 |                  8 |
| Matching write                   |       32.3 ms |                         3.0 ms |           0.089 ms |
| Unrelated write, 20-write median |       1.15 ms |                        1.13 ms |           0.012 ms |

The `single` probe uses a fixed inline set of route keys and one output subscriber. It proves the shared IVM computation is cheap; it **does not** implement mutable bindings, per-subscriber delivery, authorization separation, or unsubscribe. It is an upper-bound experiment, not a product speedup claim.

## Recommended architectural change

Make the execution and delivery unit a **shape plus route key**, rather than a graph suffix per subscriber:

1. Lower one shared parameterized graph per exact schema, read-view, durability, and authorization scope. Treat active bindings as rows in its input relation.
2. Group maintained `TopBy`/`CollectBy` state by route key. Build a route-key-to-subscriber index and deliver only changed groups. A new or retired binding changes that input relation and hydrates or retracts its own group.
3. Keep the present per-binding graph path for queries whose output cannot be routed exactly. Prove equivalence for matching and unrelated inserts, window boundary shifts, deletion/restore, multi-sink relations, claim changes, cold hydration, cancellation, and unsubscribe before switching paths.

Use the existing Jazz route receipt as the acceptance gate: at 1,000 bindings, require fewer than 200 graph nodes, a quiet write below 5 ms, a matching write below 8 ms, and every exact event/witness check still true on the same machine and `perf` profile. Compare the new path and fallback under identical fixture and authorization scope. These are engineering targets, not measurements of the current code.

This targets the measured CPU growth. Removing history alone cannot eliminate the graph traversal seen on quiet writes. Removing IVM could be viable for indexed point queries with route-aware requery, but should be compared against the shared-output design; the single-output probe shows that IVM itself can maintain this workload cheaply.

## Validation on this stack

- `cargo test -p groove`: passed (838 library tests and doctests).
- `JAZZ_SEED_COUNT=300 cargo test -p jazz --features testing m3_maintained_one_shot_differential_oracle`: passed.
- `cargo test -p jazz --features testing --test incremental_delivery_canary`: all three passed.
- `cargo check -p jazz --features testing --bench route_subscription_curve`, `cargo check -p jazz-sim --benches`, and `cargo check -p groove --example route_shared_probe`: passed.
- `cargo test -p jazz --features testing`: 2,171 passed, one failed, two ignored. The failure is `db::tests::wire_transport::standalone_adapter_services_auxiliary_after_four_canonical_extents`, at its physical-frame-count assertion; it also fails in isolation and does not exercise the changed IVM files. It was not rewritten.
- This stack has `dev/gates/benchmark-smoke.sh`, but not the newer `dev/benchmarks/smoke.sh`. The former fails on macOS `paste -sd,` before running Cargo, so its equivalent benchmark API check was run directly.

Tooling friction: a native public-API probe that reports graph nodes, phase timings, and sampled stacks in one command would have avoided repeated optimized rebuilds and temporary profiling switches.
