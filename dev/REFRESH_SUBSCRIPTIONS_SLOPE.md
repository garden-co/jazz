# Refresh Subscriptions Slope

Date: 2026-07-22

## Target

The native subscribed commit-cost slope lives in `Db::refresh_subscriptions` for
one local maintained subscription over `Query::from("todos")`. The historical
receipt in `dev/COMMIT_SUPERLINEARITY.md` showed subscribed native commit time
rising from about 84 ms at 500 rows to about 320 ms at 10k rows while the
unsubscribed native path stayed roughly flat.

The repro example named by that receipt was not present on this checkout before
this lane, so the fresh pre-fix command could not be run literally. The new
checked-in example `crates/jazz/examples/commit_superlinearity_native.rs` keeps
that bench-only receipt available on this branch.

## Attribution

The O(result) stage was the snapshot application loop in
`apply_maintained_update_to_snapshot` in `crates/jazz/src/db.rs`.

For each maintained delta, the old path scanned the accumulated materialized
snapshot to decide whether each added row was new or updated:

- `snapshot.rows.iter().take(snapshot.root_count).position(...)`
- root removals scanned the current roots against the removed delta list
- relation edge additions scanned all edges for existence
- related-row additions scanned roots and then related rows

For an append-only unbounded subscription with 500 new rows per commit, that
made the facade snapshot application O(current result size * delta size) in the
common add path. This is the INV-INC-1 violation: the maintained terminal delta
was bounded, but delivery re-materialized membership against the full delivered
snapshot.

The local maintained drain in `NodeState::drain_local_maintained_view_subscription`
already accumulates `ResultTransitions` and applies root result membership
deltas to `local.result_set`. It still owns the cost of Groove delivery and
materializing the rows in the delta, but it no longer explains the result-size
scan removed here.

## Fix

`SubscriptionState` now keeps a `RelationSnapshotIndex` beside its materialized
`RelationSnapshot`:

- root row key to root position
- related row key to related position
- relation edge set

`apply_maintained_update_to_snapshot` updates that index with the snapshot and
uses it for add/update and edge-existence decisions. Full snapshots and
authoritative reset snapshots rebuild the index once. Removal paths rebuild or
adjust indexes after vector removals because removals legitimately move the
stored snapshot.

This preserves the existing public event semantics on this branch: same
`SubscriptionEvent::Delta` shape, same snapshot storage, same root insertion
order for the current plain semantics. On the stacked branch where SPEC 16
positions are core-carried, this index should remain compatible but the root
insert location must be driven by the carried position instead of always
appending at `root_count`.

## Post-Fix Curves

Run:

```sh
cargo run -p jazz --example commit_superlinearity_native --release -- 5000
cargo run -p jazz --example commit_superlinearity_native --release -- 10000
```

Both exited 0.

Selected 10k curve:

| rows | unsub commit ms | sub commit ms | sub event adds |
|---:|---:|---:|---:|
| 500 | 11.896 | 58.090 | 500 |
| 2500 | 11.185 | 65.620 | 500 |
| 5000 | 10.902 | 68.829 | 500 |
| 7500 | 11.218 | 70.405 | 500 |
| 10000 | 11.622 | 71.370 | 500 |

5k subscribed selected points: 500 rows 56.858 ms, 2500 rows 65.672 ms,
5000 rows 69.844 ms. Unsubscribed stayed about 10.7-11.4 ms.

Compared with the historical native receipt at 10k, the subscribed terminal
batch dropped from about 320 ms to about 71 ms. The remaining subscribed cost is
mostly maintained graph delivery plus materializing the 500-row delta, not a
scan of the accumulated result snapshot.

## Gates

- `cargo check -p jazz -j 2`: exit 0
- `cargo check -p jazz --example commit_superlinearity_native -j 2`: exit 0
- `cargo fmt -p jazz --check`: exit 1 before formatting, formatting-only
- `cargo fmt -p jazz`: exit 0
- `cargo run -p jazz --example commit_superlinearity_native --release -- 5000`: exit 0
- `cargo run -p jazz --example commit_superlinearity_native --release -- 10000`: exit 0
- `cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact`: exit 101 with a temporary debug assertion that rebuilt the full snapshot index; after removing that assertion, exit 0
- `cargo test -p jazz --test incremental_delivery_canary -j 2`: exit 0
- `cargo test -p jazz subscription --lib -j 2`: exit 0
- `cargo test -p jazz --test four_tier -j 2`: exit 101; pre-existing dirty test references removed `PeerState::client_link` / `PeerRole::ClientLink`
- `cargo test -p jazz --test threaded_four_tier -j 2`: exit 101; pre-existing dirty test references removed `PeerState::client_link`
- `cargo test -p jazz -j 2`: exit 101 on the same four-tier compile failures
- `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle -j 2`: exit 101 before the oracle because package integration test compilation hits the same four-tier failures
- `cargo test -p groove -j 2`: exit 101 when run concurrently with other Cargo gates due doctest `E0460` artifact-version races; rerun by itself, exit 0
- `cargo test -p jazz-tools --features test -j 2`: exit 0
- `cargo test -p jazz-server -j 2`: exit 0
- `cargo check -p jazz-sim --benches -j 2`: exit 0
- `dev/gates/ts-wire-codec.sh`: exit 2; TypeScript parse/config failures in existing TS sources (`src/dsl.ts`, `src/migrations.ts`, `src/typed-app.ts`, and `moduleResolution`)
- `dev/benchmarks/smoke.sh`: exit 1; only `jazz/sync` failed, compiling the pre-existing dirty `crates/jazz/benches/sync.rs` reference to removed `PeerState::client_link`

Tooling-friction: having the native repro checked in before the lane would have
allowed a literal fresh pre-fix baseline from this exact checkout; running
multiple Cargo gates concurrently also caused a transient Groove doctest artifact
race that required a clean sequential rerun.
