# Reuse public row layouts for initial SELECTs

The full anonymized SaaS SELECT workload improves **1.35×** (669.237 ms →
496.841 ms). Its largest query improves **1.45×** (495.719 ms → 341.285 ms).
These are native, instrumented local medians, with three samples per arm.

## Workload and source

- Main: `7a113ff8b`; parent: UUID-page PR #3534, `49e8f9721`.
- Measured implementation: `b2527e601`. The follow-up avoids preparing an unused
  projector for a single-row result; the saved timing/binary receipt is for the
  measured implementation.
- 39 unbounded `Db::all_for_identity` queries at Global durability, member
  identity, RocksDB, 27,518 returned rows. No deleted rows in this fixture.
- One query sweep per freshly reopened copy of the same seeded authority.
  The default driver opens the copy as node 6; both measurement arms use that
  identity. OS caches are warm; these are not cold-device I/O measurements.
- Seeding, copying, opening, query preparation, result checks and encoding are
  outside SELECT time. Query lowering, policy evaluation and public row
  materialization are inside it. The driver reports preparation separately.
- The baseline adds only the SELECT driver and phase attribution to the parent.
  Its exact source patch and binary are preserved with the local receipts.
  The current driver additionally reports copy/open time and supports repeated
  sweeps after one open for CPU sampling; every A/B run uses one sweep.

## Results

Alternating order: A, B, B, A, A, B. A is baseline; B is this change.

| SELECT time, ms                     |       A |       B |
| ----------------------------------- | ------: | ------: |
| Sample 1                            | 668.295 | 514.124 |
| Sample 2                            | 669.237 | 496.841 |
| Sample 3                            | 925.881 | 494.405 |
| Median, all 39 queries              | 669.237 | 496.841 |
| Median, largest table (23,831 rows) | 495.719 | 341.285 |

The high baseline sample is retained. The other two baseline samples agree
within 1 ms. Every run checks the exact ordered authorized UUIDs against the
fixture and matches all 39 encoded-result lengths and hashes across arms.

| Exclusive phase median, ms |       A |       B |
| -------------------------- | ------: | ------: |
| Construct and bind rows    |  40.399 |   2.546 |
| Normalize public rows      | 149.937 |  35.920 |
| Install/hydrate query      | 390.409 | 369.948 |
| Lower query                |  69.456 |  61.575 |

The two targeted phases save about 152 ms. Variance in the other phases accounts
for the rest of the endpoint difference; the endpoint gain is measured directly.
Phase medians are calculated independently and need not sum to the endpoint.

## Mechanism and invariants

Previously every result constructed default field metadata, replaced that
metadata from the app-row schema, rebuilt the public descriptor, decoded owned
values, and encoded them again. Most of that work repeated the same layout.

The change binds publication metadata once per descriptor in the result batch.
The first flat row goes through the existing public projection and establishes
a reusable `RecordProjector`. Subsequent rows with identical descriptors,
publication bindings and transaction-alias presence copy the selected encoded
fields into the established public layout. Authors are still validated per row.

For example, 23,831 rows with the same schema reuse one public projection while
retaining each row's UUID, nullable cells, values, authors and timestamps. A
different layout, publication binding, nullable conversion or transaction-alias
presence uses the existing conversion. Aggregate normalization keeps its
explicit source/output bindings. History, deletion and permission semantics are
unchanged. There is no new durable encoding or wire format.

Preflight reviewed the rejected-experiments ledger and prior projection PRs.
Unlike #2851's graph-root projection experiment, this targets the separate
public `CurrentRow` conversion, measured at about 23% of this initial-SELECT
workload, and removes repeated metadata construction as well as value copying.

## Validation

- New public-API integration test: bulk and point results agree byte-for-byte
  and in publication metadata, with independent expected values, nullable cells,
  generated-name collisions, a 150 KB selected value, ordering and provenance.
- All 19 tests across the new test, shared query hydration, row provenance,
  large-value read scaling, UUID-page probes and all three incremental-delivery
  canaries pass.
- Exact ten-seed differential oracle, churn depths 10 and 1000: passes (one
  test, zero ignored, 30.00 s). Production Clippy passes with warnings denied.
  This receipt does not claim full local CI-equivalent validation.

## Reproduce and profile

```sh
CARGO_TARGET_DIR="$PWD/target" cargo build \
  -p jazz-example-permissioned-resources-benchmark \
  --bin permissioned-resources-profile --profile perf \
  --features cold-settle-attribution

JAZZ_CUSTOMER_INITIAL_SELECTS=1 JAZZ_CUSTOMER_IDENTITY=member \
JAZZ_CUSTOMER_PHASES=cold JAZZ_CUSTOMER_NO_DIAGNOSTICS=1 \
  target/perf/permissioned-resources-profile
```

Optional controls: `JAZZ_CUSTOMER_ONLY_TABLE=res_l_child_3`,
`JAZZ_CUSTOMER_QUERY_LIMIT=100`, `JAZZ_CUSTOMER_REOPEN_SEEDED_NODE=1`, and
`JAZZ_CUSTOMER_INITIAL_SELECT_REPETITIONS=25`. Repetitions after iteration zero
reuse the open database and are useful for sampling, not fresh-open receipts.

Local artifacts are in `target/saas-read-materialization-ab/`: `ab-*.json`,
`ab-summary.json`, `baseline-source.patch`, the preserved binaries and CPU
samples. SHA-256:

- Baseline: `04a3fff9ed022ab3ef054dcbdc6dc7c9353bcad88a5d92c031fa7c7ff1d676ae`
- Candidate: `0247f58f7575dd7c8468d1009b6736b90d8c02251b707b1a8c6f9b44cd10844a`

Sampling also exposed a separate recovery fallback when a copied store is
opened under another node identity: 10.448 s versus 297.875 ms for the seeded
identity in diagnostic single samples. That is outside SELECT time and tracked
in [#3537](https://github.com/garden-co/jazz/issues/3537).

The subsequent read-only sample spends about 43% of main-thread observations
in join preparation/evaluation. Avoiding unnecessary one-result join state is
the next bounded experiment in [#3538](https://github.com/garden-co/jazz/issues/3538).

Tooling friction: isolated Cargo targets avoid stale cross-worktree binaries;
timing open separately and repeating only reads keeps CPU samples on the query.
