# Classify table read counters once per scan

## Why this small change is worth retaining

The post-join CPU sample in #3540 found 177 of 2,679 SELECT observations (6.6%)
inside `storage_read_destination`. Every returned table row searched the same
physical column-family name for metric classification. This bookkeeping runs
in ordinary production builds as well as profiling builds.

A table cursor now remembers its destination and counts each returned batch
once. The shared `indices` family still classifies each returned key, because
one scan can cross multiple index destinations. Point and reverse-point read
behavior is unchanged. Empty/error range attempts still count as ranges, and
only returned rows count as reads. No storage/wire encoding or public API changes.

## Native endpoint measurement

Parent: #3540, `a47352d4416d7e3dad4a94b550f69efb20abbb80`.
`[profile.perf]`, RocksDB, member identity, fresh-open first sweep of all 39
unbounded SELECTs, 27,518 authorized rows, no deleted rows. The seeded node's
identity is preserved on open. Preparation/copy/open and result hashing are
outside SELECT time; lowering, policy evaluation and public normalization are
inside it. OS caches are warm; no builds run during timing.

| Read                        | Parent median | Trial median | Less time |
| --------------------------- | ------------: | -----------: | --------: |
| All 39 SELECTs              |    370.391 ms |   357.083 ms |      3.6% |
| Largest SELECT, 23,831 rows |    219.759 ms |   205.391 ms |      6.5% |

Two rounds, A/B/B/A/A/B then B/A/A/B/B/A, six samples per arm. The first round
showed 4.7% less total read time; a reverse-order confirmation showed 2.8% less.
The table reports the pooled medians and retains every observation.

| Arm             | All SELECTs (ms)          | Largest SELECT (ms)       |
| --------------- | ------------------------- | ------------------------- |
| A, first round  | 376.853, 369.102, 393.810 | 221.359, 218.158, 240.873 |
| B, first round  | 359.051, 351.346, 365.512 | 208.912, 203.761, 211.509 |
| A, confirmation | 360.933, 371.680, 365.781 | 213.404, 225.610, 217.751 |
| B, confirmation | 352.389, 355.435, 358.731 | 205.397, 205.385, 204.557 |

Each run asserts exact independently expected authorized UUID ordering.
All 39 encoded result lengths/hashes match across all twelve runs.
Both rounds emit the full per-table exclusive phase breakdown. In the first
round, exclusive graph install/read execution falls from 244.681 to 229.305 ms;
query lowering and public normalization are effectively unchanged. Phase
medians and total medians are computed independently.

These local native measurements isolate the scan-counter change. Do not
multiply this ratio by earlier PR ratios and call it a new combined receipt.
This is not a measurement of the retained-subscription workload or browser WASM.

## Validation

Two internal storage-API counter tests pass. They cover multiple returned
batches, zero/short/overlarge limits, reverse ordering, empty scans, failed
opens, mixed index destinations, malformed index keys, and independently
classified range attempts. This coverage is internal because the public query
API cannot request a scan across mixed internal index families.

The nine public Groove first-result and snapshot/subscription regressions pass,
as do all eight Jazz incremental-delivery canary and shared-hydration tests.
`cargo test -p groove --features test -- --test-threads=1` passes 1,077 tests
with four existing ignores, including the feature-gated async storage suites.

The ordinary parallel Groove run fails the existing
`subscription_install_does_not_sweep_unrelated_resident_graphs` timing guard:
75.542/1,301.583 microseconds (small/large), then 69.334/998 microseconds.
The unchanged guard passes alone (88/595 microseconds) and in the serialized
suite. The parent parallel suite passes. This does not establish a new
regression or dismiss the failure as noise: the same guard's scaling problem
already has CI evidence in #3144. Keep that failure visible for the draft;
standard correctness and hosted benchmark gates remain required before landing.
There is no local CI-equivalent claim.

## Reproduction and provenance

Build and run the same driver as #3540:

```sh
CARGO_TARGET_DIR="$PWD/target" cargo build \
  -p jazz-example-permissioned-resources-benchmark \
  --bin permissioned-resources-profile --profile perf \
  --features cold-settle-attribution
JAZZ_CUSTOMER_INITIAL_SELECTS=1 JAZZ_CUSTOMER_IDENTITY=member \
JAZZ_CUSTOMER_PHASES=cold JAZZ_CUSTOMER_NO_DIAGNOSTICS=1 \
JAZZ_CUSTOMER_REOPEN_SEEDED_NODE=1 target/perf/permissioned-resources-profile
```

SHA-256:

- A: `69c15a2ccac57339d9eb0b8704b0c83fef5245d49c6b7e0b2aa6ccd180fb6e17`
- B: `e24b65936dc292facb44824e1809c809207a959032e2d2507cba03402e77008a`

Artifacts in the primary checkout's `target/saas-read-scan-metrics-ab/`:
preserved binaries, runtime patch, both scripts, twelve JSON/log pairs,
`ab-summary.json`, `confirm-summary.json`, and `pooled-summary.json`.
The preflight reviewed the rejected ledger and searched existing branches and
PRs for `MeteredStorageCursor`/storage classification; no duplicate trial was found.
The separate empty-overlay experiment remains tracked in #3543; projection
work is tracked in #3542 and hosted coverage of this read lane in #3541.

Tooling-friction: isolated Cargo targets preserve correct source provenance,
but rebuilding the large benchmark consumer still costs about three minutes.
