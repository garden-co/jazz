# Pass empty transaction-overlay scans directly to storage

## Mechanism and scope

The CPU sample after #3540 included 144 observations in `OverlayScanCursor`.
Even an empty in-range staged snapshot passed every base row through a boxed
`next_entry` future, a `VecDeque`, and a newly allocated output batch.

When the captured in-range snapshot is empty, return the base cursor. This
removes the wrapper's per-row future allocation and rebatching. The production
change is six lines including comments. Nonempty overlays retain their merge
path. No history, delete, permission, transaction, or subscription functionality
is removed; no storage or wire encoding changes.

The snapshot is still captured when the scan future is created. The base scan
is still opened when that future is first polled. That distinction matters for
nested transactions: later writes to this transaction must not change its
snapshot, while the nested base transaction keeps its original opening time.
Unrelated staged keys outside the requested range do not require a merge.

## Isolated measurement

Parent #3544: `b342b10602de59e2bc3dd4cc8ff614c495cf2e3a`.
Matched native RocksDB `[profile.perf]` initial reads, member identity, all 39
unbounded SELECTs / 27,518 authorized rows, no deleted rows. Each sweep opens a
fresh copied store with the seeded node's identity; OS caches are warm.
Fixture preparation, copy/open, and result hashing are outside the SELECT
timer. Query lowering, policy evaluation, execution and normalization are inside.
No builds or other tests run during timing.

| Read                        | Parent median | Candidate median | Less time |
| --------------------------- | ------------: | ---------------: | --------: |
| All 39 SELECTs              |    355.943 ms |       350.552 ms |      1.5% |
| Largest SELECT, 23,831 rows |    208.054 ms |       202.074 ms |      2.9% |

Six observations per arm, A/B/B/A/A/B followed by B/A/A/B/B/A. This is a small
cleanup of a frequently invoked path. It is retained because it removes work
with a short early return, rather than introducing additional execution state.
It is not a major end-to-end speedup by itself.

| Arm             | All SELECTs (ms)          | Largest SELECT (ms)       |
| --------------- | ------------------------- | ------------------------- |
| A, first round  | 358.275, 361.459, 352.176 | 208.768, 208.429, 207.678 |
| B, first round  | 418.262, 350.704, 348.043 | 201.102, 201.434, 198.992 |
| A, confirmation | 347.589, 358.250, 353.636 | 201.471, 208.939, 206.581 |
| B, confirmation | 352.657, 349.525, 350.400 | 205.369, 204.391, 202.714 |

Every observation is retained, including the 418.262 ms candidate outlier.
The two rounds' total medians improve by 2.1% and 0.9%, respectively. All 39
encoded result receipts match across all twelve observations, and every run
asserts independently expected authorized UUID ordering. Full per-query phase
attribution is preserved: first-round median exclusive read installation falls
from 229.038 to 222.016 ms. Phase medians are independent of total medians.

These timings measure direct SELECTs. They do not measure relay first sync,
retained initial subscriptions, or browser WASM. Hosted coverage of this direct
read lane remains tracked in #3541.

## Direct combined-stack comparison

A separate A/B/B/A/A/B comparison measures the pre-#3539 SELECT baseline against
this candidate, including public-row materialization (#3539), first-result joins
(#3540), scan counters (#3544) and the empty-overlay bypass. It measures the
combined code directly; it does not multiply individual PR ratios. Both arms
use the older baseline driver's node-6 reopen behavior. The extra copied-store
recovery time is excluded from both SELECT timers; it remains tracked in #3537.
The UUID-page optimization #3534 is already present in this baseline.

| Read           | Before these four changes | Combined candidate | Speedup |
| -------------- | ------------------------: | -----------------: | ------: |
| All 39 SELECTs |                674.943 ms |         347.470 ms |   1.94× |
| Largest SELECT |                500.157 ms |         200.289 ms |   2.50× |

A total samples: 677.638, 674.943, 658.482 ms; B: 347.470, 347.597, 346.834 ms.
A largest-query samples: 500.157, 509.550, 490.224 ms; B: 200.519, 199.853,
200.289 ms. All encoded query receipts match. The baseline hash is
`04a3fff9ed022ab3ef054dcbdc6dc7c9353bcad88a5d92c031fa7c7ff1d676ae`;
its source/harness provenance is recorded in `SAAS_PUBLIC_ROWS_20260925.md`.
`run_combined.py`, the six JSON/log pairs and `combined-summary.json` preserve
this independent comparison and phase breakdowns in the same artifact directory.

## Validation

- `cargo test -p groove --features test`: 1,081 passed, four existing ignores.
  This ordinary parallel run includes the unchanged subscription-scaling guard
  that failed in earlier parent runs documented in #3544 / #3144.
- Four new public storage integration tests cover empty and out-of-range
  overlays, multiple batches, forward/reverse prefix and finite ranges,
  zero/short/overlarge limits, missing families, snapshot capture before first
  poll, changes after cursor creation, nested opening time, suspended opens,
  cancellation and subsequent read-your-writes.
- All 33 async hydration and three storage-residency regressions pass, as do
  all eight Jazz incremental-delivery/shared-hydration integration tests.
- Full canonical/hosted correctness and benchmark gates remain required before
  landing. This is a draft checkpoint, with no local CI-equivalent claim.

## Reproduction and provenance

Use the build and run commands in `SAAS_SCAN_METRICS_20260925.md`.

SHA-256:

- A: `e24b65936dc292facb44824e1809c809207a959032e2d2507cba03402e77008a`
- B: `2e55f51fca19d1ff28856f9b086f8b8ccdd82bf1138a28072767153b30389bcc`

The primary checkout's `target/saas-read-empty-overlays-ab/` preserves both
binaries, the production patch, scripts, twelve JSON/log pairs, per-round
summaries and `pooled-summary.json`.

Preflight reviewed the rejected ledger and existing branches/PRs. The rejected
empty join-bucket experiments #2864/#2947 concern IVM arrangements; this change
removes an empty ordered-storage transaction merge. References #3543; the
larger remaining intermediate-row projection/copy cost is tracked in #3542.

Tooling-friction: existing public controlled-storage APIs made suspension and
snapshot checks quick; the optimized native consumer rebuild took 2m57s.
