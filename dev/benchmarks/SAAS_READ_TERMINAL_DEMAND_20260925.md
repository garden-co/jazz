# Skip unused source-membership lowering for initial reads

## Finding and change

Twelve fresh-process CPU samples on #3546 contain 3,727 initial-SELECT
observations. Query lowering accounts for 818 (21.9%); terminal lowering
accounts for 206 (5.5%). These inclusive scopes overlap. Repeated warm reads
had hidden most of the compiler work. Schema hashing is only 54 observations
(1.4%), so it is not the main setup cost.

Although #3085 stopped requesting unused sync witness outputs for one-shot
reads, terminal construction still builds their child-source membership and
recursive contributor graphs. Policy authorization subqueries do this too,
even though they return only authorized root IDs and binding routes.

`RowSetOutputRequest::requires_source_membership` now distinguishes those
root-only outputs from outputs consuming source membership. Application rows
and `AuthorizedRows` skip child-source/witness construction. Every other fact
role conservatively keeps the existing complete construction path. This is
output-demand propagation in the compiler, not removal of witness functionality.

Required-include root gates run before the guard. The root's predicates,
permission evaluation, recursive traversal, route fields, public projection,
and collector remain intact. Maintained membership, version/replacement
witnesses, coverage, and other fact roles retain their source closures. An
unrecognized future fact role also retains the full path. No public Jazz API,
storage/wire encoding, history or deletion semantics change.

For example, an application query may return an owned document whose group
is hidden. A required group include must still suppress that document; an
optional include keeps it. Changing group permission or deleting the group
must affect the next read. Separately, a retained source-coverage request still
constructs the exact admitted contributor graphs needed by its receiver.

## Matched native results

Parent: #3546, `7c6818eeddd1050bb479e6ea593492f8d8e61c8e`.
Native RocksDB `[profile.perf]`, identical `cold-settle-attribution` features,
fixed member identity, no deleted rows. Each observation reopens a fresh copy
using seeded node 1. Store copy/open, preparation and result encoding are
outside SELECT timing; query lowering, permissions and normalization are
inside. OS caches are warm. Diagnostics, builds and tests are off during timing.

| Read sweep                                   | Parent median | Candidate median |               Speedup |
| -------------------------------------------- | ------------: | ---------------: | --------------------: |
| 39 SELECTs, at most 100 rows each / 879 rows |    162.747 ms |       145.574 ms |             **1.12×** |
| 39 unbounded SELECTs / 27,518 rows           |    299.989 ms |       292.248 ms |             **1.03×** |
| Largest unbounded SELECT / 23,831 rows       |    157.396 ms |       158.017 ms | effectively unchanged |

Each workload has six observations per arm, ordered ABBAAB then BAABBA.
The page sweep improves 1.121× and 1.115× in the two rounds. The unbounded
sweep improves 1.035× and 1.018×. All 39 encoded result lengths/hashes and the
independently calculated authorized UUID order match in every observation.
These are gains relative to #3546, not new combined-stack measurements.

| Workload / arm        | All SELECT times (ms)                                |
| --------------------- | ---------------------------------------------------- |
| Pages / parent        | 162.224, 159.714, 170.049, 167.592, 160.205, 163.269 |
| Pages / candidate     | 143.406, 147.795, 144.669, 148.310, 146.479, 142.504 |
| Unbounded / parent    | 302.300, 300.205, 299.773, 298.114, 297.807, 302.335 |
| Unbounded / candidate | 295.732, 289.843, 290.082, 291.608, 292.888, 299.671 |

Every query emits phase attribution. The page sweep's exclusive lowering
median falls **81.579 → 65.044 ms (20.3%)**; installation is **41.602 → 41.429 ms**
and normalization **3.039 → 3.035 ms**. Unbounded lowering falls
**59.906 → 51.520 ms (14.0%)**, while installation stays about 175 ms.
Separate phase medians do not necessarily add to the total median.

The narrower first trial skipped construction only when the fact set was
empty. Policy authorization still constructed its unused contributors, and
12 unbounded observations were essentially unchanged. The retained version
also recognizes `AuthorizedRows` as consuming only the filtered root. The
first trial is preserved in `target/saas-read-terminal-demand-ab/`; it is not
presented as another win.

## Validation and provenance

A new public-API integration test pins optional versus required includes with
visible, unauthorized, missing and deleted targets, including a permission
change between reads. Existing assertions and thresholds are unchanged.
All six shared-query hydration tests and all three incremental-delivery
canaries pass. The exact ten-seed maintained-vs-one-shot oracle at churn depths
10/1,000 passes in 30.56 seconds (one executed test, zero ignored).
Clippy for the Jazz library and affected integration target passes with
`-D warnings`; scoped rustfmt passes.
Full canonical and hosted acceptance remain required before landing. The
private sensitive-data guard is unavailable locally; fixtures are anonymized.

Preflight read the rejection ledger and searched preserved branches and open/
closed PR descriptions, including #3085 and #2882. The changed premise from
#3085 is the remaining construction after output roles have already been
removed. This does not resurrect the rejected graph-memo/source-certificate
experiments.

Local artifacts under `target/saas-read-terminal-source-demand-ab/` contain
both binaries, the exact source/test patch, all 24 JSON/log pairs, scripts,
phase summaries and manifest. Fresh-process CPU scripts, 12 raw samples and
exact result receipts are in `target/saas-read-cold-cpu/`. CPU-sampled timings
are diagnostic only and are not mixed into the endpoint comparisons above.

SHA-256:

- Parent binary: `7ffc58ab4068fa08be71c9836d83b2130929d02df58fe2b63536cc6203196298`.
- Candidate binary: `fd6f7efd53abb0e0e45ef8b0bb16d376794db83c41dc6450b6b944f39ab19696`.
- Source/test patch: `1d544358afef2255018d41d9eb6b4585cb30abd2137a49d5de3185dcb2797da0`.

Follow-ups: #3542 tracks remaining copying/lowering work. #3541 tracks hosted
coverage for these exact direct-SELECT workloads; the existing first-sync
benchmark does not confirm their speedups.

Tooling-friction: sampling fresh processes exposed cold compiler work that the
warm-read loop hid; a build still costs about 2.5 minutes per experiment.
