# Authority-served unbounded SaaS reads (2026-09-25)

On the refreshed main `67316cb7f`, the complete relay workload falls from a
median **6,602 ms to 899 ms (7.34×)**. All 27,518 rows match the independent
authorized result, including exact binding bytes on the candidate.

## Why ship this separately

The first-page authority route in #3456 does not cover an unbounded query.
The same no-deletion SaaS workload still takes over six seconds to return
27,518 authorized rows through Core, a device relay, and Client. This change
extends that result-first route to flat current Global one-shot queries with
no limit. The authority evaluates under the admitted identity and claims;
the receiver gets exact binding-encoded rows without building a transient
maintained graph and receiving its supporting version closure.

Limited pages retain their 1 MiB result cap. Unbounded results have a 16 MiB
cap, and oversized results use the existing coverage path. The 32 KiB query
envelope, 64 pending-route cap, authorization checks, claim revision check,
local-write fallback, and legacy-peer fallback remain in force. Live
subscriptions, history, deleted-row semantics, and source replication are
unchanged. A result over 16 MiB currently pays for authority evaluation
before falling back; streaming or an early size estimate is future work.

## Matched end-to-end result

Native `perf` profile with `cold-settle-attribution` on main `a6925a903`
plus #3456. The public anonymized permissioned-resource fixture has no
deleted rows. Each run independently checks every result against a separate
authorized Core evaluation before the read timer. The candidate checks exact
binding bytes; the control checks exact row IDs. Both traverse the same
session-aware Core→device relay→Client transport, including postcard encode,
zstd compress/decompress, and decode on each hop. Seeding, store copy, and
oracle evaluation are excluded. The control binary differs from the
experimental candidate only by forcing the authority route off in the read
eligibility condition.

| 39 unbounded Global reads |    Run 1 |    Run 2 |    Run 3 |       Median |
| ------------------------- | -------: | -------: | -------: | -----------: |
| Receiver coverage         | 6,661 ms | 6,545 ms | 6,716 ms | **6,661 ms** |
| Authority result          |   900 ms |   933 ms |   917 ms |   **917 ms** |

The matched median speedup is **7.26×** for 27,518 returned rows. Total
binding result bytes are 13,408,002; the largest query returns 12,344,715
bytes, within the 16 MiB cap. The control spends about 2.7 s in relay ticks
and 1.0 s in Client ticks; with authority results, those phases take about
0.08 s and 0.04 s. Core ticks fall from about 2.2 s to 0.8 s. This is a
receiver-maintenance and supporting-data cost even with zero deleted rows,
not a history-only cost.

The experimental candidate binary SHA-256 is
`f9ddbe3e8629ea2375d9ea26c2bcdb70ca101ae9034ff03cc999c386e34fcc5a`;
the control is
`f2647cd01601b66e29ac6c4e2492b2498518526281d37cd0b2f1ae87b9aa50c4`.
The final implementation keeps the experimental unbounded route and restores
the original 1 MiB cap for limited pages. Its three validation runs were
**907, 893, and 882 ms** (median **893 ms**), each with the same exact bytes.
Its binary SHA-256 is
`aa37754f5f97a9040d71814248f9b127549801b3209db42925211c37844b82f4`.
The final request-limit refactor is not part of the single-condition matched
pair above; its repeated timing confirms that it did not erase the gain.
Per-run JSON receipts are in
[`saas-authority-full-20260925`](saas-authority-full-20260925/).

After #3433 (prepared-binding snapshots) and #3435 (IDB write batches)
merged, the same source was rebased onto main `67316cb7f` and rerun with
three matched relay trials:

| 39 unbounded Global reads on refreshed main |    Run 1 |    Run 2 |    Run 3 |       Median |
| ------------------------------------------- | -------: | -------: | -------: | -----------: |
| Receiver coverage                           | 6,628 ms | 6,602 ms | 6,599 ms | **6,602 ms** |
| Authority result                            |   928 ms |   899 ms |   899 ms |   **899 ms** |

The **7.34×** median gain retains the same 27,518 authorized rows and
13,408,002 binding bytes. Candidate/control binary SHA-256 values are
`c8462e4fe36a236a23f696c7df61d035fd876b4896752e16fc5f05b856550994` /
`d6c9b81feba2d672bcd8a9c255f51eba3174f7587aa4ce3d327c32da70905f84`.
The `latest-main-` receipts contain the complete phase and payload counts.

To reproduce, build `permissioned-resources-profile` with
`cargo build -p jazz-example-permissioned-resources-benchmark --bin
permissioned-resources-profile --profile perf --features
cold-settle-attribution`. Run it with `JAZZ_CUSTOMER_IDENTITY=member
JAZZ_CUSTOMER_PHASES=cold JAZZ_CUSTOMER_NO_DIAGNOSTICS=1
JAZZ_CUSTOMER_CLIENT_ONESHOT=1 JAZZ_CUSTOMER_MAX_TICKS=200000`; add
`JAZZ_CUSTOMER_EXPECT_REMOTE_READ=1` for the candidate. Leave
`JAZZ_CUSTOMER_QUERY_LIMIT` unset.

Tooling friction: optimized binaries relink for roughly two minutes when
switching the matched control branch condition; sharing Cargo artifacts
between isolated worktrees saves disk but still recompiles path-keyed crates.
