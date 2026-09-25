# Authority-served unbounded SaaS reads (2026-09-25)

On main `f629fa7a9`, the complete opt-in relay workload falls from a median
**6,652 ms to 847 ms (7.85×)**. All 27,518 rows match the independent
authorized result, including exact binding bytes on the candidate.

## Why ship this separately

The opt-in first-page authority route in #3456 does not cover an unbounded query.
The same no-deletion SaaS workload still takes over six seconds to return
27,518 authorized rows through Core, a device relay, and Client. This change
extends that result-first route to flat current Global one-shot queries with
no limit when `resultOnly: true` is requested. The authority evaluates under
the admitted identity and claims;
the receiver gets exact binding-encoded rows without building a transient
maintained graph and receiving its supporting version closure.

Limited pages retain their 1 MiB result cap. Unbounded results have a 16 MiB
cap, and oversized results use the existing coverage path. The 32 KiB query
envelope, 64 pending-route cap, authorization checks, claim revision check,
local-write fallback, and legacy-peer fallback remain in force. Ordinary
Global reads continue to populate local offline data. A successful result-only
read does not fill that cache. Live
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

After #3451 (ordered pages), #3354 (incremental attach), and the bulk ingest
changes merged, the corrected opt-in stack was rebased onto main `5b6417fe5`.
Both arms request `resultOnly: true`; the matched control differs only by
disabling its authority route in the read eligibility condition:

| 39 unbounded Global reads | Run 1 | Run 2 | Run 3 | Median |
| ------------------------- | ----: | ----: | ----: | -----: |
| Receiver coverage         | 6,378 | 6,460 | 6,292 |  6,378 |
| Authority result          |   861 |   856 |   858 |    858 |

The **7.43×** median gain retains all 27,518 authorized rows and 13,408,002
binding bytes. Candidate/control binary SHA-256 values are
`d4fd758d2fe2de036ec7ed77b57d250d6d70273fe90153e5ea5a64d2fca4f19a` /
`32e41a046b5cfe0f39386c96353d7a9376e69d925f61a87628908fab419f0c91`.
The six `main-5b6417-` receipts record row counts and phase timings.

On main `f629fa7a9`, after #3452 and #3460 merged, the same matched relay
workload was rerun three times. Both arms request `resultOnly: true`, and the
control disables only the authority eligibility check:

| 39 unbounded Global reads | Run 1 | Run 2 | Run 3 | Median |
| ------------------------- | ----: | ----: | ----: | -----: |
| Receiver coverage         | 6,652 | 6,986 | 6,461 |  6,652 |
| Authority result          |   843 |   851 |   847 |    847 |

The median gain is **7.85×** for the same 27,518 authorized rows and
13,408,002 binding bytes. Median candidate Core/relay/Client ticks are
741/69/34 ms; the control's are 2,125/2,652/1,156 ms. Candidate/control
binary SHA-256 values are
`3042a1514bab3ff67cd100c7c4191200d42c5a2deacbae96f14deac8ce38d21c` /
`c77fefb0a9c1130515e7914ab7c2d3151dfc31cf22ced08f37d73ea1d4b430f8`.
The six `main-f629fa7-` receipts contain row counts, bytes, and phases.

On main `c5e405fa0`, after the merged projection, subscription
root-position, SQLite, wire-encoding, and transaction-read changes, the stack
was rebased without conflicts. A new matched native `perf` A/B used three
alternating control/candidate runs. Both arms requested `resultOnly: true`;
only the authority eligibility check was disabled in the control:

| 39 unbounded Global reads | Run 1 | Run 2 | Run 3 | Median |
| ------------------------- | ----: | ----: | ----: | -----: |
| Receiver coverage         | 6,266 | 6,129 | 6,191 |  6,191 |
| Authority result          |   926 |   854 |   857 |    857 |

The median gain is **7.22×** for the same 27,518 authorized rows and
13,408,002 binding bytes. Candidate Core/relay/Client tick medians are
752/69/33 ms; the control's are 2,143/2,445/980 ms. Candidate/control
binary SHA-256 values are
`c8bd9c2b95689004635504d2f20fc7f27f4da5974fa56dc14546db7242f31267` /
`586300a66164044d7857487f168d165e9168f00ff19624e2431078b51d0e8093`.
The six `main-c5e405f-` receipts record rows, bytes, and phase timings.
Default reads and live subscriptions do not request result-only delivery and
are unchanged by this branch.

A separate ordinary live-subscription run on the same candidate binary,
without result-only delivery, still settled 39 subscriptions and 27,518 rows
in **6,452 ms**. Core/relay/Client tick wall times were 2,096/2,406/1,948 ms.
The largest measured exclusive phases included Core query setup (884 ms),
relay own work (686 ms), and Client storage apply (675 ms). It peaked at
3.37 GB RSS. Core processed 835,203 map/projection inputs and 601,822
keyed-join left records. Across Core, relay, and Client, the run processed
1,340,421 projection inputs and requested 818 MB of new projection-buffer
capacity for 27,518 output rows. Relay and Client each bulk-ingested 27,518
bundles. These are cumulative work counters, not peak live bytes.
This no-deletion path remains the next architectural target: it
still builds the receiver's supporting-row closure and maintained graph. The
sanitized summary receipt is `main-c5e405f-live-summary.json`; the full raw
diagnostic stays outside the public repository because it includes host
metadata.

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
