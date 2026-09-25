# Carry only demanded fields through permission existence checks

## What the trace found

On #3545, the largest initial SELECT returns 23,831 authorized rows (12,344,715
encoded bytes). Its authorization branch carries wide text/JSON payloads for
43,000 source rows through a nullable-parent check, an identity-binding join,
a field reorder and a semi join. It then retains only row UUID and account.
Four standalone projections each process all 43,000 rows, including unused
payload. Standalone map output totals 104,096,379 bytes; completed fused-pipeline
output is separately 867,264 bytes.

The existing compiler narrowed inner joins and the right side of existence
joins, but consumer field demand stopped at the left side of a semi join and
at nullable unwraps. Compiler-generated selections also bypassed the ordinary
plain-field composition rule. This is why relaxing runtime unary fusion did
not remove those four maps; that earlier trial was rejected (#3542).

## Change and contracts

- Apply the existing plain-field composition rule to generated selections too.
  Constants, enum translations and other fallible projections remain barriers.
- For a projected semi join, retain the left matching keys and downstream
  fields, then propagate that demand into eligible inner joins and plain maps.
- For a projected nullable unwrap, retain its tested field even when the
  consumer omits it. Narrow the other columns before unwrapping.
- Build consumer-specific nodes and arrangements. Independent readers of the
  original full row keep their own unchanged output.
- Preserve comparison modes, bag weights, resolved field identities, root
  ordering and every upstream error/filter. No storage/wire encoding change,
  history/deletion change, authorization relaxation or public Jazz API change.

Anti joins remain a propagation boundary. The first version also narrowed them,
but that duplicated the shared current-row deletion check: left join visits
rose from 221,854 to 264,854. Keeping that shared check restores 221,854 visits
and reduces standalone map output further, from the first trial's 61,498,162
bytes to **39,102,874 bytes**. The candidate has one wide 43,000-row map; its
other large permission projections carry two or three fields.

A semi join is linear in its left bag. Two source rows with different discarded
payloads can become the same key with weight two. Deleting one must leave weight
one. Duplicate right matches must continue to authorize that key until the last
match disappears; replacing the last match in a single commit must not flicker.
A discarded nullable field still filters nulls. A discarded invalid constant or
enum conversion still fails before a later null or existence filter can hide it.

## Matched native initial SELECTs

Parent production: #3545, `c6804d8d50cf3a823cb08286658f62774913d6fc`.
Both binaries include the same optional diagnostic additions. Native RocksDB
`[profile.perf]`, member permissions, 39 unbounded SELECTs / 27,518 rows, no
deletes. Each sweep opens a fresh copy using the seeded node's identity; OS
caches are warm. Copy/open/recovery, preparation and result hashing are outside
the SELECT timer. Lowering, permission evaluation and normalization are inside.
No builds or test suites run during timing; graph tracing is disabled.

| Read           | Parent median | Candidate median | Speedup |
| -------------- | ------------: | ---------------: | ------: |
| All 39 SELECTs |    379.868 ms |       319.555 ms |   1.19× |
| Largest SELECT |    226.978 ms |       169.303 ms |   1.34× |

Nine observations per arm across A/B/B/A/A/B, B/A/A/B/B/A and A/B/B/A/A/B.
The third round checks the higher parent times observed during confirmation.
The total speedup by round is 1.13×, 1.19× and 1.20×. Every observation is
retained, including candidate outliers; absolute timings show host variation.
All 39 exact encoded result lengths/hashes match across all eighteen runs,
and the fixture independently asserts authorized UUID ordering.

| Arm / round      | All SELECTs (ms)          | Largest SELECT (ms)       |
| ---------------- | ------------------------- | ------------------------- |
| A, first         | 356.325, 366.923, 364.553 | 204.161, 213.708, 214.595 |
| B, first         | 339.943, 323.163, 319.555 | 166.477, 171.488, 169.303 |
| A, reverse order | 387.238, 379.868, 380.753 | 226.978, 230.227, 230.169 |
| B, reverse order | 318.800, 345.658, 314.713 | 168.212, 189.927, 166.981 |
| A, repeat        | 382.915, 386.140, 377.426 | 230.808, 227.355, 223.342 |
| B, repeat        | 319.127, 338.588, 318.122 | 169.824, 183.377, 168.648 |

Exclusive read installation/execution median: **246.956 → 188.515 ms** across
all queries. Graph compilation increases **10.234 → 11.558 ms**; normalization
is effectively unchanged (**37.330 → 37.255 ms**). These are separate medians,
not components that must add to the total median. Every query emits its own
complete phase breakdown.

Separate diagnostic runs show standalone map output **104.10 → 39.10 MB**
(62.4% less); fused final output stays **0.867 MB**. These counters exclude
scratch rewrites and join copies. They measure cumulative encoded work, not
peak memory, allocation count, or total copies. Standalone map elapsed work
is 37.15 → 17.84 ms in those diagnostic runs; that is attribution, not another
endpoint comparison. The graph has 104 reachable nodes versus 98: less wide
row work, despite a few additional small projections.

## Direct combined-stack measurement

A separate six-run A/B/B/A/A/B comparison starts before #3539 and ends at this
candidate, including row materialization, first-result joins, scan metrics,
empty overlays and field demand. Both arms use the older driver's node-6
reopen behavior. Its recovery cost is outside both SELECT timers (#3537).
#3534's UUID-page optimization is already present in both arms. This measures
the stack directly; it does not multiply independent improvement ratios.

| Read           | Before #3539 | Combined candidate | Speedup |
| -------------- | -----------: | -----------------: | ------: |
| All 39 SELECTs |   681.946 ms |         317.144 ms |   2.15× |
| Largest SELECT |   513.019 ms |         167.917 ms |   3.06× |

A totals: 681.946, 765.056, 680.072 ms; B totals:
306.963, 362.36, 317.144 ms. A largest-query samples:
506.394, 575.489, 513.019 ms; B largest:
164.058, 209.838, 167.917 ms. All encoded results match.
Neither comparison measures the retained initial-subscription workload or WASM.

## Validation / draft

Five new public Groove integration tests cover discarded-payload edits reaching
an independent full-row reader, live duplicate/retraction transitions, same-tick
right replacement, null filtering, array-key multiplicities and empty arrays,
unnamed slots/nested fields, and discarded fallible constants/enum conversions.
They compare live and first-result behavior using public schema/query builders.
Existing tests and thresholds are unchanged.

The first parallel Groove suite ran alongside an optimized build and failed the
existing subscription-install timing guard (#3144), with small 85.875 µs / large
1,994.208 µs. Its 859 other library tests passed. The quiet ordinary parallel repetition passes **1,085 tests** with four existing
ignores, including that unchanged timing guard. The final focused projection
suite passes all five new tests (the fifth case was added after the full run).
All three Jazz incremental-delivery canaries and five shared-query hydration
regressions pass. The exact ten-seed maintained-vs-one-shot oracle at churn
depths 10/1,000 passes in 32.46 seconds (one executed test, zero ignored). Full canonical and hosted
correctness/performance gates remain required before landing;
no local CI-equivalent result is claimed. The optional private sensitive-data
guard is unavailable; all fixtures are anonymized.

## Reproduction and provenance

Build using the commands in `SAAS_SCAN_METRICS_20260925.md`. Enable
`JAZZ_CUSTOMER_READ_WORK=1` for per-operator counters and projection plans,
and `GROOVE_TRACE_READ_GRAPH=1` for the reachable graph/consumer topology.
Those diagnostics require `cold-settle-attribution`; keep them disabled during
latency comparisons. The added pipeline-output counter makes the separate
fused output visible instead of incorrectly treating it as avoided copying.

SHA-256:

- Matched parent: `c2206b17b4ee85c4c884d5a6e8fc1dc907daf00b4992dfaedaa0b0e7a36b1d41`.
- Candidate: `7ffc58ab4068fa08be71c9836d83b2130929d02df58fe2b63536cc6203196298`.
- Combined earlier parent: `04a3fff9ed022ab3ef054dcbdc6dc7c9353bcad88a5d92c031fa7c7ff1d676ae`
  (source/driver provenance in `SAAS_PUBLIC_ROWS_20260925.md`).

`target/saas-read-projection-topology/` preserves the parent diagnostic source
patch, binary, graph and trace. `target/saas-read-column-demand-shared-ab/`
preserves candidate source/test snapshots, both binaries, all eighteen native
and six combined JSON/log pairs, scripts, graph/operator traces and summaries.
The intermediate anti-join trial is preserved separately in
`target/saas-read-column-demand-ab/`: twelve runs gave 349.366 → 325.080 ms
overall and 201.183 → 173.958 ms on its largest SELECT. These absolute timings
are from an earlier measurement period and should not be mixed with the final
paired comparison.

Preflight reviewed the rejected ledger, preserved branches and open/closed
PRs including #2828, #2882, #3217 and #3219. This targets unused payload in
existence checks and preserves the previous plain-field composition boundary;
it does not repeat the rejected broader prepared-expression composition.
Remaining projection/copy work is tracked in #3542; hosted direct-SELECT
coverage is #3541.

Tooling-friction: graph fanout plus separate fused-output bytes made the actual
copy path visible; each optimized consumer rebuild still costs about three minutes.
