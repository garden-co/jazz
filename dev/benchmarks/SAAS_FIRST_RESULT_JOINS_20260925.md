# Stream first-result join probes

## Result

Native `[profile.perf]`, the anonymized permissioned-resources fixture, member
identity, all 39 initial unbounded SELECTs, 27,518 authorized rows, no deleted
rows. A fresh database is opened for every sample, using the seeded node's own
identity. Seed/copy/open and result hashing are outside SELECT time; query
lowering, permission evaluation, and public row normalization are inside it.

| Read                                       | Parent median | Trial median | Speedup |
| ------------------------------------------ | ------------: | -----------: | ------: |
| All 39 SELECTs                             |    506.099 ms |   369.842 ms |   1.37× |
| Largest SELECT, 23,831 rows                |    346.645 ms |   221.651 ms |   1.56× |
| Initial graph execution/install, exclusive |    376.859 ms |   244.089 ms |   1.54× |

Order: A/B/B/A/A/B, three samples per arm. All samples retained:

| Arm | All SELECTs (ms)          | Largest SELECT (ms)       |
| --- | ------------------------- | ------------------------- |
| A   | 504.106, 506.099, 525.582 | 345.334, 346.645, 363.231 |
| B   | 370.112, 369.842, 367.834 | 221.651, 220.142, 222.839 |

Every run checks the exact authorized UUID ordering against the fixture.
All 39 encoded result lengths and hashes match across all six samples.
These are local native receipts; they do not measure browser execution or
the six-second retained-subscription workload tracked in #3535.

## Cause and change

One-result reads used the incremental join machinery to build both input
arrangements. Completion then discarded that private state. A snapshot inner
join only needs to probe the right index while streaming the left records.
Snapshot semi/anti joins likewise do not need left buckets or visibility state
for future retractions.

This trial marks only `FirstResult` subscription hydration as eligible. Root
snapshot joins stream their left input through the existing right arrangement.
An Arrange node still builds its index if any consumer needs it on the right,
including self-joins, or if an aggregate/non-root collector needs it.
Recursive scopes and retained subscriptions keep their existing evaluation.
Generic `query_graph` hydration is also unchanged: that path can install state.

Diagnostic runs of the largest SELECT show:

| Arrangement work                              |      Parent |   Trial |
| --------------------------------------------- | ----------: | ------: |
| Snapshot arrangement constructions            |          34 |      21 |
| Input record visits                           |     247,126 |  25,886 |
| Encoded input bytes presented to construction | 114,219,909 | 885,494 |

Five 43,000-row arrangements disappear. These byte counts describe work over
encoded inputs, **not allocated bytes or peak memory**: record payloads can be
shared. Diagnostic hashing is enabled only for these count runs, not the timing
comparison. The endpoint saving is 136 ms, about 27% of full SELECT time.

## Combined result with public-row layout reuse

A second matched A/B/B/A/A/B run compares the pre-layout parent from #3534
(`49e8f9721` plus the diagnostic driver/spans) with this trial. Both arms open
the copied store as node 6, matching the older driver's fixed identity.
Recovery/open remains outside the SELECT timer. This comparison is distinct
from the same-node experiment above, so its absolute times are not pooled.

| Read           | Before both changes | After both changes | Speedup |
| -------------- | ------------------: | -----------------: | ------: |
| All 39 SELECTs |          665.030 ms |         376.376 ms |   1.77× |
| Largest SELECT |          498.514 ms |         218.654 ms |   2.28× |

All SELECT samples: A = 665.030, 664.724, 687.166 ms;
B = 382.008, 363.496, 376.376 ms. Largest SELECT samples:
A = 486.149, 498.514, 504.363 ms; B = 219.398, 217.104, 218.654 ms.
All 39 encoded result receipts match across all six runs. This is a measured
combined gain, not a product of separately measured ratios. It covers the
layout and join changes; it is not a retrospective measurement of every
earlier history/deletion optimization in the project.

The older baseline binary SHA-256 is
`04a3fff9ed022ab3ef054dcbdc6dc7c9353bcad88a5d92c031fa7c7ff1d676ae`,
preserved with its source patch in `target/saas-read-materialization-ab/`.
`run_combined.py`, `combined-*.json`/logs, and `combined-summary.json` in this
experiment's artifact directory preserve the new comparison.

The changed premise from rejected #2864/#2947 is elimination of unused
maintenance state for a finite consumer, rather than a different container
for state that still has to be constructed. Follow-up context: #3538.

## Semantics and edge cases

- The right lookup and key encoder are shared with the maintained execution.
  Final consolidation preserves signed weights and duplicate projected rows.
- Array keys keep their existing distinct-key expansion. Two occurrences of a
  left row matching two keys still contribute weight four to a semi join.
- Nullable keys and policy numeric equality retain their existing semantics.
- A skipped left arrangement no longer resolves indirect keys itself; the
  join resolves those keys before probing and can suspend for missing chunks.
  Selected payload values still materialize through the existing read path.
- First-result session state is never installed over live consumer state.
  Cancellation discards the private session. Subsequent retained deltas still
  use the maintained indexes and threshold visibility state.

No history, deletion, permission, query, or subscription functionality is removed.

## Source and binary receipt

Parent: `c542d619a15f5a0041e99199aa340cc0fc791ece` (PR #3539).
The first comparison isolates this change; the second measures it together
with the result-layout improvement.

- A SHA-256: `4e9b9a6ee01b8570bbfa64f1f37e900b459f70da5f9c02890d120f881249cca4`
- B SHA-256: `69c15a2ccac57339d9eb0b8704b0c83fef5245d49c6b7e0b2aa6ccd180fb6e17`

Local artifact directory: `target/saas-read-joins-ab/` in the primary checkout.
It contains `baseline-binary`, `candidate-binary`, `candidate-source.patch`,
`run_ab.py`, all six JSON/log pairs, `ab-summary.json`, and separate arrangement
diagnostic receipts. Each worktree uses its own Cargo build directory.

```sh
CARGO_TARGET_DIR="$PWD/target" cargo build \
  -p jazz-example-permissioned-resources-benchmark \
  --bin permissioned-resources-profile --profile perf \
  --features cold-settle-attribution

JAZZ_CUSTOMER_INITIAL_SELECTS=1 \
JAZZ_CUSTOMER_IDENTITY=member JAZZ_CUSTOMER_PHASES=cold \
JAZZ_CUSTOMER_NO_DIAGNOSTICS=1 JAZZ_CUSTOMER_REOPEN_SEEDED_NODE=1 \
  target/perf/permissioned-resources-profile
```

For arrangement counts, add `JAZZ_CUSTOMER_ONLY_TABLE=res_l_child_3` and
`GROOVE_TRACE_ARRANGEMENT_SNAPSHOTS=1`. Do not use those tracing timings as A/B
latency. The benchmark's per-table phase trace is reset around the SELECT API.

## Validation

- `cargo test -p groove`: 1,035 tests/doc tests passed; four existing ignored.
- Five new public-API regressions cover duplicates/array expansion, nullable
  and numeric comparison, a shared self-join lookup, retained semi/anti updates
  after one-result reads, and suspended/cancelled large join-key reads.
- All 14 Jazz tests in `bulk_read_materialization`, `shared_query_hydration`,
  `large_value_read_scaling`, and the three incremental-delivery canaries pass.
- The exact ten-seed maintained/one-shot differential oracle passes with churn
  depths 10 and 1000: one test executed, zero ignored, 30.81 seconds.
- Full hosted correctness and realistic benchmark gates remain required before
  landing. No full local CI-equivalent result is claimed.

Tooling-friction: a prebuilt per-worktree native profiling target would remove
the roughly three-minute compile between executor experiments.
