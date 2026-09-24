# Composite equality first-result read, 2026-09-24

## Why this experiment

The 10,000-issue SaaS `q2_join` returned 196 issues but read 1,967 full issue
rows and 2,526 full junction rows. Jazz already stored a declared
`(project, state)` index, but its initial-read equality selector considered only
single-column indexes. This trial uses both equalities to narrow the root
source before the ordinary join graph runs. The junction source is unchanged.

## Matched receipt

Both arms used the same public schema with an opt-in `issues(project, state)`
composite index, 10,000 issues, 20,170 issue-tag links, 30,253 total rows,
globally settled mergeable seed batches, `SELECT true`, no deletions, and an
independent result oracle. `q2_join` filters project and state, requires a tag
link, includes the project, and returns 196 rows. Timings are warm Global
SELECT medians of samples 1–6 per process; seed, prepare, and query are
separate. All four processes ran sequentially in A/B/B/A order on one machine.

| Process      | q2 Global SELECT | Execution phase | Full current rows | Index entries |
| ------------ | ---------------: | --------------: | ----------------: | ------------: |
| Parent A1    |        30.801 ms |       23.721 ms |             4,503 |         4,493 |
| Candidate B1 |        22.787 ms |       18.052 ms |             3,366 |         3,356 |
| Candidate B2 |        22.729 ms |       17.649 ms |             3,366 |         3,356 |
| Parent A2    |        30.808 ms |       23.591 ms |             4,503 |         4,493 |

The direct-parent improvement is **1.35×** for this declared-index query.
The extra index can add write and storage cost; this experiment did not
isolate that cost. Without the declared composite index, this new path is not
selected. The point join and `q1` filter/include query showed no established
gain in this comparison. All deletion-register reads were zero.

The parent binary was built from `1f4bed700` plus the identical benchmark
fixture flag, SHA-256
`8afb8a66474eaf131173268329e1abaa8d2dd72f1a88599951cf69743149ebd5`.
The candidate's optimized binary SHA-256 was
`f3d564ac8226bdcb02d3ba32bda55f444197d39278e3146ce773f489a6ca16a2`.
The exact command for each process was:

```sh
JAZZ_S1_COMPOSITE_EQ=1 JAZZ_S1_READ_RECEIPT=1 \
JAZZ_S1_READ_POLICY=1 JAZZ_S1_ORGS=1 JAZZ_S1_ISSUES_PER_ORG=10000 \
<arm's optimized s1_saas binary>
```

Tooling friction: selecting one S1 read query and repeating it required a
temporary local harness patch for CPU sampling; a maintained query/sample
selector would shorten future profiles.
