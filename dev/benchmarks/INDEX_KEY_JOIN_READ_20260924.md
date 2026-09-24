# First-result join index-key filtering, 2026-09-24

## Why ship

The no-deletion SaaS join returns 196 issues but its first Global SELECT still
hydrated 2,526 junction rows, most of which cannot join the indexed root
candidates. Compare covered UUIDs from the two declared indexes before loading
full junction rows. On the 10,000-issue fixture this reduces full current-row
reads **3,366 → 1,036** and warm Global SELECT **23.4–23.5 → 15.5–15.6 ms**,
about **1.50–1.51× faster** against the direct parent. The normal graph still
checks the join, filters, policy, and deletion state for surviving rows.

## Shipping decision

Keep this PR draft pending review of the narrow-query tradeoff. In three
sparse-tag controls the new path performs the **same** row and index reads as
the parent, but Global SELECT medians are roughly 2–8% slower. The snapshot
source returns to ordinary hydration below 512 junction entries, so there is
no extra candidate scan in these cases; the remaining overhead is query-path
setup. This is a strong win for broad junction prefixes, not a universal read
speedup. The opt-in composite index also has write and storage cost that these
read comparisons do not establish.

## Matched broad, no-deletion read

Both arms used the same public-API fixture and explicitly declared indexes
`issues(project, state)` and `issueTags(tag, issue)`: 10,000 issues, 20,170
junction links, 30,253 total rows, globally accepted mergeable seed batches,
`SELECT true`, no deletions, and an independent result oracle. Seed, prepare,
and SELECT are measured separately. Each process runs seven reads; medians use
samples 1–6 with RocksDB and OS caches warm from seeding. Four final-source
processes ran sequentially in A/B/B/A order on one machine.

| Process            | Warm Global q2 SELECT median | Full current rows | Index entries | Results |
| ------------------ | ---------------------------: | ----------------: | ------------: | ------: |
| Direct parent A1   |                    23.396 ms |             3,366 |         3,356 |     196 |
| Final candidate B1 |                    15.540 ms |             1,036 |         4,186 |     196 |
| Final candidate B2 |                    15.630 ms |             1,036 |         4,186 |     196 |
| Direct parent A2   |                    23.506 ms |             3,366 |         3,356 |     196 |

The candidate reads **2,330 fewer complete rows** and **830 additional index
entries**. Deletion-register reads are zero in both arms. Local q2 remains
about 20.2–20.5 ms; q1 and the exact-ID point join have no established win.
The measured gain is first-result Global SELECT execution, not startup,
retained-subscription writes, or network delivery.

The parent is commit `df5a751b7` with only the identical benchmark schema
flag applied, built in a separate Cargo target. Parent optimized binary
SHA-256: `d3f7e2cd648c244ef507c24570f3a26b2fafebcb1306efb1165fab7c5cdb0ec9`.
Final candidate binary SHA-256:
`330f48053d378c3752a78a108065ec8b4d1893dd9068df3d62081056a5f907b1`.
The final binary includes the bounded-probe guard and passing expanded test.

The command for each broad process was:

```sh
JAZZ_S1_COMPOSITE_EQ=1 JAZZ_S1_COMPOSITE_JOIN=1 \
JAZZ_S1_READ_RECEIPT=1 JAZZ_S1_READ_POLICY=1 \
JAZZ_S1_ORGS=1 JAZZ_S1_ISSUES_PER_ORG=10000 \
<arm's optimized s1_saas binary>
```

## Sparse controls and index cost

The same benchmark with 1,000 issues, one project, and many tags makes the
junction prefix small relative to the root prefix. The final candidate skips
its candidate-index scan below 512 source entries. In the following A/B/B/A
runs, each arm read identical full rows and index entries for the same result:

| Tags | Parent Global q2 medians | Candidate medians | Full rows / index entries |
| ---: | -----------------------: | ----------------: | ------------------------: |
|  100 |         3.797 / 3.922 ms |  4.090 / 4.067 ms |                 455 / 454 |
|   50 |         4.131 / 3.994 ms |  4.245 / 4.153 ms |                 475 / 474 |
|    5 |         8.381 / 8.280 ms |  8.614 / 8.709 ms |                 841 / 840 |

Before the guard, the 100-tag case saved only 11 full rows while scanning 437
extra index entries. The guard removes that wasted scan. For larger junction
prefixes, candidate scanning stops after half the source-entry count and falls
back to ordinary hydration when the root set is broad.

A small seed-cost probe used the parent binary with the extra junction index
absent or present: 13.893/14.136 s without it versus 13.840/14.141 s with it
in two comparable runs. This does not establish a write-cost difference;
index space was not measured. Applications must explicitly declare the
composite index for frequent broad joins where the read gain earns that cost.

## Scope and invariants

The snapshot-only path requires one simple join, a safe indexed root equality,
a declared junction composite index whose first column has a bound equality
and whose second column is the join FK, Global current reads, and no LIMIT,
ORDER BY, or aggregate. A declared composite index can be used by its leading
equality without a separate single-column index. Local reads and retained
subscriptions keep live sources so changes can enter or leave either prefix.
History, deletion/restore, and authorization behavior are unchanged.

Tooling friction: a shared Cargo target contaminated the first parent binary;
row-read counters exposed it, so this receipt uses an isolated control build.
A second full-workspace test-index build filled the task's temporary disk
allocation; clearing only ignored generated targets recovered space.
