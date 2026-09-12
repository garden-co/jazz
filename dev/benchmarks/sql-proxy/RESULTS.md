# First shallow-history comparison

The same exported synthetic fixture was loaded into SQLite 3.45.1 and PostgreSQL
17.10 on the development host. Each number below is the median of five passes,
with variant order rotated and complete results fetched. Databases were seeded
and analyzed beforehand; these are cache-warm reads, not cold disk reads.

| Work across all 39 queries                         |   SQLite | PostgreSQL |
| -------------------------------------------------- | -------: | ---------: |
| Current rows + recursive permissions               |  30.6 ms |    31.3 ms |
| Accepted versions + successor checks + permissions |  74.8 ms |    57.3 ms |
| Above + conservative supporting bundles            | 168.0 ms |   121.5 ms |

Measured ranges respectively: SQLite 30.3–37.3 / 73.9–78.8 / 166.5–176.3 ms;
PostgreSQL 30.4–39.6 / 56.8–65.7 / 120.8–122.2 ms.
The dominant child query alone took 24.6 / 41.1 / 111.1 ms in SQLite and
21.4 / 33.8 / 75.9 ms in PostgreSQL.

## What is held equal

- 46,740 seeded application rows, **one accepted version per row**, no predecessor
  versions. Each seed row is authored/finalized in its own mergeable transaction
  in the Rust fixture. This statement does not count internal metadata records.
- 39 independent queries, 27,518 total output rows, 23,831 in the dominant child.
- Same row UUIDs, full user fields, group graph, depth bound and grant conditions.
- 33,104 conservative supporting entries summed over queries. Shared entries
  between queries are counted again; there is no cross-query permission cache.
- All IDs and fields agree with the Rust generator's expected visibility; support
  coordinates/payloads agree with an independent graph oracle. Every per-query
  hash agrees across engines and rounds.
- Untimed mutations exercise accepting a successor and withdrawing permission
  seeds. The empty parent relation is not used to elide version selection.

## Interpretation

Adding shallow-history selection costs about **26–44 ms**, and adding support
set construction/fetch costs another **64–93 ms**. The work is real, but this
experiment does not support seconds of intrinsic cost for shallow history and
these permission relationships. It strengthens the case for investigating work
amplification in Jazz's implementation and distributed loading pipeline.

It does not locate all that amplification. The SQL proxy does not implement
Jazz's full distributed transaction validation, per-node persistent ingestion,
long-lived IVM state, branch conflicts or complete author/schema envelopes.
Its supporting set is semantically sufficient for this synthetic read, but is
not a byte-for-byte oracle for Jazz's selected provenance. In particular,
**the ratio to a full three-runtime Jazz cold synchronization is not an engine
speedup measurement**.

Useful next comparison: count and time work at the same boundaries in Jazz—
authoritative query/support production, then transfer and downstream replay—
before assigning the unexplained difference to query lowering alone. This
reference shows a plausible low-cost execution of the shallow relational work;
it does not yet tell us which Jazz stage can attain that cost.

## Jazz timing boundary correction

A fresh same-workload Jazz checkpoint took **12.935 s total**, of which
**11.224 s was settling**; the preserved pre-export runtime binary took
13.428 s total / 11.618 s settling. The first fresh pass was slower at
15.360 s total / 13.590 s settling, so these are checkpoints, not a stable
new median. Earlier approximately 10.94 s figures described settling rather
than the complete connect/subscribe/settle wall time. Do not label them total
cold-load latency. The experiment adds only an opt-in export path, not a
runtime optimization.

## Reproduction notes

See README for commands and timing boundaries. Local raw receipts are preserved
under `/home/ubuntu/jazz-debug-evidence/permissioned-profile/sql-proxy/`:
`fixture.json`, `sqlite-final.json`, `postgres-final.json` and
`postgres-plans.json`. The last captures plans with per-node timing disabled:
ordinary EXPLAIN ANALYZE's per-node clocks substantially distorted this host's
execution times, so plan timings must not replace uninstrumented fetch timings.
No owned build or other benchmark ran concurrently with these final SQL rounds.
