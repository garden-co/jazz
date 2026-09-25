# Permissioned SaaS initial-read investigation (2026-09-25)

## Decision

Prioritize an **authoritative result-first read path** for remote initial reads.
Core should evaluate the first snapshot with its one-shot plan and publish the
authenticated result before the Client has
replayed the complete supporting version closure and built its own maintained
graph. Continue source synchronization and live-query catch-up separately.
Keep the present receiver-local path for local-write composition and as the
correctness fallback until the handoff is proven. This is a proposed product
change, **not a measured product speedup**.

The next implementation should first target one-shot reads and the first reset
of a remote subscription. It should preserve the identity/claims binding and
authority revision in the result receipt, then switch to local maintained
deltas only after the receiver catches up to that revision. Deletions,
reordering within a limited query, permission revocation, disconnect/replay,
and concurrent local writes need black-box tests before the switch is enabled.

## Fixture and reproducibility

This uses the public-API, anonymized permissioned-resource fixture at scale
1.0, identity `member`, RocksDB with WAL/no sync, native Cargo `perf` profile, and
zstd transport. It has 39 subscriptions and 27,518 visible rows. The dominant
child table has 43k physical rows, of which 23,831 are member-visible through
36 parent resources. All timings exclude seeding. The isolated integration
worktree is `d06df18ad` (top read stack `46b32eecf` merged with then-current
main `fe34ff553`); these results do not describe unmerged `main` alone.

The benchmark adds `JAZZ_CUSTOMER_ONLY_TABLE`, `JAZZ_CUSTOMER_QUERY_LIMIT`,
`JAZZ_CUSTOMER_DIRECT_CORE`, and `JAZZ_CUSTOMER_SERVER_ONESHOT` controls. The
limited subscription and one-shot shapes are both built with public
`Query::from(table).limit(n)`. Result membership is checked against the
independent visible-row oracle. The one-shot lane uses public
`Db::all_for_identity` at Global durability on a copied seeded RocksDB store;
copy/open time is excluded. `JAZZ_CUSTOMER_SIMULATE_RESULT_WIRE` additionally
extracts application cells, postcard-encodes them, zstd-compresses them,
decompresses and decodes them, and checks roundtrip equality. It does not
implement a network protocol or Client persistence. The clean comparison
binary SHA-256 is `dd28bc0753bbc6c8bd88438d04f7dd5be01e6f592f16c7afddd5a867ae6bf564`.
The committed JSON receipts omit the local hostname; measurement fields are unchanged.

Build `permissioned-resources-profile` with
`cargo build -p jazz-example-permissioned-resources-benchmark --bin permissioned-resources-profile --profile perf --features cold-settle-attribution`.
For the maintained 100-row lane, run the binary with
`JAZZ_CUSTOMER_IDENTITY=member JAZZ_CUSTOMER_PHASES=cold JAZZ_CUSTOMER_NO_DIAGNOSTICS=1 JAZZ_CUSTOMER_DIRECT_CORE=1 JAZZ_CUSTOMER_ONLY_TABLE=res_l_child_3 JAZZ_CUSTOMER_QUERY_LIMIT=100`.
For the trusted-host comparison, replace the diagnostics and direct flags with
`JAZZ_CUSTOMER_SERVER_ONESHOT=1 JAZZ_CUSTOMER_SIMULATE_RESULT_WIRE=1`.
Omit `JAZZ_CUSTOMER_QUERY_LIMIT` for the full child case.
Add `JAZZ_CUSTOMER_SERVER_ONESHOT_ALL=1` and omit
`JAZZ_CUSTOMER_ONLY_TABLE` to run all 39 trusted-host reads in one process.

## Matched observations

| Child query | Maintained Core→Client settle | Trusted-host read + app-cell roundtrip | Ratio, diagnostic only |
| --- | ---: | ---: | ---: |
| `limit(100)` | 700 ms | 318 ms | 2.20× |
| `limit(1000)` | 757 ms | 336 ms | 2.25× |
| All 23,831 rows | 3,025 ms | 485 ms | 6.23× |

These pairs use the same binary and fixture. Their row-set hashes match.
The result-only times omit protocol framing, connection scheduling, local
durability, local pending writes, and live-update handoff, so the ratio is an
opportunity bound, not a forecast for shipping code. Receipts:
[maintained 100](saas-read-20260925/child-direct-wire-match-100.json),
[1000](saas-read-20260925/child-direct-wire-match-1000.json),
[all](saas-read-20260925/child-direct-wire-match-all.json);
[one-shot with roundtrip 100](saas-read-20260925/server-oneshot-wire-100.json),
[1000](saas-read-20260925/server-oneshot-wire-1000.json),
[all](saas-read-20260925/server-oneshot-wire-all.json).

The 100-row result payload is 30.6 kB before zstd and 607 B after it. The
23,831-row result payload is 7.32 MB before zstd and 113 kB after it. This
synthetic fixture compresses unusually well; the byte counts are not a
general traffic estimate.

## Complete 39-query workload

A second matched binary (`e54e4a2074534c2d7f1f76cd1d6f3035a8edc52d9fa2dd9afd5c83cedc07f11f`)
ran all 39 public-API trusted-host one-shot reads in sequence on one opened
copy of the seeded store. Each result was encoded, compressed, decoded, and
checked. All 27,518 rows matched the independent authorization oracle. The
read-plus-roundtrip totals were **661, 658, and 638 ms** (median 658 ms);
the slow child took about 470 ms of the first run. App-cell payloads totalled
7.65 MB before compression and 142 kB after compression in this synthetic
fixture. [Run 1](saas-read-20260925/server-oneshot-wire-all-39.json),
[run 2](saas-read-20260925/server-oneshot-wire-all-39-r2.json),
[run 3](saas-read-20260925/server-oneshot-wire-all-39-r3.json).

On that same binary, all 39 maintained subscriptions settled in 7,104 ms
through the device relay and 4,311 ms direct Core→Client, with every expected
row delivered. [Relay](saas-read-20260925/full-relay-oneshot-match.json),
[direct](saas-read-20260925/full-direct-oneshot-match.json). Relative to the
658 ms result-only median, those are **10.8× and 6.5× diagnostic gaps**.
They are not shipped gains: the one-shot lane omits network framing,
durability, receiver-local IVM, and the catch-up protocol that a live query
needs. Direct topology itself gave a 1.65× complete-settle gain in this pair.

## Refreshed `main` control

Fetched `origin/main` on 2026-09-25 at `fe34ff553` (merge of #3332). An
isolated worktree at that revision received only the six benchmark/report
commits, ending at `d10850608`; the engine directories have no diff from
`origin/main`. The benchmark source SHA-256 matches the stacked worktree
exactly (`70d72d638f780d01925d6aff72d46df4f40639a2e08e50ed8afd54d7f1fa7259`).
The optimized control binary SHA-256 was
`40c28a3c9c71d127291ea35c0814681c6cc71d9a0a7ba00dbd6ba7d3d1ea4662`.

| Complete 39-query lane | Current `main` control | Earlier integrated read stack |
| --- | ---: | ---: |
| Core→device relay→Client settle | 6,917 ms | 7,104 ms |
| Direct Core→Client settle | 4,152 ms | 4,311 ms |
| Trusted-host one-shot read + app-cell roundtrip | 612 ms | 658 ms median |

Every lane returned the expected 27,518 authorized rows. Control receipts:
[relay](saas-read-20260925/main-full-relay.json),
[direct](saas-read-20260925/main-full-direct.json), and
[one-shot](saas-read-20260925/main-server-one-shot-all39.json).
The `main` control has one run per lane, whereas the earlier one-shot value is
the median of three. These small differences do not establish a stack-wide
gain or regression. The multi-second first-read gap remains on merged `main`.
The integrated read-stack checkout predates the proposed #3352 restack, so
these timings do not measure that future integration.

## Actual client one-shot SELECTs

The added `JAZZ_CUSTOMER_CLIENT_ONESHOT=1` lane issues the public serialized
read API from a fresh Client at strict Global tier, drives the real
Core→relay→Client or direct Core→Client links, and checks every result against
the independent authorization oracle. It measures from read request through
the returned 39 results, including coverage, receiver work, and local result
construction. The source is benchmark commit `a1c20ca86` on the unchanged
`fe34ff553` engine. The optimized baseline binary SHA-256 was
`01ff7eb082ab8abc696a8961f999594a04eb1b6f16cbc2a67267be940fb6ea69`.

| Client request-to-result | Relay | Direct |
| --- | ---: | ---: |
| All 39 plain SELECTs, 27,518 rows | 6,684 / 6,649 ms | 3,943 / 3,931 ms |
| Child `limit(100)`, 100 rows | 748 ms | 683 ms |

The clean full-result runs spent about 2.2 s in Core ticks, 2.7 s in relay
ticks when present, 1.0 s in Client ticks, and 0.68 s polling the read futures
and constructing results. The one-shot trusted-host control's 0.612 s is
therefore an **opportunity bound**, not an end-to-end speedup. For the small
page, Core ticks alone took 0.64–0.65 s and the relay added only about 0.05 s.
Optimizing relay duplication helps the full feed, while the first-page floor
requires avoiding Core's initial maintained-query hydration.

An attempted shortcut changed covered flat reads to return the existing
settled receiver subscription snapshot instead of attaching coverage and
calling `Db::all`. It returned the same authorized row sets, but **regressed**
the full 39-query result to 7,967 / 7,897 ms via relay and 5,197 / 5,330 ms
direct. Client tick work rose from about 1.0 s to 2.5–2.6 s because this route
builds a complete Client subscription graph; the roughly 0.25 s saved in
read-future polling could not pay for it. The 100-row page showed no gain.
The product change was reverted. The matched runs, binary hashes, exact
experimental condition, and phase totals are in the
[client serialized-read A/B receipt](saas-read-20260925/client-serialized-ab.json).
These are local OS-cache-warm samples with two full runs per arm and one page
run per arm, not a hosted latency claim.

This narrows the product change: a fast remote SELECT needs an authority-owned
result response that does not first construct another receiver maintained
graph. A live subscription can build that graph and hand over later, with the
authorization scope and authority cut bound to the first result.

## Why the current path has a high floor

The `limit(100)` path still records 484,189 keyed-join left-record visits,
361,452 keyed-join outputs, and 533,435 map/projection inputs on Core. Map
output buffers used 230 MB **cumulatively across nodes**, not at once. Core
query setup alone takes 597 ms of its 619 ms tick; Client tick is 45 ms. At
`limit(1000)`, Core query setup remains 602 ms. The limit cuts output shipping
and Client work but does not bound Core's permissioned maintained graph. See
[100-row phases](saas-read-20260925/child-direct-limit-100-phases.json) and
[1000-row phases](saas-read-20260925/child-direct-limit-1000-phases.json).

For the full child result, Core query setup is 807 ms and Client storage apply
is 625 ms, with another 201 ms of Client ingest and 148 ms of persistence in
the instrumented run. Core processed 723,458 map/projection visits; Client
processed 286,332. The CPU sample attributes Core query setup to initial
Groove subscription hydration, join arrangements, and terminal evaluation;
Client spends a separate large block ingesting and re-evaluating the delivered
closure. [Full-child phase receipt](saas-read-20260925/child-direct-phases-r1.json).

An allocation-site run makes the scale of that work concrete. The direct
`limit(100)` query requested **3.60 million allocations / 1.62 GB** before
settlement; Core `query_setup` alone accounted for 2.41 million allocations /
1.48 GB. The profiled Core setup took 657 ms of a 734 ms settle. Across all
39 direct subscriptions, the same scope requested **38.43 million allocations /
9.77 GB**. The largest allocation phases were Core query setup (2.20 GB),
Client storage apply (1.38 GB), Core supporting-row publication (538 MB), and
Client ingest (401 MB). In that profiled full run, the largest exclusive phase
times were Core query setup (951 ms), Client storage apply (729 ms), Client
ingest (257 ms), and Core supporting-row publication (248 ms). Sampled callers
include join arrangement, record-key
encoding, and record projection into large intermediate buffers. These are
*requested bytes over the run*, not live heap size. Allocation totals and
phase counters are exact; caller ranks are sampled estimates, and profiled
wall times are not comparable to clean receipts. [Compact attribution
receipt](saas-read-20260925/allocation-attribution.json).
Build with `--features cold-settle-attribution,bench-alloc-sites` for this
profile. The compact receipt uses diagnostics for phase attribution. Clean
timing mode now stops the allocator counter at readiness too; a separate
`limit(100)` check reported 3.62 million allocations / 1.62 GB and 100
authorized rows with `JAZZ_CUSTOMER_NO_DIAGNOSTICS=1`.

This points to two distinct interventions: avoid constructing the initial
maintained graph for first results, then reduce arrangement and record-copy
work for subscriptions that still need IVM. Shrinking one allocation site
cannot remove the receiver's full supporting-set ingest and second evaluation.

Changing receiver storage from RocksDB to memory moved the full child read
from 3,279 ms to 3,051 ms (memory receivers) or 2,999 ms (all memory), only
about 1.07–1.09× in this run. [Rocks](saas-read-20260925/child-direct-r2.json),
[memory receivers](saas-read-20260925/child-direct-memory-receivers.json),
[all memory](saas-read-20260925/child-direct-all-memory.json).

For all 39 subscriptions, bypassing the device relay through the existing
Core→Client protocol moved settle from 7,314 ms to 4,547 ms (1.61×) on the
same binary, with all 27,518 expected rows delivered. The dominant child
alone was 5,532 ms through the relay and 3,279 ms direct. This is a topology
comparison, not a shipped default or a substitute for offline/reconnect
requirements. [Full relay](saas-read-20260925/full-relay-r2.json),
[full direct](saas-read-20260925/full-direct-r2.json),
[child relay](saas-read-20260925/child-relay-r2.json).

The one-shot `limit(100)` still takes about 318 ms on the trusted host, so
indexing the authorization path may offer a second, separate improvement.
The first large win is avoiding full maintained hydration before a remote
read can return. Removing history/deletion bookkeeping cannot by itself
explain the no-deletion `limit(100)` floor.

An identity comparison narrows this further. On `limit(100)`, the member's
Core query setup was 597 ms and processed 484k join-left visits. An admin's
query setup was 237 ms with 172k join-left visits, and its trusted-host
one-shot read plus app-cell roundtrip was 124 ms versus the member's 318 ms.
Both returned 100 authorized rows with matching one-shot/maintained row sets.
The denied spy returned zero rows in 38 ms without opening the query graph,
so it is not a comparable 100-row timing. Permission graph work is a large
part of the fixed floor, while the admin's remaining 237 ms shows a separate
maintained-plan cost. [Admin maintained phases](saas-read-20260925/child-direct-limit-100-admin-phases.json),
[admin one-shot](saas-read-20260925/server-oneshot-wire-100-admin.json),
[denied-reader control](saas-read-20260925/child-direct-limit-100-spy-phases.json).

The one-shot access-path selector explicitly declines a source limit when
`table.has_any_policy()` is true. That guard prevents an early limit from
discarding unauthorized candidates and underfilling the result, but it also
means the member's `limit(100)` must consider the much larger source. A
policy-aware ordered candidate cursor with refill is a separate planner
experiment; merely lifting this guard would be incorrect.

## Relation to recent PRs

- Current `main` includes [#3306](https://github.com/garden-co/jazz/pull/3306)
  (touched-binding routing), [#3309](https://github.com/garden-co/jazz/pull/3309)
  (TopBy root keys), [#3330](https://github.com/garden-co/jazz/pull/3330)
  (root-collector routing), and [#3332](https://github.com/garden-co/jazz/pull/3332)
  (route activation and collector CPU). The earlier #3313 was closed and
  superseded by #3332.
- The earlier storage/read stack PRs #3314, #3319, #3322, and #3323 were
  closed. Their improvements were replaced on `main` by
  [#3335](https://github.com/garden-co/jazz/pull/3335) (RocksDB open validation),
  [#3336](https://github.com/garden-co/jazz/pull/3336) (point joins),
  [#3337](https://github.com/garden-co/jazz/pull/3337) (initial root/join filters),
  and [#3338](https://github.com/garden-co/jazz/pull/3338) (index-key
  intersection). Their query-specific receipts are valuable,
  but the refreshed 39-query control still takes seconds.
- Open draft [#3352](https://github.com/garden-co/jazz/pull/3352) declares
  versioned ordered composite current indexes and states that it is the new
  base for [#3315](https://github.com/garden-co/jazz/pull/3315)–[#3317](https://github.com/garden-co/jazz/pull/3317),
  [#3325](https://github.com/garden-co/jazz/pull/3325), and
  [#3334](https://github.com/garden-co/jazz/pull/3334). As checked on
  2026-09-25, those drafts still name older closed stack branches as their
  bases; the restack has not happened yet.
- [#3281](https://github.com/garden-co/jazz/pull/3281) is a draft that
  removes the version DAG. Its stage-1 CodSpeed receipt reports only a 2.5%
  improvement for its permissioned cold sync and read/query suites within
  ±2%. That is a different benchmark from this one, but supports treating
  history removal as a separate feature decision rather than the main first
  read fix.
- [#3260](https://github.com/garden-co/jazz/pull/3260) exposes progressive
  *local* previews while a stronger tier is pending. It does not supply the
  complete authoritative result proposed here.
- [#3348](https://github.com/garden-co/jazz/pull/3348) shares prepared shapes
  for Local-tier subscriptions and reports faster writes but slower hydration
  at 1,000 bindings. It deliberately leaves Global-tier receivers unchanged.
  The initial-read path needs its own benchmark gate.

The ordered/composite index work complements the separate policy-aware
limited-page experiment.

## Implementation checkpoints

1. Add a result receipt carrying the authority revision/cut and the exact
   admitted identity/claims scope. Compare it with receiver-local output in
   integration tests; do not publish an unscoped result.
2. Make a remote one-shot return after a complete result receipt. Keep local
   pending writes on the existing path until composition is supported. Measure
   first result and total background settle separately.
3. Let a remote subscription publish its initial reset from that receipt,
   then catch up its Client graph and hand over without duplicate, missing,
   or reordered rows. Run the differential oracle, permission, deletion,
   reconnect, and local-write cases before enabling it by default.
4. Re-benchmark 100/1000/full child and all 39 subscriptions through both
   topologies. Require an end-to-end improvement for a PR; the diagnostic
   ratios above are insufficient to claim one.

Tooling friction: a benchmark-only source edit takes about 43 s to rebuild
the optimized binary, and copying the seeded RocksDB store costs about 13 s
per one-shot process outside the measured read; a reusable in-process fresh
reader fixture would shorten the iteration loop.
