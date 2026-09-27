# Native witness identities

Adapt the native-reference checkpoint of [#2961](https://github.com/garden-co/jazz/pull/2961)
to the current performance stack. This does not include that draft's later
direct-coordinate projection: the existing visibility/witness join remains.
Follow-up and qualification live in [#3609](https://github.com/garden-co/jazz/issues/3609).

## Removed work

A native version witness previously reconstructed every selected source cell,
decoded a full `VersionRow`, and retained a full byte identity. Publication then
looked up the canonical authored version again, because a projected witness is
not the authoritative wire payload.

The resolver now marks eligible native sources with their exact physical table.
Their witness terminals retain the table, branch, row, transaction, schema and
deletion identity. Publication loads the canonical immutable body at delivery.
Public result materialization still reconstructs values requested by the app.
This applies to ordinary native rows as well as large values; inline, synthetic
and unproven sources retain their complete materialized witnesses.

## Contracts

- Both browser databases, incremental maintenance, history, deletes and public
  query results remain. No wire or storage format, codec or serializer changes.
- Authorization, source membership, binding routes and signed role lifetimes
  still come from the existing graph. Possessing a body does not grant coverage.
- A title-only projection still ships the complete canonical authored version,
  including fields excluded from the query projection.
- Missing native history and mismatched schema/deletion metadata fail closed.
  A missing native body cannot fall back to an invented or wider source.
- A renamed table uses physical identity for canonical lookup and exclusive
  bundle filtering. Reused logical names retain the existing materialized path
  and collision rejection. That behavior is deliberately not changed here.
- Register projections remain excluded when selecting content witnesses.
  Prepared-template equality includes the new native-source proof.

## Native resident-read measurement

The control already includes the ordering-hydration optimization from #3610.
Base: `c1e4a5607de748b2313631a68f243ba4249a8008`; candidate production-file hashes
are in the accompanying receipt. Tests added after freezing the candidate do
not contribute to the timed executable.

Two independent preloaded MemoryStorage databases; 32 schema tables; 36 settled
background subscriptions; eight identity claims; one file with a folder
reference. Every timed read obtains fresh coverage. No IndexedDB, browser,
transport codec, network or application startup is timed.

Four independent processes, control/candidate/candidate/control, 16 reads per
size/process. Exclude the first read from each process; report the median of
the two process medians (30 warm observations per arm/size). Instrumentation is
disabled during timing. Exact returned bytes are asserted by the harness.

| Value size     |    Control | Candidate | Speedup | Full reconstructions |
| -------------- | ---------: | --------: | ------: | -------------------: |
| 32 KiB, inline |   3.549 ms |  3.622 ms |   0.98× |                0 → 0 |
| 256 KiB        |   4.742 ms |  4.159 ms |   1.14× |                2 → 1 |
| 4 MiB          |  19.805 ms | 11.615 ms |   1.70× |                2 → 1 |
| 16 MiB         | 111.265 ms | 57.449 ms |   1.94× |                2 → 1 |

The inline result is inconclusive: its small positive latency difference lies
within the observed run spread. It is not evidence of a general small-row win.

At 4 MiB the serving peer falls from 10.930 to 2.783 ms, while foreground read
polling stays 8.711 versus 8.696 ms. At 16 MiB the serving peer falls from 56.731
to 2.947 ms, while foreground polling stays 54.338 versus 54.313 ms. This removes
the serving peer's payload-sized reconstruction; the public result still costs
what it returns. These results are not combined with earlier, differently based
receipts into an overall application speedup.

Separate initial diagnostic traces have identical messages, supporting/version
counts and canonical payload sizes, including setup and cleanup. A 4 MiB read
sends a 3,988-byte ViewUpdate and no chunk request. Canonical payload size is not
framed or compressed transport size.

## Exact review-base comparison

This comparison isolates the draft on its actual review base: production control
1d16705c174071f00ab253c816ffd14e4593438d (#3610), candidate
5f2970b6ac351d0a28cba1e721ce8a71aebe5720. The ordinary-row harness is identical
in both executables (harness checkpoint 2ac55d72e), including the repaired
membership counter. Source and executable hashes accompany every run.

### Ordinary text rows

The existing maintained_rehydrate_scaling fixture uses independent RocksDB
WalNoSync databases and current rows, with no deletions or binary blobs. Half
the rows initially match; one previously nonmatching row enters the result.
The table reports a fresh subscription returning 501 or 5,001 rows. Two text
columns are the original fixture; the wide case adds 18 ordinary text columns
of 128 characters plus a short row/column prefix each.

Four fresh processes per width run control/candidate/candidate/control with
all local builds and tests stopped. Each process runs the 1,000 and 10,000
source-row rungs once. Report the median of two process observations per arm;
these sample counts establish a local comparison, not a confidence interval.

| Source rows | Text columns | Subscription open, before → after | Speedup | Estimated retained state, before → after |
| ----------: | -----------: | --------------------------------: | ------: | ---------------------------------------: |
|       1,000 |            2 |                  15.41 → 13.72 ms |   1.12× |                           1.59 → 1.32 MB |
|      10,000 |            2 |                164.14 → 148.75 ms |   1.10× |                         15.92 → 13.21 MB |
|       1,000 |           20 |                  27.82 → 24.38 ms |   1.14× |                           7.23 → 2.68 MB |
|      10,000 |           20 |                317.57 → 289.86 ms |   1.10× |                         72.57 → 26.83 MB |

MB above is decimal and the retained-state counter is an estimate, not measured
RSS or an allocation-count profile. The narrow estimate falls about 17%; the
wide estimate falls about 63%. Fresh-subscription latency falls about
9–12% in this fixture (1.10–1.14× faster). Both independent process observations improve at each
rung. Single-row maintained delivery is already below 0.25 ms in both arms;
that small delta is not the main justification for this change.

All result digests, supporting-row additions/removals, bundle counts and encoded
message sizes match across arms. Storage reads/ranges also match: the single-row
update reads three entries/ranges, and fresh subscription reads 3N+3 entries
and N+5 ranges. This removes duplicate witness work and retained payload
state; the storage-read count stays the same.

Reproduction: build maintained_rehydrate_scaling at each source with cargo bench,
package jazz, no default features, features testing,transport-compression-zstd,
profile perf and --no-run. Freeze the executables, then run them serially with
JAZZ_PERF5_ROWS=1000,10000, JAZZ_PERF5_EXTRA_TEXT_COLUMNS=0 or 18,
and JAZZ_PERF5_EXTRA_TEXT_BYTES=128.

### Resident large values on the same review base

The same four-process resident-read recipe from the integration comparison
above produces:

| Resident value |     Before |     After | Speedup |
| -------------- | ---------: | --------: | ------: |
| 32 KiB, inline |   3.894 ms |  3.727 ms |   1.04× |
| 256 KiB        |   5.014 ms |  4.169 ms |   1.20× |
| 4 MiB          |  21.540 ms | 12.090 ms |   1.78× |
| 16 MiB         | 113.808 ms | 58.422 ms |   1.95× |

The 32 KiB difference remains within the control process spread; it is not a
demonstrated inline-row win. Large-value full reconstructions remain two → one.
These review-base results confirm the earlier integration result without
pooling differently based observations or multiplying stacked PR speedups.

The accompanying witness-review-abba-receipt.json records per-process
observations, phase breakdowns, hashes, environment and aggregation. Published
row records omit the harness's runtime hostname/git fields: those identify its
execution checkout, not the frozen executable's compiled source. Original raw
output hashes remain in the receipt; source/binary provenance is explicit.

## Correctness and qualification

The public-facade Core and scope-relay tests check repeated large reads, exact
bytes and foreign keys, serving/public reconstruction counts and authorization.
The approved existing-fixture changes preserve their data, ordering, sharing and
reused-name collision assertions. The native-prefix test covers ordering and
signed retraction that cannot be observed solely through public row equality.

The qualification checkpoint `9e63d6f55b4ed7d99b21e9fb3106c0467bc76156` passes all
hosted CI jobs in [run 36312702810](https://github.com/garden-co/jazz/actions/runs/36312702810):
Rust workspace, bounded differential, storage compatibility, TypeScript, React
Native and lint. The Rust job explicitly reports PASS for all four new reference
guards: manual/budgeted eviction fails closed, old bytes survive update/delete/
restore, schema/missing-coordinate checks, and deletion-event mismatch checks.
The accompanying CI receipt preserves those individual log lines.

The three repair reproducers linked to #2960 remain explicitly ignored and are
**not** included in those passing guards. Fresh-response repair was deliberately
run outside the passing suite and fails on both the candidate manual probe and
the exact production base's independent manual/budgeted probes, as detailed below.
No existing correctness assertion or transaction-conflict guard was weakened.

Both unchanged native-relay restart regressions pass on integration `159c12c55`.
The earlier exact review head also passed the public peer tests, native seek,
rename/collision checks, all three mechanism canaries and both targeted native
benchmark compile gates. These follow-up commits add only tests, benchmarks and
receipts; the optimized production implementation is unchanged.

The private sensitive-data guard is absent locally; no local pass is claimed.
The draft remains open for review. Browser fixture timing below is now measured;
Core network latency, application startup and a full browser memory profile are
not established by these results.

## Measured tradeoffs and failure boundaries

### Actual process memory

The same frozen exact-review-base native executables were run serially as
control/candidate/candidate/control, separately for 10,000 narrow and wide rows.
macOS `/usr/bin/time -l` measures the whole process high-water resident set. This
includes seeding, two RocksDB databases, storage caches and both subscriptions;
it is not an isolated witness heap measurement. Report the median of two fresh
processes per arm. Result digests, membership additions, wire bytes and read
counts match within each width.

| Text columns | Control peak RSS | Candidate peak RSS |               Reduction |
| -----------: | ---------------: | -----------------: | ----------------------: |
|            2 |        365.47 MB |          363.50 MB | Effectively flat (0.5%) |
|           20 |      1,287.31 MB |        1,180.39 MB |                    8.3% |

The 63% retained-state estimate is specific to the maintained view. It must not
be reported as a 63% process-memory saving. Wide-table process observations were
1,281.51/1,293.11 MB before and 1,178.86/1,181.91 MB after. The RSS runs preserve
phase measurements as provenance, but do not replace the earlier timing recipe.

### Release Chromium browser measurement

The opt-in `witness-tradeoffs.abstract-bench.test.ts` uses public account/schema/
transaction/query/subscription APIs. Each fixture inserts 150 or 1,500 ordinary
text rows plus 15 shared relation targets in one transaction. Each target carries
2,048 text characters; includes return the same parent for many roots. Each read
validates row count, unique result IDs, body text, foreign keys and included
parent values.

The persistent driver retains the foreground plus durable IndexedDB worker. The
memory driver uses one runtime, so subtracting their times does not isolate an
IndexedDB tax. Writes are locally durable and still pending; no Core is running.
Reopen resets database/runtime page caches while browser, WASM and OS caches
remain warm. These are database-reopen measurements, not a cold OS or app-login
benchmark.

Control production is `1d16705c1`; its browser/test harness checkpoint is
`697377c11`. Candidate production is identical across release builds `6f5e0b3ab`
and `9e63d6f55`. The production-tree receipt checks that the qualification files
changed neither implementation. Same checkout, release profile and wasm-opt
pipeline; source-bound WASM manifests and the identical harness hash accompany
every run. Four fresh Chromium processes run candidate/control/control/candidate,
with local builds/tests stopped. Other desktop applications remained running.

Warm phases have five reads per process. Each reopen/first-delivery phase has one
observation per process. Aggregate the median of the two process medians per arm;
these sample counts establish a local comparison, not confidence intervals.

| 1,500-row phase                            |   Control | Candidate | Ratio |
| :----------------------------------------- | --------: | --------: | ----: |
| Memory: warm flat                          |  32.88 ms |  33.66 ms | 0.98× |
| Memory: warm include                       | 392.65 ms | 380.91 ms | 1.03× |
| IndexedDB: warm flat                       | 158.34 ms | 144.45 ms | 1.10× |
| IndexedDB: warm include                    | 548.95 ms | 533.16 ms | 1.03× |
| IndexedDB: first flat after reopen         | 350.68 ms | 303.63 ms | 1.15× |
| IndexedDB: first include after reopen      | 711.40 ms | 656.21 ms | 1.08× |
| IndexedDB: first subscription after reopen | 391.95 ms | 367.92 ms | 1.07× |

This fixture shows modest browser gains, not a general 2× speedup. The 2.3%
slower memory flat-read median overlaps the observed process spread and does not
establish a regression. Native reference resolution does replace retained-row
cloning with exact history lookups in local materialization; the measured net
browser times do not establish backend read counts for that path.

The 150-row persistent cases are mixed/noisy. The balanced warm-include aggregate
is **56.73 → 69.95 ms**, with candidate process medians **85.72/54.19 ms** versus
control **56.73/56.73 ms** (unrounded values are in the receipt). A later full
candidate diagnostic gives **49.98 ms** for that phase and does not reproduce
the slow process. It is preserved separately and excluded from the original
balanced aggregate. Do not claim a win or a demonstrated regression for this
small case; keep the inconclusive signal visible in #3609.

All 16 primary browser cases and four additional diagnostic cases pass their
result assertions. The remaining ~0.53 s warm include path is separately worth
profiling: the single-runtime memory case already costs ~0.38 s. Repeated
transaction-witness list copies and per-edge projection work are code-level
suspects, not phase-attributed causes established by this measurement.

### WASM size cost

Same release build/wasm-opt pipeline, WASM binary only (not the whole JavaScript
bundle): raw **33,176,704 → 33,311,165 bytes (+134,461; 0.41%)**; deterministic
gzip level 9 **9,408,894 → 9,472,332 bytes (+63,438; 0.67%)**. The reference path
therefore has a small measured download-size cost alongside its memory/runtime
benefits. Build manifests and binary hashes accompany the size receipt.

### Explicit body eviction

A native witness is an immutable coordinate, not a body lease. Holding a
reference does not pin evictable history. Explicit manual and budgeted eviction
invalidate coverage before removing bodies; resolving the old reference then
fails closed. The new reference tests cover this boundary, exact old bytes
across update/delete/restore, a missing row coordinate, and schema/deletion-event
mismatches. Logical and storage descriptors can have different field names, so
the tests compare full encoded bytes plus table/branch identity rather than
requiring descriptor-name equality.

A repair gap remains in [#2960](https://github.com/garden-co/jazz/issues/2960):
a fresh nonempty authority reset after accepted-body eviction is rejected as
`ConflictingCommitUnit`. The candidate manual probe and unchanged production
base both fail. The base
`697377c11` preserves production `1d16705c1`; both its manual and budgeted probes
return `ConflictingCommitUnit` after verifying real body removal. A portable
reproducer tests those paths separately; a native-reference repair reproducer
retains the same expected recovery. They remain issue-linked ignored tests. The
base portable probes were explicitly run as diagnostics; no repair pass is claimed. The receiver keeps a
complete transaction header while eviction removes its stored version keys;
`preflight_view_bundle_conflicts` requires those keys to match a complete resend
or contain every key of a view-scoped resend. Missing bodies consequently look
like conflicting immutable membership. A repair must preserve conflict
validation and fresh authority coverage; merely dropping those checks would
weaken the contract.

### Local stack diagnostic

The exact catalogue test
`offline_replica_opens_requested_schema_only_after_published_lineage` aborts with
stack overflow on integration candidate `159c12c55` and unchanged control
`c1e4a5607`, with the identical test-source SHA-256. The control passes with
`RUST_MIN_STACK=33554432`. Thus this native-reference change did not introduce
that local default-stack failure. This is attribution, not a fix or a canonical
default-stack gate pass; the remaining defect is tracked in #3609.

Tooling-friction: retaining verified release-WASM generations within this checkout
would have avoided a repeat build; the opt-in browser fixture reruns in ~26 s.
