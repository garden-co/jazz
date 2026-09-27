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

New public-facade tests independently drive a serving Core and a scope-isolated
relay. They check repeated reads of 128 KiB and 2 MiB values, exact bytes and
foreign keys, zero serving-side full reconstructions, and one public-result
reconstruction. The Core scenario also denies a row already resident at the
client. The Core uses its required Global registration; the local relay uses
the foreground's Local registration.

Existing fixture changes only adapt the internal witness type; the maintainer
approved them. Data, ordering, sharing and reused-name rejection expectations
are preserved. A new internal prefix-seek test covers the reference identity's
ordering and signed retraction boundary, which row equality cannot observe.

Both unchanged native-relay restart regressions now pass on integration
commit 159c12c55: offline-relay restart and worker-plus-relay restart. This covers
the current counterpart of the failure reported on the older #2961 prototype.

On exact review head 5f2970b6a, both new public peer tests, the native seek test,
both existing rename/collision tests, all three incremental-delivery canaries
and both targeted benchmark compile gates pass locally. CI run
[36309137828](https://github.com/garden-co/jazz/actions/runs/36309137828) passes
the Rust workspace, bounded differential, TypeScript, React Native and storage
compatibility partitions. Lint stopped on formatting of approved fixture edits;
this receipt follow-up fixes formatting without changing assertions.

The integration library diagnostic passes 2,256 tests (four ignored) with
32 MiB test-thread stacks. Its default-stack catalogue-test abort is still
unattributed; the larger-stack diagnostic is not a canonical default-gate pass.
The bounded local differential oracle also passes. Full landing qualification,
the missing local private sensitive-data guard and browser/application timing
remain tracked in #3609. Native measurements do not establish browser or
Core-service end-to-end latency gains.

Tooling-friction: frozen native executables made matched reruns cheap; a repaired
membership counter and text-width controls avoided a new benchmark, while a
feature-specific debug RocksDB rebuild delayed the targeted compile gate.
