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

This remains a draft experiment. The earlier #2961 merged-tree relay-write
failure must be qualified on the current implementation, alongside the full
canonical gates. Native timings do not establish browser or Core-service
end-to-end latency gains. Ordinary text-row measurements are reported separately.

Tooling-friction: preserving frozen native executables makes matched reruns cheap;
the existing subscription harness is being extended with optional text-width controls.
