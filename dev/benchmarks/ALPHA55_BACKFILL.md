# alpha.55 historical policy workload backfill

This permanent harness branch must not merge into main or the alpha.56 release.
It starts at released engine `11738be1b2b442ffb0ea37c3c3891d538ec02251`
(`v2.0.0-alpha.55`) and imports the policy-scoped-documents example verbatim
from `1a5add74c7da1509049ffedaf7f663b2cbd36c6e`. No compatibility shim is
required. Every released `crates/` entry is pinned by the provenance verifier;
Cargo dependency versions and profiles are preserved, adding only the new
workspace package and its lockfile entry.

Dispatch `.github/workflows/codspeed.yml` on this branch; there are no inputs.
The sole matrix entry is `policy-scoped-documents`, measured in walltime mode
on `codspeed-macro`, with Rust 1.93.1, cargo-codspeed 5.0.1 and mimalloc.
The workflow uploads `alpha55-provenance-policy-scoped-documents` before build.
The verifier rejects changed engine trees and tracked working-tree changes.

The ten benchmark functions each use 10,000 and 100,000 documents (20 cases).
Five cold page reads and five cold subscription-first-result cases preserve
schema, explicit SELECT policies, identities, row distribution, query ordering,
50-row limit, RocksDB durability, seeding, preparation and timing boundaries.
Runtime reopening/preparation is outside measurement; the read or subscription
through its first event is inside. This is runtime-cold, not OS-cache-cold.
The `policy_free` historical names use explicit SELECT true in both sides.
Do not compare these measurements with earlier absent-policy control fixtures.
Existing permissioned-resources fixtures also changed to explicit SELECT true;
their older unrestricted results are not interchangeable with the new fixture.

After CI, inspect successful job output and provenance, resolve exact CodSpeed
run/result IDs, and register only actually measured results in the current
main branch's `docs/lib/perf-timeline/backfills.ts`. Use release publication
`2026-09-15T22:33:17.491Z` from npm `jazz-tools` time[2.0.0-alpha.55].
This is a timeline effective-date override only: keep the actual CodSpeed
measurement timestamp, harness SHA, source SHA, released engine SHA and workflow
artifact link. Never fabricate a historical execution timestamp or include
carried-forward/skipped results. Registration produces a Released dot connected
to later main measurements using existing reviewed backfill machinery.

For headline improvements, divide alpha.55 median seconds by the final alpha.56
candidate median for the exact same case and harness. Report raw walltime
separately from the timeline's illustrative 5x estimated time/throughput.
No speedup claim exists until both receipts have completed.

Local verification: `cargo check --locked -p
jazz-example-policy-scoped-documents-benchmark --all-targets` compiles unchanged
harness, receipt binary, benchmark and correctness test against the released
engine. This is a compile receipt, not a runtime correctness or timing result.
