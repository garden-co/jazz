# Borrowed author and version values

This pass follows the projection/metadata preparation measurements in
[the preceding report](projection-composition-metadata-preparation.md).

## What changed

- Author lookup borrows `(account, issuer, subject)` and uses the existing intern
  pool. Canonical JSON spelling and native author records are built once per
  identity. Record readers borrow nested authors instead of copying their
  records and reconstructing JSON. SYSTEM provenance cannot become authority.
- Incoming versions copy compatible encoded fields directly into one output
  record. Storage aliases and converted timestamps are generated alongside them.
  Differing types retain conversion; author admission and canonical parent
  ordering are still checked. This is not a new storage or wire format.
- Groove large-value reference accounting walks encoded containers and decodes
  only indirect references. Inline strings and unrelated fields are not rebuilt
  as owned `Value` trees. Reference multiplicity is preserved.

## Measurements

Same optimized native member fixture: preseeded Core, empty relay and client,
39 subscriptions, 27,518 expected output rows, RocksDB WalNoSync, semantic
in-process transport. The dominant child subscription has 23,831 visible rows
from 43,000 candidates. Seeding is outside the measured load. This is not a
browser reopen or the 1,500-todo bulk-write benchmark.

| Metric                            |    Previous tip |       This pass |
| --------------------------------- | --------------: | --------------: |
| All subscriptions ready           |       32,415 ms |       29,485 ms |
| Settle                            |       31,946 ms |       29,013 ms |
| Full load plus diagnostic queries |       33,956 ms |       30,924 ms |
| Allocation requests               |     383,877,574 |     318,503,788 |
| Cumulative allocated bytes        | 289,350,646,379 | 275,184,546,230 |

A second clean run after restoration and rebuild reached all-ready at 29,720 ms
(settle 29,249 ms; full wall 31,188 ms), with all 27,518 expected rows. The two
clean runs improve readiness by 8.3–9.0%. These are individual runs,
not statistically established confidence intervals. Allocation totals come from
separate instrumented runs and include final diagnostic queries. They decrease
17.0% in requests and 4.9% in bytes; cumulative bytes are not peak RAM.
The 5-second cold-load target remains unmet.

## New profile and interpretation

A fresh 99-Hz DWARF CPU profile still shows allocation/copy self costs:
`_int_malloc` 8.31%, `memmove` 6.96%, `_int_free` 5.10%, `malloc` 4.09%,
and `String::clone` 2.15%. These cover the complete benchmark process and
are not additive to inclusive caller costs. Kernel symbols are unavailable;
async/unresolved stacks limit inclusive attribution. The self-cost report had
zero lost samples. Disabling inline expansion makes an exploratory inclusive
report fast, but its incomplete stack attribution must not be treated as a
complete phase breakdown.

Allocation stack samples plus code inspection identify remaining ownership and
representation work:

1. `complete_parent_versions` in `node/ingest/validation.rs` constructs a map
   and clones incoming `VersionRecord`s, including their owned payloads, before
   returning a new vector. This is still active during bulk reset ingestion.
2. `Database::apply_batch` constructs primary-key descriptors per write;
   `canonical_history_version_for_maintained_witness` reconstructs a history
   table to obtain its record descriptor. These are preparation costs in row
   loops, separate from the descriptor paths fixed in the preceding PR.
3. Other storage writes still enter `resolve_owned_record_input` as
   `RecordInput::Values` and go through `encode_record`/`RecordDescriptor::create`.
   Current-row carriers also reconstruct parents, cells and metadata as values.
4. Staged writes and physical storage-operation translation copy owned
   operations and keys. `IndexBy` and terminal collection remain visible too.

The allocation sampler stops collecting candidate stacks at 50,000 samples
(one per 4,096 requests), so its ranks identify early hot callers rather than
whole-run percentage shares. Full allocation counters remain valid.

The next hypothesis is to preserve encoded ownership through remaining
transaction/current-row/storage staging boundaries, and prepare descriptors
outside row loops. The evidence does not justify assuming another scalar
encoding micro-optimization will supply the remaining roughly 6x improvement.
Avoiding repeated payload copies and reducing total row visits need separate
measurement; these are related but distinct costs.

## Validation

- Final Groove library suite: 743 passed, 2 ignored, including two added
  byte/reference tests. The focused record subset also passed (83 tests).
- Final Jazz library suite: 2,005 passed, 2 ignored. Test builds compare each
  incoming-version encoding with the previous Value-based encoder byte for byte.
- Three incremental delivery canaries passed: relation changes, complete
  supporting snapshots, and batched writes.
- Two-seed differential oracle with churn depths 10 and 1,000 passed.
- The first Jazz full run had one deletion-coverage failure. The exact same
  binary passed that case alone, and the subsequent full suite passed. This
  intermittent result is recorded rather than counted as an initial clean pass.
- Record mutation checks: corrupting offset assembly and stopping reference
  traversal after its first reference each fail their corresponding new test.
  Mutations were restored. Disabling author-record reuse also fails the new
  cache-reuse test; its mutation was restored.
- Clippy passes with a narrow `InternedAuthor` interior-mutability exception:
  derived caches are excluded from manual Eq/Hash and stable Debug; identity
  never mutates.

Independent review, full canonical CI and browser acceptance are not claimed.

## Reproduction

Build `cargo build -p jazz-sim --bench customer_cold_start --profile perf` and
run the executable from the repository root with `JAZZ_CUSTOMER_IDENTITY=member`,
`JAZZ_CUSTOMER_PHASES=cold`, `JAZZ_CUSTOMER_SCALE=1.0`, and
`JAZZ_CUSTOMER_MAX_TICKS=200000`. Build separately with `--features bench-alloc-sites`
for allocation counts. Keep clean timing runs separate from builds and tests.
Local raw evidence is under `jazz-debug-evidence/permissioned-profile/` with the
`borrowed-values` prefix (timing, allocation trace and CPU profile).
