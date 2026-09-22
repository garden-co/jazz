# Slim encoded-memory reference and engine work budget

This is a workload-specific executable reference, not a proposed production
engine. It asks whether handling the actual encoded records is inherently slow.
It accompanies the unchanged-engine storage comparison in MEMORY.md.

## Reference contract

`JAZZ_CUSTOMER_SLIM_MEMORY=/path/to/inputs` runs the reference inside the
`customer_cold_start` native benchmark. Inputs are `fixture.json` from the SQL
fixture exporter plus `core-edge.jsonl` and `edge-client.jsonl` from the actual
sync capture. Capture JSON parsing and hexadecimal conversion are setup.

The Core is prepopulated before timing. During the measured interval:

1. Evaluate the 39 fixture queries and collect supporting rows at Core.
2. Decode and install the actual Core→Edge captured transactions and versions.
3. Evaluate the queries and collect supporting rows at Edge.
4. Decode and install the actual Edge→Client captured transactions and versions.
5. Evaluate the queries and collect supporting rows at Client.

Each receiver owns decoded VersionRecords (which retain native encoded record
bytes), raw transaction bytes, an ordered table/row identity index and table
vectors of row indices. Exact duplicate rows are checked. Queries read predicate
fields through VersionRecord APIs and materialize every application field of
all outputs. No synthetic smaller payload substitutes for the encoded records.

Permissions are independently implemented from the fixture relationships:
account→group membership, bounded eight-step non-administrator group traversal,
non-administrator resource grants, and inherited child access. Reachability is
recomputed per query. Supporting sets include outputs, parent rows, matching
grants and readable reachable permission-graph rows. This is the conservative
SQL-reference supporting-set definition, not a claim of identical Jazz witnesses.
After timing, every output ID and application field is checked against the
independently exported fixture at all three nodes.

## Deliberate differences from full Jazz

The captured deliveries are replayed, not generated from the reference's
supporting sets. This charges the real payload volume (including duplicate
deliveries), but omits subscription scope construction, query compilation,
transport serialization/compression and maintained incremental operator state.
The fixture has one accepted version per row; the reference stores that version
and transaction bytes but implements no general history, concurrency, branch,
fate-transition, revocation or crash-recovery machinery. Its timing is an
achievable workload-specific reference, not an equivalent end-to-end DB result.

## Work budget

`JAZZ_CUSTOMER_WORK_BUDGET=/path/to/counts.json` wraps the benchmark's storage
adapters. Counting begins at connect/subscribe and stops at readiness, before
one-shot verification and size inspection. Keys include role and column family.
It counts requested point/compare/reverse reads, scan calls and returned rows,
returned byte volumes, and submitted batch entries/bytes. These are storage
boundary volumes, not a claim to count every internal memcpy or backend copy.
The wrapper delegates the same adapter operations rather than replacing its
batch/cursor behavior. Counted timings are diagnostic only.

Combine this with feature `cold-settle-attribution` for existing projection,
join and phase attribution. Projection input counts mean record visits, not
unique rows; projection buffer bytes count newly produced buffers. Trace span
entries can include async polls and must not be interpreted as logical commits.

## Measured results

Three final optimized native reference rounds: 271.742, 270.055 and 270.739 ms.
Independent phase medians: Core queries 30.911 ms; Edge decode/install
113.580 ms; Edge queries 30.377 ms; Client decode/install 67.195 ms; Client
queries 29.344 ms. The initial checkpoint was 267.215 ms; the final run includes
the complete duplicate-version equality check and passed permission sensitivity. Do not compare instrumented diagnostic wall time with these
clean timings. The previous unchanged all-memory Jazz median was 11.213 s;
the reference deliberately omits the machinery listed above.

All three nodes render 135,254 application fields and collect 33,104 supporting
memberships across the 39 queries. Reference query-loop visits are 55,922 at
Core, 55,922 at Edge and 36,700 at Client; this counter excludes ingest/index
work, container-internal comparisons and output field reads.

Storage-boundary work through readiness:

| Role   | Unique fixture rows present | Point get requests | Scan requests | Scan rows returned | KV set entries submitted |
| ------ | --------------------------: | -----------------: | ------------: | -----------------: | -----------------------: |
| Core   |                      46,740 |             47,439 |           132 |            293,588 |                        0 |
| Edge   |                      46,740 |            402,063 |       187,271 |            373,401 |                  565,566 |
| Client |                      27,518 |            220,148 |       110,270 |             82,555 |                  334,739 |

Client receives roughly eight point reads, four scans and twelve KV writes per
unique row. Its set entries include 141,175 index entries, 111,010 metadata
entries, and 27,518 each in history, global-current and changes families.
Edge has a similar write amplification. Both receivers submit 83 backend
batches through readiness, but the large received-bundle ingest is one bulk
commit per receiver; batch count is not a count of per-row commits.

Existing operator instrumentation reports:

| Role   | Projection input visits | New projection bytes | Join left visits | Join right visits |
| ------ | ----------------------: | -------------------: | ---------------: | ----------------: |
| Core   |                 616,745 |          252,613,514 |          191,444 |            97,983 |
| Edge   |               1,024,313 |          397,796,811 |          598,757 |           139,545 |
| Client |                 451,302 |          148,634,823 |          141,470 |         1,132,578 |

The 799 MB of newly produced projection bytes is cumulative, not peak memory.
Projection capacity totals 1.390 GB; neither measure counts every allocation or
copy in the engine. Join counters are operator-specific record visits, not a
count of successful pair matches. Reference and engine visit counters describe
different operations; their ratio is not a normalized throughput measurement.

## Code-walk interpretation

The bulk receive path groups transactions, deduplicates versions, completes
parents, translates wire rows into storage records, stages transactions,
history, current-row records and merge heads, then applies one engine batch.
After that batch it also rebuilds merge heads from persisted history.
See `ingest_reset_view_bundle_refs_in_bulk` in Jazz's
`node/ingest/commit_bundles.rs` and the two bulk merge-head helpers in
`node/ingest/view_updates.rs`. Even shallow histories pay for several forms of
the same logical row; the post-commit rebuild rereads each row's history and
transaction fate. Removing it safely would require proving why both passes
exist across all supported cases, not assuming the fixture covers them.

Groove's `compute_table_deltas` constructs old/new record deltas, performs
storage lookups except on proven-fresh inserts, and copies payloads into grouped
weighted records. Query execution then processes many intermediate records and
terminal/publication representations. These measured volumes support a thesis
of amplification across representations and passes. They do not assign all
remaining time to encoding or prove that a single fast path removes it.

The next useful design question is how to install a large immutable batch and
initialize maintained query state from it without repeatedly expanding the same
records through the generic mutation/event machinery. No production change is
part of this experiment.

## Allocation budget (separate instrumented runs)

With only allocation counting enabled, no phase tracing and no storage-budget
wrapper, the all-memory engine requests **108,924,168 allocations/reallocations**
and **27,676,205,024 bytes** through readiness. The reference requests a median
**2,530,439 allocations/reallocations** and **362,597,278 bytes** for its measured
Core query + two receiver ingest/query sequence. Both use the benchmark's
mimalloc adapter underneath the counting wrapper.

These are cumulative requested bytes (a reallocation charges its full requested
new size), not resident memory, leaked memory, bytes necessarily copied, or an
exact explanation of elapsed time. The reference omits engine responsibilities
listed above. Nevertheless, cheap record access is compatible with a much
smaller allocation workload on the same encoded data. Even this deliberately
simple reference performs millions of allocations; it is not an optimized
zero-allocation target.

Verification also checks that an identity without membership receives no
protected resource/child rows. Reproduction receipts, exact executable paths
and logs are preserved locally under
`/home/ubuntu/jazz-debug-evidence/permissioned-profile/slim-memory/`.
Benchmark-only source is based on #2890 (`c4189ddba1`); measurements precede the
experiment commit, so emitted Git metadata identifies that parent and a dirty
tree. Instrumentation modes have separate executables and receipt files.
