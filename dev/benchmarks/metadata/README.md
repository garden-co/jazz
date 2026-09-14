# Benchmark description and throughput metadata

The benchmark-owned `metadata.ts` files are the structured source for dashboard
descriptions and work-unit denominators. This directory provides their shared
types, exact-name registry and arithmetic helper. The adjacent Rust harness
remains the executable authority for what is timed; update both when that changes.

Each entry documents one timed iteration: purpose, fixture, storage/durability,
included/excluded phases, source path, and a positive work count with a precise
rate unit. Never infer work from a number in a benchmark name: a 100k-row table
may return 50 rows; 100 live subscriptions may process one matching write; task
detail executes two queries but counts as one operation. Batch rows are not
transactions. `throughput(seconds, work)` returns work/seconds, or null for invalid
inputs. It is reciprocal-median workload rate, not a separately measured mean
throughput or concurrent/sustained capacity test.

Registry revision 1 documents the harness at `metadataRevision.reviewedCommit`.
Historical timer boundaries may differ. The dashboard labels these as reviewed
definitions, links that commit as documentation authority, and separately links
the measured source. Counts are stable for the documented named workloads;
renamed or unknown cases get no guessed denominator. Semantic workload changes
must update the metadata/revision and should receive new benchmark names when
they would invalidate comparisons.

Run the metadata tests with `pnpm --filter docs test:perf-timeline`. They check
unique names, source existence, required documentation, every current Divan
function in these suites, parameterized variants and arithmetic. Each owning
benchmark should keep its README pointing here rather than maintaining a second
dashboard-specific description.
