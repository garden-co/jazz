# Projection composition and metadata preparation

This continues [prepared field projections](prepared-field-projections.md).
The two review slices are [#2798](https://github.com/garden-co/jazz/pull/2798)
and [#2799](https://github.com/garden-co/jazz/pull/2799).

## Scope and timings

The optimized native permissioned fixture has a seeded Core and empty relay and
client, 39 subscriptions and 27,518 expected result rows. Its dominant child
query returns 23,831 rows from 43,000 candidates. It uses RocksDB WalNoSync and
in-process semantic messages. Seeding is excluded. This is neither a populated
browser reopen nor a measurement of the separate 1,500-row write fixture.

| Code                                                   | All subscriptions ready |   Settle | Full measured wall |
| ------------------------------------------------------ | ----------------------: | -------: | -----------------: |
| #2797, `1f7eb2884f`                                    |                35.254 s | 34.757 s |           36.985 s |
| Projection composition/reuse, `f1f72e96f8`             |                35.320 s | 34.831 s |           37.098 s |
| Descriptor preparation, `33712f26aa`                   |                32.504 s | 32.036 s |           33.999 s |
| Plus borrowed schema/owned version reuse, `c1d58fc30c` |                32.415 s | 31.946 s |           33.956 s |

All runs returned the expected rows. These are individual observations, not a
statistical speedup claim. Clean timing runs had no concurrent builds or tests.
The projection slice has no measurable end-to-end win here; the combined pass
improves readiness by about 8%. The extra version ownership cleanup primarily
removes allocation work, without a distinguishable timing gain in these runs.
We remain well above the 5-second target.

Allocation totals come from separate instrumented runs. They cover the measured
load and final verification/diagnostic reads, excluding seeding; they do not
isolate just the readiness interval.

| Code                   | Allocation requests | Cumulative requested bytes |
| ---------------------- | ------------------: | -------------------------: |
| #2797                  |         483,481,285 |            295,523,063,958 |
| Descriptor preparation |         391,198,694 |            289,941,226,709 |
| Final metadata slice   |         383,877,574 |            289,350,646,379 |

The combined pass removes 20.6% of allocation requests but only 2.1% of requested
bytes. Requested bytes are allocation churn, **not peak resident memory**.
There is no separate allocation baseline for the projection-only commit.

## Changes and boundaries

Projection plans prove physical slot/type identity once. A projection that only
renames or reorders logical fields while keeping identical physical slots shares
its immutable input bytes. Rearranging variable payload slots still copies.
Adjacent pure field selections compose directly onto their underlying source;
shared inner projections remain available. Composition does not cross fallible
expressions, enum conversions, filters, joins or other semantic operators.

Table and index source readers resolve each stored variant descriptor once per
batch rather than once per row. The mechanism canary proves two preparations for
1,000 rows across two variants. Unknown variants still fail.

The fixed identity, durable row-author and nullable session-author descriptors
are prepared once. Nested identity decoding borrows the encoded field instead
of creating an intermediate owned record. Durable authors still cannot restore
the in-process SYSTEM capability; validation and exact provenance are preserved.

Version encoding uses prepared history descriptors and the fixed deletion
register descriptor directly, avoiding complete storage-table/index construction
per version. The history cache retains at most 128 shapes per thread and matches
table name, ordered field names/types and large-value semantic kinds. Table names
matter for native enum registry binding; JSON and text can have different
physical representations. Eviction affects only preparation cost.

Version decoding resolves the physical table ID without formatting a name for
every catalogue candidate, borrows its schema, and keeps the incoming owned
record buffer. The inverse name parser accepts exactly the constructor's
unsigned decimal spelling and distinguishes history from register names.
There are no storage or wire encoding changes.

## Verification

On final code: 741 Groove library tests and 2,004 Jazz library tests passed,
with two ignored in each. All three incremental-delivery canaries passed,
including scale-independent relation updates and writes and the linear snapshot
receiver bound. The maintained/one-shot differential oracle passed with two
seeds and churn depths 10 and 1,000.

Mutation checks deliberately disabled reuse/composition, restored per-row
variant preparation, disabled history caching, or accepted padded physical
names. The relevant tests failed: pointer/dependency assertions; 1,000 builds
instead of two; 500 builds instead of five; and acceptance of `01` instead of
only `1`. All mutations were restored before final verification.

These are draft PRs. Full canonical CI, browser acceptance and independent review
are still outstanding. No subagents were used for this single-track pass.

## Interpretation and next investigation

A fresh full-process CPU profile still shows substantial allocator and copy self
cost: `_int_malloc` about 8.6%, `memmove` 6.4%, and `_int_free` 5.6%, with other
allocation/free routines adding further cost. This was a 99-Hz DWARF userspace
profile; 46 samples were lost and kernel symbols were unavailable. It is
indicative, not a phase-specific exact accounting.

The new allocation trace and code walk point to remaining repeated work:

- `Database::apply_batch` reconstructs primary-key descriptors for pending writes
  and decodes complete delta rows into `Value`s to collect large-value roots.
- `resolve_owned_record_input` / `encode_record` still assemble records from
  owned values during ingestion.
- `canonical_history_version_for_maintained_witness` still constructs a history
  storage table to obtain its descriptor, outside the new VersionRow cache.
- Whole author/value reconstruction and other schema/version clones remain.

The stack sampler caps at 50,000 samples, so these are concrete candidates, not
full-run allocation-share rankings. Full allocation totals remain valid. The
next investigation should concentrate on eliminating whole-record/value
round-trips and moving remaining table metadata preparation to batch/plan scope,
rather than continuing with projection-only micro-optimizations. Follow-up is
tracked in [#2789](https://github.com/garden-co/jazz/issues/2789).

## Reproduction and iteration notes

Build the native fixture with `cargo build -p jazz-sim --bench customer_cold_start
--profile perf`, then run its emitted executable from the worktree root with
`JAZZ_CUSTOMER_IDENTITY=member JAZZ_CUSTOMER_PHASES=cold JAZZ_CUSTOMER_SCALE=1.0
JAZZ_CUSTOMER_MAX_TICKS=200000`. Build separately with `--features bench-alloc-sites`
for allocation totals. Do not compare instrumented wall time with clean timings.

For the bounded oracle, use `JAZZ_SEED_COUNT=2
JAZZ_DIFFERENTIAL_CHURN_DEPTHS=10,1000 dev/t --exact
node::tests::harness::m3_maintained_one_shot_differential_oracle -- --ignored`.
A seed limit alone does not bound its separate aggregate-churn prelude: an initial
run was stopped after encountering the default 100,000-depth workload.
