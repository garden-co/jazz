# Separate absolute latency from phase attribution

At runtime commit a8d0bb5a30, the optimized native permissioned fixture
materializes all 27,518 expected rows in both configurations:

| Configuration             |  Settle | Dominant-query readiness |
| ------------------------- | ------: | -----------------------: |
| `cold-settle-attribution` | 20.438s |                  20.922s |
| No attribution features   | 18.736s |                  19.203s |

The attribution build adds about 1.7s in this comparison. The host uses HPET;
641,651 span entries require approximately 1.28M clock reads. A Python loop
including one million monotonic clock reads takes about 1.13s. That loop also
includes interpreter overhead, and the feature enables other counters, so it
is not a precise isolation of clock cost. Do not subtract its estimate from
individual phase times.

Use the feature-free build for absolute latency. Use like-for-like attribution
builds to understand phases and changes within them. Previous reports calling
these runs clean meant no allocation sampling or CPU recording; they still
included the explicitly enabled phase instrumentation. The JSON now states
`phase_attribution_enabled` directly and the instrumented executable prints a
notice. An empty phase snapshot alone was too easy to overlook.

## Scoped CPU profiles

Both captures use external `perf` at 99Hz with 65,528-byte DWARF stack snapshots,
acknowledged enable/disable around connection through readiness, excluding
seeding and final diagnostic queries. Both materialize all expected rows from
the same clean runtime revision.

- With attribution: 2,121 samples. Allocation/free routines remain about a
  quarter of sampled CPU; memory copying is 5.44%.
- Without attribution: 1,970 samples. Named allocation/free routines are about
  23%, and memory copying is 6.62%. Kernel clock/copy activity largely recedes;
  page clearing remains visible. Percentages are sample shares, not additive
  independent elapsed-time measurements or a causal estimate of savings.

Kernel symbols were read locally without changing clock or kernel settings.
Some kernel PCs could be resolved against that live symbol table even though
`perf report` lacked a usable recorded kernel map. Inclusive async stacks still
have unresolved outer frames. Raw-sample caller aggregation leaves those
samples explicitly unassigned rather than inventing callers.

## Profiling friction

Use `perf script --no-inline --ns` when extracting stacks. Inline expansion
spent minutes on a small fraction of this capture; disabling inline expansion
processed the same data in a fraction of a second. The partial inline export
was stopped and retained separately. This sacrifices inline call-site detail,
not the sampled leaf symbols. Strip LLVM suffixes before passing residual Rust
mangled names through `c++filt -s rust` when grouping callers.

Do not interpret a filtered inclusive call graph as an exclusive caller
breakdown. The next analysis groups raw samples by sampled leaf and nearest
recovered repository caller, weighted by recorded event period.

## Operator counter correction

The old `tick_other` / `tick_dominant_child` split was not reliable after
lowering: the classifier looked for a logical table name or logical field
names, whereas the evaluated graph uses physical identities. All dominant
buckets were zero in the recent captures. Do not interpret those zeroes as
absence of work for the dominant query. Counts summed across both buckets
remain useful.

The classifier also recursively traversed the graph, allocating a vector at
each visited node, every time an operator recorded its counters. This was
additional observer work, separate from clock reads. Remove that classification
and report only the directly known `tick` / `hydrate` modes, separated by the
benchmark's runtime roles. Dominant-query readiness remains measured from its
actual subscription; this correction affects operator attribution only.

The retained runtime at `1111af7539` measured 18.866s settle and 19.332s dominant
readiness without instrumentation. The 1,350-of-1,500 todo update roundtrip was
274.082ms; the worker storage-write phase was 5.493ms within 91.041ms ingest.
These remain native measurements, not browser reopen timings.

Two subsequent experiments were discarded: broader total-projection composition
(#2828, 18.912s settle) and direct witness metadata copying (#2830, 18.729s).
Neither showed a meaningful elapsed improvement over the retained baseline,
so the extra compiler/encoding logic is not in the retained branch. The latter
passed all 2,006 Jazz tests after correcting the test shell's file-descriptor
limit; its initial 14 failures all explicitly reported too many open files.
