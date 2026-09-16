# Encoded query-witness cells and publication preparation

First measured implementation: `6b4aaecd15634f733f49dea75e4d44be9064a878`, clean.
Parent: #2801. This is a limited representation pass, not a protocol change.

## Mechanism

Witness user fields already have Groove encodings. Previously, each field became
an owned Value in a column-name map, was cloned into a values vector, then encoded
again. Matching nullable field types now copy their encoded spans directly into
the output history record. Differing types use the existing decoded semantics.
Metadata still uses the existing normalization path; further metadata/plan work
is possible. Unit-test builds compare every reconstructed witness against the
old value-based encoder, byte for byte.

Publication previously cloned the authored table and constructed its complete
history descriptor before attempting the exact stored-version lookup. Most rows
return from that lookup. Descriptor construction now happens only in the
synthetic-row fallback, which still verifies authored layout and completeness.
The lookup itself remains essential: a projected witness can omit cells even
when its descriptor matches storage.

## Initial optimized measurement

Same 39-subscription native member fixture, 27,518 output rows, preseeded Core,
empty relay and client, RocksDB WalNoSync, in-process semantic transport. Seed and
final diagnostic queries are excluded from settling. Both runs include the same
three additional phase spans. Baseline was parent code plus only those spans
(dirty instrumentation tree); the after run was the clean implementation above.
No concurrent build or test ran during either measurement.

| Measure                                         |  Before |   After |
| ----------------------------------------------- | ------: | ------: |
| Settling                                        | 30.649s | 28.700s |
| Dominant subscription ready from scenario start | 31.145s | 29.269s |
| Witness reconstruction                          |  1.923s |  1.191s |
| Canonical supporting-version preparation/lookup |  1.600s |  0.615s |
| Root-layout conversion                          |  0.679s |  0.694s |
| Other output decoding/bookkeeping               |  2.563s |  2.506s |
| Other supporting-update construction            |  2.269s |  2.287s |

Phase rows use exclusive times summed across Core, relay and client. They are
components of settling, not additional time. The targeted phases improve by
about 1.72s together; total settling improves by 1.95s (6.4%). These are initial
single-run comparisons, not confidence intervals. Earlier parent measurements
were closer to 29.55s, so do not interpret the whole difference against an older
run as a precise isolated effect. All expected rows and timing partitions pass.

## Remaining work

Output bookkeeping still costs about 2.5s, separate from witness decoding.
The code also clones a root record just to read its UUID, and layout rebinding
can reconstruct unchanged nested fields. These are follow-up candidates under
#2789; the current pass does not claim to remove them. The 5s cold-load target
remains far away.

Validation so far: Jazz library 2,005 passed, 2 ignored; default scoped Clippy and
formatting pass. All three incremental canaries and the two-seed maintained-vs-one-shot oracle
(churn depths 10 and 1,000) pass. Flipping one output byte makes the internal
byte oracle fail in `maintained_local_index_snapshot_is_complete`; restoring
the exact source makes that test pass. Full canonical CI, browser acceptance, and independent
review are not claimed.
