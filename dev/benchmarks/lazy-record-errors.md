# Lazy record errors on successful reads

Runtime commit: `ce627e571e`, PR #2818, on #2817.

The scoped CPU profile showed record-error destructor self cost. Disassembly
confirmed that `RecordDescriptor::field_span` constructed unused errors and
called their destructor twice on its successful path. The active variants have
no heap payload, but share the enum's nontrivial drop glue. This is CPU work,
not evidence that successful reads were allocating those errors on the heap.

Use lazy error construction for the record/value access sites in this pass.
All checks and returned errors remain. The Clippy exception is scoped to the
record module and explains the measured reason for retaining the closures.
The optimized field_span symbol shrinks from 382 to 297 bytes and has no calls
afterward, including no destructor calls. No format or ownership API changes.

## Native permissioned cold load

Seeded Core, empty relay/client, 39 subscriptions, all 27,518 rows correct.
Clean trees and no concurrent builds/tests during timing. Individual runs:

| Measure                               |   #2817 |   #2818 |
| ------------------------------------- | ------: | ------: |
| Settle                                | 22.951s | 21.757s |
| Dominant query ready                  | 23.434s | 22.247s |
| Query evaluation, exclusive           |  2.915s |  2.409s |
| Storage application, exclusive        |  2.350s |  2.241s |
| Ingestion, exclusive                  |  2.640s |  2.466s |
| Query-output decoding, exclusive      |  2.375s |  2.326s |
| Supporting-row publication, exclusive |  2.143s |  2.063s |

About 5.2% less settling time, with the largest measured phase improvement in
query evaluation (~17%). Phase gains are part of the total, not additive to it.
The approximately 5s target remains unmet.

The separate 1,500-todo/1,350-update RocksDB-worker benchmark reports a 319.211ms
batch roundtrip, within the preceding checkpoint's 319–328ms observations.
There is no meaningful batch-roundtrip improvement claimed. Initial receiver
query is 13.528ms and fresh post-update ingestion is 52.493ms. These are native
measurements, not browser or IndexedDB acceptance.

Validation: 744 Groove library tests passed, 2 ignored; three scaling canaries
and the two-seed maintained-query oracle at depths 10 and 1,000 pass. Scoped
Clippy and formatting pass. No new behavior or byte format is introduced.
Independent review, full canonical CI and browser acceptance remain outstanding.

Tooling friction: allocation counts did not expose this success-path destructor
work. CPU self samples plus optimized assembly did. Clippy's default suggestion
would reintroduce it; the scoped rationale preserves the evidence in code.
