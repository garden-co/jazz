# Stored-version publication without value reconstruction

The exact phase allocation collector (#2820) ran the 39-subscription native
permissioned fixture with all 27,518 expected rows present. At clean commit
02171f062c it counted 143,366,980 Rust allocation requests and 35,787,730,107
cumulative requested bytes between connection and readiness. The exclusive
phase buckets sum exactly to those totals. This is allocator traffic, not RSS;
C++/RocksDB internal allocations are outside the counter. Instrumented elapsed
times are not clean performance receipts.

| Exclusive phase, summed across roles     | Allocations | Requested bytes |
| ---------------------------------------- | ----------: | --------------: |
| Ingest                                   |     20.684M |         2.767GB |
| Query setup                              |     17.277M |         2.732GB |
| Storage application                      |     16.494M |         1.962GB |
| Supporting-row publication               |     13.180M |         2.756GB |
| Result collection                        |     11.364M |         2.573GB |
| Receive updates                          |      9.966M |         1.851GB |
| Query evaluation, excluding child phases |      9.506M |         5.930GB |
| Decode query outputs                     |      6.083M |         5.225GB |
| Persistence                              |      4.121M |         0.398GB |

This is not a CPU ranking. For example, removing fixed-nullable temporary
buffers (#2821) left total settling flat at 21.850s versus 21.757s.

Sampled allocation stacks identified another representation roundtrip:
`version_record_from_row -> VersionRecord::from_stored -> VersionRecord::encode`.
Every outgoing stored version decoded its cells into owned Values, reconstructed
its provenance and parents, rebuilt its descriptor, and encoded everything.

The new path copies the existing encoded parents, authors and user fields into
one wire-record output buffer. It converts packed storage HLC timestamps to wire
milliseconds and emits deletion/null fields explicitly. The existing bounded
schema-shape cache prepares the wire descriptor alongside the history descriptor.
There is no new storage or wire format. Test builds retain the old value encoder
as an independent exact-byte oracle for every conversion.

Clean optimized run at 245b1c39ae: all 27,518 rows, 21.037s settle and 21.522s
for dominant-query readiness. Parent: 21.850s / 22.335s. Supporting-row
publication falls from 2.022s to 1.414s (30%); total settling improves 3.7%.
Most other phases stay close. These are individual runs, not a confidence
interval, and the local phase gain must not be advertised as an end-to-end gain.

All 2,005 Jazz library tests passed (2 ignored), including wire/history exact
roundtrips and provenance boundary cases. Clippy and formatting passed.
