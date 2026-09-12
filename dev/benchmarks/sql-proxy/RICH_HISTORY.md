# Richer row-local history reference

`JAZZ_CUSTOMER_SLIM_MEMORY=/path/to/capture JAZZ_SLIM_RICH_HISTORY=1`
runs a separate reference that keeps exact encoded versions, immutable
transaction bytes, transaction fates, per-row history indices and a maintained
winner. It recomputes only the affected row's history after each arrival.

The shallow control uses the original captured deliveries. The expanded control
constructs five versions per physical row: a base, two competing siblings, a merge
of both siblings, and a pending child. Half of the pending children subsequently
become accepted; half become rejected. Every sibling is also delivered twice.
Delivery order is reversed before the final fate updates, so descendants precede
ancestors. Payloads remain identical to the original fixture, preserving the
full application-field and permission checks. A separately generated map checks
which exact transaction wins, not only whether the final payload looks right.

Accepted heads exclude known transitive ancestors; remaining heads are ordered
by the same transaction ordering convention, using actual transaction IDs.
Unknown ancestry remains in the retained parent references and is reconsidered
when another version of that row arrives. Terminal fates cannot change, and
repeated immutable coordinates must contain identical transaction and row bytes.

This prices an explicit subset of read-side history maintenance, not Jazz's
complete protocol. It only admits single-row mergeable transactions in the
default branch, has no deletion registers or schema changes, and trusts supplied
fates. It does not validate write permissions, issue fate decisions, implement
cross-row atomic publication, park network messages or recover from crashes.
The richer fixture has more input bytes and versions; compare both total time
and delivered-version throughput rather than treating it as the original load.

## First optimized native result

Three rounds, with exact winner and output checks after timing. Milliseconds:

| Dataset | Delivered records | History shape                              | Ingest | Permissioned queries |
| ------- | ----------------: | ------------------------------------------ | -----: | -------------------: |
| Larger  |            47,437 | Original shallow                           |    145 |                   30 |
| Larger  |           327,180 | Five versions, duplicates and fate updates |    977 |                   46 |
| Smaller |            28,137 | Original shallow                           |     86 |                   29 |
| Smaller |           192,626 | Five versions, duplicates and fate updates |    574 |                   39 |

The richer case does about 6.9× as many deliveries in about 6.7× the ingest time.
That does not prove arbitrary deep history is cheap: this reference deliberately
recomputes row-local ancestry and can become expensive on long chains. It shows
that explicitly retaining shallow branching history and resolving fates does not
by itself force a large per-delivery penalty in this representation.
