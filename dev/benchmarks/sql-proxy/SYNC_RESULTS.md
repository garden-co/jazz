# First synchronization comparison

Three fresh-store rounds per engine/durability setting. All timings are wall-time
medians; source data is cache-warm. Initial source seeding and empty-store schema
creation are outside the timer. Receiver planner-statistics refresh is inside.

| Work                                          | SQLite relaxed | SQLite synchronous | PostgreSQL relaxed | PostgreSQL synchronous |
| --------------------------------------------- | -------------: | -----------------: | -----------------: | ---------------------: |
| Replay actual Client bundles into empty store |        0.497 s |            0.823 s |            1.493 s |                1.527 s |
| Core → Client → local results                 |        0.914 s |            1.238 s |            1.892 s |                1.949 s |
| Core → Edge → Client → local results          |        1.969 s |            2.840 s |            4.808 s |                4.837 s |
| Replay identical bundles into populated store |        0.514 s |            0.520 s |            1.330 s |                1.333 s |

The last row includes staging/byte comparison and statistics work. It does not
simulate reconnect negotiation that avoids resending known bytes. Durable
version rows remain unchanged, but staging and statistics can still write.

## Two-hop work distribution, relaxed setting

| Stage                                    | SQLite | PostgreSQL |
| ---------------------------------------- | -----: | ---------: |
| Core query and complete envelope fetch   | 233 ms |     440 ms |
| Edge ingestion including statistics      | 790 ms |   2,444 ms |
| Edge permission query and envelope fetch | 274 ms |     328 ms |
| Client ingestion including statistics    | 452 ms |   1,464 ms |
| Client local permissioned reads          | 131 ms |      75 ms |

These component medians are descriptive, not an additive reconstruction of the
measured median total. Totals also include packet grouping/order work and small
counter-collection overhead. PostgreSQL client/backend CPU overlap and must not
be summed as wall time.

## What this demonstrates

The SQL read-only reference took about 31 ms. Making the load populate two
indexed stores raises that to seconds. **Synchronization is a materially larger
job than a request/response read**, even for shallow histories. The receivers
dominate these SQL topology runs.

This does not establish an intrinsic lower bound. The proxy writes full captured
wire envelopes, including repeated descriptors, and additionally stages them
before exact-match insertion. For example, Client traffic contains 64.65 MB of
uncompressed per-bundle transaction/version encodings including duplicates;
Jazz's captured Client physical-class estimate is about 19.16 MB. SQL's conservative
representation does more storage work. It also omits maintained IVM state and
other runtime protocol machinery; see SYNC.md before interpreting ratios.

PostgreSQL also showed that preparing a newly populated query engine matters:
without refreshing statistics, a trial Client query pass took about 1.62 s.
With the refresh charged to ingestion (about 0.19–0.22 s), reads took about
0.075 s. This is an observed trial contrast, not an isolated multi-round A/B.

## Fresh Jazz checkpoint

Three optimized native runs with only lightweight tick-boundary timers took
13.277, 13.267 and 12.775 seconds total (median **13.267 s**). Per-node tick
medians were **Core 2.729 s, Edge 5.620 s, Client 2.892 s**. Remaining time is
mostly connect/subscription setup and final accounting. Component medians do
not reconstruct a single run's total; raw receipts retain exact per-run values.

The roughly 8.5 s in receiver nodes is **not pure storage time**: it includes
query execution, maintained runtime initialization, publication and protocol
processing. Likewise Core's 2.7 s includes serving/runtime work despite its
application rows already being present. This prevents attributing the entire
gap to the inherent necessity of writing replicated rows. The SQL reference
shows the expanded task can be substantially cheaper, even with its oversized
persistent envelopes, but a finer Jazz node split is needed to locate the
remaining work.

## Decoder boundary

The real Jazz postcard/JVRR decoder plus field extraction, retaining the decoded
batch, took three-run medians of **111 ms for Core → Edge** and **65 ms for
Edge → Client**. Capture JSON/hex parsing was outside those timers. These are
separate diagnostic timings, not part of the SQL total, and should not be added
and relabeled as a measured full-network result. They make the predecoded-input
omission explicit and quantify this particular decoder's scale.

## Validation and provenance

All four configurations verify every visible field, every persisted transaction
and version-envelope byte, captured duplicate order/counts, rejection and rollback
of conflicting bytes, and equality after close/reopen. The 39 queries return
27,518 rows. The Edge holds 46,740 distinct versions and Client 27,518, matching
the actual Jazz receiver counts. There is one data-ingest transaction per node;
ANALYZE has its own engine bookkeeping/transaction cost, included in the timer.

SQLite 3.45.1 WAL uses NORMAL or FULL synchronous mode. PostgreSQL 17.10 runs
with fsync and full_page_writes enabled, and synchronous_commit off/on. All SQL
nodes have separate stores on one host; PostgreSQL databases share an isolated
cluster/WAL writer. No owned compilation or other benchmark overlapped these
measurement rounds. This is not a WAN or power-failure acceptance test.

Raw receipts are preserved locally under
`/home/ubuntu/jazz-debug-evidence/permissioned-profile/sql-sync/`, including the
four `<engine>-<durability>.json` results, the original captures, and the two
`decode-<hop>.jsonl` diagnostics. Captures are large synthetic artifacts and are
not committed.
