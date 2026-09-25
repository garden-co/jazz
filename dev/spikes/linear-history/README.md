# Spike: Core-sequenced linear history

A measurement spike, not a Jazz subsystem. It asks what the storage/decision
path costs if row history is a **linear log ordered by Core** instead of a
per-row version DAG (parents, merge versions, merge heads, pending edges, a
separate deletion register). Byte layouts here are deliberately simple and
**not durable contracts**.

## Model

| Concern              | Today (DAG)                                                   | Spike (linear)                                                            |
| -------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------- |
| Write unit           | versions with same-row `parents`                              | column patches, no parents                                                |
| Ordering             | HLC + DAG domination; Core assigns `GlobalTime`               | Core assigns one `Seq` per accepted tx                                    |
| Concurrent LWW       | head argmax + minted merge version                            | per-column `(HLC, node)` stamp, folded at Core                            |
| Complex merges       | per-strategy fold over parent closures                        | `ThreeWay(base, ours, theirs)` column function                            |
| Merge base           | parent closure                                                | `AtSeq(s)`: history snapshot; `Inline(v)`: author's own unconfirmed value |
| Deletion             | separate register layer + tables                              | stamped `deleted` register on the row image                               |
| History              | versions keyed by `(row, tx)` + `jazz_global_changes` pointer | full post-image at `(table, row, !seq)` + `(table, seq, row)` change log  |
| Snapshot read at `S` | winner among covered versions                                 | `current` if `row.seq <= S`, else one seek into history                   |
| Exclusive tx         | read sets + predicate output compared at `base_snapshot`      | optimistic: change log in `(base, now]`                                   |
| Rejection            | cascade over pending edges                                    | client drops that tx and replays the rest                                 |

`AtSeq` is correct when the author's base was confirmed state. When a client
chains offline edits on its own unconfirmed write, it ships that base value
inline: using Core's post-image of the earlier transaction as the base would
double-count concurrent effects. See
`tests/semantics.rs::chained_pending_three_way_writes_ship_inline_bases_and_do_not_double_count`.

## Semantics covered (`cargo test -p jazz-linear-history-spike`)

These tests run on Memory and RocksDB, in both the "current+history" and
"history-only" layouts. Each tests one property:

- per-column LWW without parents;
- the stale stamp loses whatever the arrival order;
- three-way counter from history bases;
- chained pending with inline bases;
- three-way set with concurrent add/remove;
- rejection without cascade;
- predicate-phantom detection from the change log;
- snapshot point reads and both query strategies agreeing at every cut;
- a content write does not restore a deleted row;
- idempotent replay.

## Receipts

- Machine: a 4-vCPU / 15 GB VM.
- Source: branch base `ffb16b75` plus this spike.
- Build profile: `perf`.
- Commands:
  - spike: `LH_REPEATS=5 target/perf/linear-profile`, minimum of 5 runs;
  - baseline: `JAZZ_BATCH_ROWS=1500 target/perf/todo-profile`, one run.
- Raw outputs are in `receipts/`.

### Write path: 1,350 updates of one indexed column in one transaction (RocksDB `WalNoSync`)

|                                                         |    ms | writes | writes/row |     bytes | bytes/row |
| ------------------------------------------------------- | ----: | -----: | ---------: | --------: | --------: |
| Jazz today: worker ingest (`batch_worker_ingest`)       | 178.8 |  8,102 |        6.0 | 1,703,641 |     1,262 |
| – of which the storage write alone (`storage_write_us`) |  11.5 |        |            |           |           |
| Spike: Core apply, LWW column (index moves)             |   7.7 |  6,752 |        5.0 |   473,887 |       351 |
| Spike: Core apply, three-way counter from `AtSeq` bases |   7.0 |  4,052 |        3.0 |   391,537 |       290 |
| Spike: one transaction per row (1,350 transactions)     |  21.5 |  6,750 |        5.0 |   406,350 |       301 |

How to read this:

- **Writes per row.** Jazz writes 6 per row: history row, history index, and 4 others. The spike writes 3 (current row, history post-image, change-log entry), plus 2 when an indexed value moves (delete + set). That is half as many writes per row for non-indexed changes, and 3.6× fewer bytes.
- **Timings are not comparable end to end.** The spike excludes IVM and maintained queries, permissions, wire decode, and Jazz's value/record conversions. The fair comparison is the storage-plus-decision slice: 7–8 ms in the spike against 11.5 ms for Jazz's storage write alone. The remaining ~167 ms of Jazz worker ingest is work above storage. Earlier profiles attribute a large share of it to version and transaction bookkeeping (parent checks, known-transaction matching, preflight conflicts, value round-trips), but only an in-Jazz port can say how much disappears.

### Read path (RocksDB, 1,500 rows, depth 10 vs 100 extra update rounds)

|                                                              |                  depth 10 |      depth 100 |
| ------------------------------------------------------------ | ------------------------: | -------------: |
| current table scan                                           |                    2.0 ms |         1.3 ms |
| 1,500 current point reads                                    |                    1.2 ms |         1.3 ms |
| 1,500 point reads at the seed cut (oldest)                   |                    4.6 ms |        17.7 ms |
| table scan at the seed cut                                   |                    5.3 ms |        13.6 ms |
| indexed query at the seed cut: forward / rewind              |             5.1 / 13.0 ms | 13.5 / 55.4 ms |
| indexed query at a recent cut, 15-row tail: forward / rewind |              2.0 / 1.5 ms |   1.4 / 4.6 ms |
| `changes_since(recent cut)` sync payload                     | 15 rows, 1,650 B, 0.02 ms |           same |
| exclusive predicate validation: clean / conflict             |             0.03 / 4.5 ms |              – |

Findings:

1. **Current reads don't depend on history depth.** Keep a separate current table: the history-only layout saves one write per row but makes current table scans 4–7× slower (`receipts/`).
2. **Old-cut reads are one seek per row, yet still grow with depth** (about 3 µs per row at depth 10, about 12 µs at depth 100). Two things drive this:
   - The RocksDB adapter builds a fresh `DBIterator` for every scan request. A storage primitive for "latest key ≤ K" with iterator reuse would help.
   - An unflushed memtable holding 674k history entries.
   - Storing history under an inverted sequence number (`!seq`, forward seek) already cut this from 6.9 to 4.6 ms at depth 10.
3. **Rewind (current result plus the inverted change-log tail) wins only for short tails.** In memory it is 0.10 ms against 0.70 ms forward. It loses badly for old cuts because it is O(changes since the cut). A planner should pick between the two using the size of the change-log range.
4. **Index churn in the LSM hurts rewind at depth.** At depth 100, a recent-cut rewind takes 4.6 ms against 1.5 ms at depth 10: every indexed-value move leaves a tombstone. Jazz's overwrite indexes share this cost; it is not specific to linear history.
5. **Three-way merges from `AtSeq` bases** cost one extra history seek per row (2,701 reads against 1,351), with no measurable time difference here.
6. **Exclusive-transaction validation never evaluates a query at the base cut.** A clean validation costs one change-log probe. A conflicting one currently reads the whole change-log range before exiting at the first conflict; a streaming cursor would stop earlier.

The sync payload of current row images is 157 KB for 1,420 rows. Jazz's `batch_payload` is 2.44 MB uncompressed for 1,350 rows, but it also carries query membership, witnesses and authorization data. The two are listed for scale only.

## Not modelled

- IVM and maintained subscriptions.
- Permissions.
- Branch keys.
- Lenses and schema aliases.
- Large values.
- The IndexedDB backend (suspended reads).
- Relays.
- Wire codecs.
- Multi-node settlement and durability tiers.
- Aggregate-predicate validation.
- History GC.

## Reproduce

```sh
cargo test -p jazz-linear-history-spike
cargo build -p jazz-linear-history-spike --profile perf
LH_ROWS=1500 LH_UPDATE_PERCENT=90 LH_DEPTH=10 LH_REPEATS=5 target/perf/linear-profile
# baseline
cargo build -p jazz-example-todo-benchmark --bin todo-profile --profile perf
JAZZ_BATCH_ROWS=1500 target/perf/todo-profile
```
