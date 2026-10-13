# Transaction record v2 (`jazz.tx-record.v2`)

Compatibility write-up for the alpha.60 transaction record change (#4012,
#3768 and the tx metadata diet), required by the AGENTS.md "Durable encodings"
rule. Target release: `2.0.0-alpha.60`, the coordinated breaking release that
also carries linear history (#3281). The storage codec version bump and the
fixture regeneration are done once for the whole release by the linear
history change, not here.

## Old form (linear history, #3281)

`jazz_transactions`, keyed `(time, node_id)`, held every fact about a
transaction in one record:

- identity and status: time, node_id, kind, n_total_writes, fate, global_time,
  durability;
- `made_by` as a full `RowAuthor` record (71 bytes for a short test author);
- the four exclusive read-evidence slots and `permission_subject`, kept for the
  transaction's whole life although they matter only while it is pending;
- `contribution_merge` provenance, which grows with the transaction's branch
  writes;
- a merge-strategy string marking a view-scoped write count;
- three rejection columns, one of them a fixed-width nullable tuple that cost
  25 bytes on every transaction that was not rejected.

Every fate, seq or durability read loaded all of it. History rows stored
`base_pending` as a fixed-width nullable tuple: 25 bytes even when null, which
is every settled write.

## New form

SPEC 2 §2.8 has the normative layout. In short:

| table                       | key               | holds                                                                                                                                                          | lifetime                                          |
| --------------------------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `jazz_transactions`         | `(time, node_id)` | time, node_id, kind, flags `U8`, n_total_writes, `made_by` as a `U32` author code, fate, global_time, durability, user_metadata, one nullable rejection record | forever                                           |
| `jazz_tx_provenance`        | `(time, node_id)` | the contribution provenance record                                                                                                                             | forever, only when flag bit 1 is set              |
| `jazz_pending_transactions` | `(time, node_id)` | permission_subject and the four read-evidence slots                                                                                                            | until the batch that records a seq or a rejection |
| `jazz_authors`              | `code: U32`       | canonical `RowAuthor` bytes, unique value index                                                                                                                | forever, append-only                              |
| `jazz_history_horizon`      | `id: U8 = 0`      | the seq at or below which history was truncated                                                                                                                | forever; this release requires 0                  |

`base_pending` in history becomes a nullable `{time: U64, node: Uuid}` record.

Exclusive read evidence moves to `jazz.exclusive-read-evidence.v2` (#3768):
the predicate-read slot gains each aggregate read's recorded result
(`jazz.aggregate-read-result.v1`), and aggregate reads no longer record the
rows they consumed. The other three slots keep their v1 bytes.

Byte pins: `node::codec::transaction_record_tests`,
`aggregate_read_result::tests`, `exclusive_read_evidence::tests` and
`jazz-protocol` `wire::tests::predicate_read_aggregate_result_wire_bytes_are_pinned`.

## Wire

The transaction record change is storage-only. A `Transaction` on the wire
still carries its full author, provenance, permission hint and read evidence;
author codes never leave the node.

`PredicateRead` gains a trailing `aggregate_result` (#3768). Old and new peers
cannot decode each other's units, so mixed peers must be refused at Hello by
the alpha.60 wire version bump. If an old-form unit did reach a new authority,
its aggregate read would have no result and be rejected as `MalformedCommit`,
never silently accepted.

## Upgrade path

None. alpha.60 is a coordinated breaking release, so a store written by an
earlier build is refused by the storage codec profile before any of these
decoders runs, as for earlier layout changes. A store from this build that a
later release truncates is refused by this build with
`TruncatedHistoryUnsupported` instead of being read as if its history were
complete.

## Decisions a reader should know

- **Settled transactions keep no permission hint or read evidence.** Duplicate
  detection treats a stored copy without the permission hint as matching a
  redelivery that carries one, as it already did for read evidence.
- **Author codes are node-local and append-only.** An entry is written in the
  same batch as the first record that uses its code, so no durable record can
  name a code without an entry. A batch that is dropped before it applies
  keeps its code reserved in memory, so no code is handed to two authors.
- **Only the transaction record uses author codes so far.** History and
  current rows still store full authors, because `$createdBy` and `$updatedBy`
  are query fields that can be filtered, sorted and projected; coding them
  needs query engine support and would be a later layout change.
- **The horizon is reserved, not used.** Truncation itself is alpha.61. New
  side tables can be added later without a codec change, and truncation can
  keep the winning transactions' records, so the horizon record and its
  refusal are all this release needs.
- **An exclusive aggregate read after a write in the same transaction is
  refused** with `UnsupportedExclusiveRead` (#3768). The authority re-runs the
  aggregate over committed state, which does not include the transaction's
  own writes, so such a result cannot be compared soundly.

## Measured size

`crates/jazz/tests/storage_per_row_bytes.rs` (1000 todos, each inserted and
then updated in its own transaction, so 2001 transactions; one author; raw key
plus value bytes on RocksDB). Core and the writer client store the same
per-transaction bytes:

| per transaction                          | before (#3281, `tx-metadata` probe) |   after |
| ---------------------------------------- | ----------------------------------: | ------: |
| `jazz_transactions` record (key + value) |                                 287 |      83 |
| `by_global_time` index entry             |                                  30 |      30 |
| `jazz_tx_touched_rows` record            |                                  97 |      97 |
| **transaction metadata**                 |                             **414** | **210** |

`jazz_authors` adds one entry and one index entry per distinct author (about
180 bytes), not per transaction. History images are 20 bytes smaller from the
variable-width `base_pending`.

The rest of the way to about 120 bytes per transaction is the touched-row
list, which settled transactions could delete. That changes no layout, so it
can land after alpha.60 without another format break.
