# Exclusive read evidence in `jazz_transactions` (#3663)

Compatibility write-up for `jazz.exclusive-read-evidence.v1`, required by the
AGENTS.md "Durable encodings" rule before the change lands. Target release:
`2.0.0-alpha.60`, the breaking release that also carries linear history
(#3281) and read tiers (#3659).

## Problem and current state (origin/main `35b91c75`)

An exclusive transaction proves serializability with evidence captured while
it was open: its base `Snapshot` and its row, absent-row and predicate read sets
(SPEC 3 §3.5, §3.7). The evidence travels on the `Transaction` in the
`CommitUnit` and the authority validates it there.

The durable audit row never keeps this evidence:

- `jazz_transactions` has four nullable `Bytes` slots for it, at positions 5–8:
  `base_snapshot`, `row_read_set`, `absent_read_set`, `predicate_read_set`
  (`crates/jazz/layers/model/src/schema.rs` `transactions_table`,
  `crates/jazz/layers/node/src/node/codec.rs` `TransactionRowRecord`).
- `transaction_values_with_cardinality_scope` (`node/codec.rs`) always writes
  `Value::Nullable(None)` into all four slots.
- `stored_transaction_from_record` (`node/currency.rs`) always returns
  `base_snapshot: None` and `None` for all three read sets. It never looks at
  the slots.
- SPEC 2 §2.8 records this as an epoch-1 decision. The slots are "retained
  nullable layout slots" that the audit row "writes null in epoch 1", and the
  evidence "is deliberately not a recovery-time revalidation log".

The in-memory path works. The recovery path does not:

1. `commit_exclusive` persists the transaction as `Pending`/`Local` and hands the
   in-memory commit unit, with its evidence, to the outbox.
2. If the process dies before the fate arrives, the reopened outbox holds only
   tx ids. `peer_connection.rs` rebuilds each unit with
   `NodeState::commit_unit_for` (`node/state/commit.rs`), which reads the
   stored transaction back with no evidence.
3. The authority's `validate_exclusive_commit_unit` (`node/ingest/fates.rs`)
   returns `false` when `base_snapshot` is `None`. `ingest_commit_unit`
   (`node/ingest/commit_bundles.rs`) then rejects with `ExclusiveConflict`.

Every exclusive transaction that was in flight at a crash is therefore
rejected on replay as a conflict, even though no conflicting write exists.
SPEC 3 §3.10 lets a client retransmit a committed unit until it sees the fate,
but it cannot, because the payload it would retransmit is gone. The same
applies to a Local relay that restarts while holding unfated exclusive units.

## Decision

An exclusive transaction's audit row stores its original evidence in the four
existing slots. The transaction is read back with its evidence, so
`commit_unit_for` (outbox recovery, local replay) retransmits exactly the unit
that was originally committed. The authority validates it with the unchanged
§3.7 rules.

Fate rewrites, duplicate delivery and redacted repairs retain this original
proof, including its original absence. They do not backfill proof from later
observations. Retention now lasts with transaction history rather than only
until settlement; this supports exact replay of accepted units after reopen.
The four-slot encoding and codec-family identity are unchanged.

Evidence is written only for `TxKind::Exclusive` rows. Mergeable rows keep all
four slots null. SPEC 2 §2.8 still holds that mergeable rows never confer
serializability.

### Encoding `jazz.exclusive-read-evidence.v1`

Each slot holds `Value::Bytes` that are exactly the Groove typed-record v1
bytes (`groove.typed-record.v1`, `OwnedRecord::raw`) of one fixed descriptor.
There is no additional serializer and no default serde layout. Each record
starts with an explicit `format_v1: U8 = 1` field so a later format can be
recognised rather than misread.

Slot 5, `base_snapshot`, is `jazz_exclusive_base_snapshot_v1`:

| #   | field         | type                                                               |
| --- | ------------- | ------------------------------------------------------------------ |
| 0   | `format_v1`   | `U8` = 1                                                           |
| 1   | `owner`       | `Uuid` (`NodeUuid`)                                                |
| 2   | `global_base` | `U64` (`GlobalTime`)                                               |
| 3   | `local_base`  | `U64` (`TxTime`)                                                   |
| 4   | `dots`        | `Array(Record { time: U64, node: Uuid })`, in the snapshot's order |

Slot 6, `row_read_set`, is `jazz_exclusive_row_reads_v1`:

| #   | field       | type                                                                                     |
| --- | ----------- | ---------------------------------------------------------------------------------------- |
| 0   | `format_v1` | `U8` = 1                                                                                 |
| 1   | `reads`     | `Array(Record { table: String, row_uuid: Uuid, version_time: U64, version_node: Uuid })` |

Slot 7, `absent_read_set`, is `jazz_exclusive_absent_reads_v1`:

| #   | field       | type                                              |
| --- | ----------- | ------------------------------------------------- |
| 0   | `format_v1` | `U8` = 1                                          |
| 1   | `reads`     | `Array(Record { table: String, row_uuid: Uuid })` |

Slot 8, `predicate_read_set`, is `jazz_exclusive_predicate_reads_v1`:

| #   | field       | type                                                                                               |
| --- | ----------- | -------------------------------------------------------------------------------------------------- |
| 0   | `format_v1` | `U8` = 1                                                                                           |
| 1   | `reads`     | `Array(Record { table: String, shape_id: Uuid, query: Bytes, binding_id: Uuid, bindings: Bytes })` |

The two nested byte strings are not new formats. Each reuses an encoding that
is already pinned:

- `query` holds the `Query` in the native query Postcard codec. It is pinned
  by `crates/jazz/fixtures/native_query_codec.json`
  (`crates/jazz/tests/wire_fixtures.rs`) and the TS
  `native-query-codec.test.ts`, and specified by SPEC 19
  "Relation-query Postcard carrier". The decoder requires exact canonical
  Postcard: no trailing bytes, and re-encoding must give the same bytes.
- `bindings` holds the canonical binding bytes `jazz-binding-v0`
  (`crates/jazz/layers/model/src/query/canonical_request.rs`
  `canonical_binding_bytes`). These are the bytes the `BindingId` is already
  derived from (`uuid_v5(QUERY_NAMESPACE, bytes)`). The decoder
  (`binding_values_from_canonical_bytes`, `model/src/query/validation.rs`)
  must re-derive
  exactly the stored `binding_id` and re-encode to identical bytes. v1 decodes
  scalar, tuple, array and nullable binding values (tags 1–15). Record-valued
  bindings (tag 16) are not decodable in v1. A transaction whose predicate
  bindings contain one is stored without evidence, which fails closed exactly
  as today (see "Limits").

The `None` versus `Some(empty)` distinction of each read set is kept: a slot
is null when the `Transaction` field is `None`, and a record with an empty
`reads` array when it is `Some(vec![])`.

Evidence is all-or-nothing per row. If any component cannot be encoded, all
four slots are written null. The writer also decodes what it just encoded and
compares it with the transaction's evidence; on any difference it writes all
four null. This matters on a relay, which stores downstream units it did not
author: a malformed unit (for example a `binding_id` that is not the hash of
its bindings) is stored without evidence and fails closed on replay, instead
of leaving a row that every later read rejects. The decoder does not check
that `shape_id` matches `query`; the authority re-derives the shape when it
validates, and a mismatch fails validation there.

Decoding follows the native-record rules of `local-row-availability.v1`:

- The bytes must decode under the exact descriptor.
- Re-encoding the decoded values must reproduce the stored bytes.
- `format_v1` must be 1.
- The query and bindings checks above must pass.

A malformed, non-canonical or unknown-version slot fails the transaction read
with `InvalidStoredValue`, the same fail-closed stance as the other epoch-1
records (`INV-DATA-23`). So do read sets stored without a base snapshot and
any non-null slot on a `Mergeable` row. A null slot is valid and means "no
evidence".

Groove typed records delimit their last variable-width field by the length of
the stored value. The row and absent read records end with a table name, so a
value cut inside that name decodes as a shorter, still canonical name. That is
a property of every Groove record, not of this family: the ordered-KV store
owns value integrity. Cuts into fixed-width fields, and into the nested query
and binding bytes, are structural and are rejected.

The implementation is `crates/jazz/layers/node/src/node/exclusive_read_evidence.rs`.
It is written by `transaction_values_with_cardinality_scope` (`node/codec.rs`)
and read by `stored_transaction_from_record` (`node/currency.rs`). Maintained
view bundles (`node/views.rs`) set `base_snapshot` to `None` explicitly.

Pinned fixture: a pending exclusive transaction with owner `01…01`,
`global_base = 3`, `local_base = TxTime::from(12)`, dots `(10, 01…01)` and
`(11, 02…02)`; one row read `todos/22…22` at `(10, 01…01)`; one absent read
`todos/33…33`; and one predicate read of `Query::from("todos")`, shape
`44…44`, bindings `{done: true}`. It encodes to:

| slot | hex                                                                                                                                                                                                                            |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 5    | `010101010101010101010101010101010103000000000000000000300000000000020000002000000000002800000000000101010101010101010101010101010100002c000000000002020202020202020202020202020202`                                           |
| 6    | `01010000002222222222222222222222222222222200002800000000000101010101010101010101010101010102746f646f73`                                                                                                                       |
| 7    | `01010000003333333333333333333333333333333302746f646f73`                                                                                                                                                                       |
| 8    | `010100000044444444444444444444444444444444d327cd149e8f531dbb3fd829eba589f22e0000004300000002746f646f730205746f646f730000000000000000000000000000026a617a7a2d62696e64696e672d763000000000000000010000000000000004646f6e650601` |

### Storage epoch, profile and registry

The Groove table schema of `jazz_transactions` does not change: same columns,
same types (`Bytes.nullable()`), same key and index. Only the values written
into existing nullable columns change, so the Groove descriptor bytes and the
storage-epoch manifest are unchanged. Like `jazz.contribution-provenance.v1`
and `jazz.local-row-availability.v1`, the new family is a typed-record family
inside an existing store. It gets a row in
`crates/jazz/fixtures/persistent_codec_family_registry.json` with no manifest
`profile` entry. The epoch-1 JSM1 codec-profile receipt and its checksum are
unchanged, and no new storage epoch is needed.

The row points to:

- the SPEC 2 §2.8 amendment;
- the exact-byte fixture test;
- the malformed/non-canonical rejection test;
- the restart/replay regression test.

## Mixed versions

The wire format does not change. `SyncMessage::CommitUnit` already carries
`Transaction.base_snapshot` and the three read sets as `Option`s, and a
restarted new client now fills them with exactly what an old client sent
before its crash. No wire message, field, tag or frame changes, so
`jazz.wire-frame.v1` and `jazz.binding-abi.v1` are untouched. A replayed unit
matches the original payload, and `known_transaction_payload_matches` already
tolerates evidence being present or absent on duplicates.

| Combination                                      | Behaviour                                                                                                                                                                                                                                                                                      |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| New client, new server                           | An in-flight exclusive transaction replays after restart with its evidence and is validated normally. It is accepted, or rejected only for a real conflict.                                                                                                                                    |
| New client, old server                           | The old server already validates evidence carried on the unit, so the replay validates normally. The fix is client-side and helps old servers too.                                                                                                                                             |
| Old client, new server                           | The old client still replays without evidence. The new server rejects it with `ExclusiveConflict`, as today. Nothing gets worse.                                                                                                                                                               |
| New binary opens an old data dir                 | Rows written by the old binary have null slots, so a transaction that was in flight at the upgrade is rejected on replay, as today. Rows written after the upgrade replay correctly. No migration and no rewrite of old rows.                                                                  |
| New data dir opened by an old binary (downgrade) | The old decoder never reads slots 5–8, so the rows decode as before and the evidence is ignored. An in-flight exclusive transaction is rejected on replay, as today. No corruption and no new failure. Downgrade is not a supported path across alpha.60 anyway, because #3281 breaks storage. |
| Relay (Local tier) restart                       | The relay stores forwarded unfated exclusive units with evidence and re-forwards them intact. Old relays behave as today.                                                                                                                                                                      |

## Security and correctness

- Conflict detection is not weakened. The authority runs the same
  `validate_exclusive_commit_unit` on the same evidence it would have received
  before the crash. A write that really conflicted while the node was down is
  still rejected, and a unit with no evidence is still rejected. The only
  change is that honest evidence is no longer lost.
- Exclusive-transaction conflict detection is a correctness guarantee for
  honest writers, not a security boundary. A client could always send
  arbitrary evidence, so persisting and replaying our own evidence gives an
  attacker nothing new.
- The evidence stays on the node that authored or relayed the unit, and it
  already went over the wire to the authority. Maintained view bundles keep
  sending `base_snapshot: None` and no read sets to subscribers. Their
  construction explicitly redacts the stored evidence, so subscribers see no
  new data.

## Interaction with other alpha.60 storage work

- **#3281 (linear Core-sequenced history).** No semantic conflict.
  - On that branch `jazz_transactions` keeps the same four nullable `Bytes`
    slots at positions 5–8 (`schema.rs` `transactions_table`, codec
    `TransactionRowRecord`). It still writes them null and still reads them
    back as `None`.
  - `validate_exclusive_commit_unit` still requires `base_snapshot` and
    validates against snapshot dots. The PR says so ("Exclusive transactions
    still validate against snapshot dots, not base seq S plus read set", #3473).
  - `commit_unit_for` still rebuilds outbox units from storage. That branch has
    the same bug, and this change fixes it there once rebased.
  - Expected textual conflicts: `codec.rs`
    (`transaction_values_with_cardinality_scope`), `currency.rs`
    (`stored_transaction_from_record`), and SPEC 2 §2.8. #3281 also defers its
    byte fixtures and codec registry, so the registry row added here lands
    first and carries over.
  - If #3473 later moves exclusive validation to "base seq + read set", the
    snapshot record becomes a `v2` family (`format_v1` → a new record).
    Retained v1 evidence would need an explicit storage transition; it cannot
    be reinterpreted using the later grammar.
- **#3673 (4-byte author aliases, stacked on #3281).** Changes
  `jazz_transactions.made_by` to a `U32` alias and decodes it in
  `stored_transaction_from_record`. This is a textual conflict only, in the
  same function, on a different field.
- **#3675 (retire or restate DAG-era invariants).** Covers TX-5/6/8/23 and
  others whose tests #3281 deleted. None of them concern exclusive evidence
  persistence. INV-TX-16/17/18/20 (read and CAS validation) are unchanged and
  keep their tests.
- **#3659 (read tiers).** Does not touch `jazz_transactions`.

## Limits and non-goals

- **Record-valued predicate bindings.** A predicate read whose bindings contain
  a record value cannot be decoded in v1. That transaction is persisted without
  evidence and, if in flight at a crash, is rejected on replay as before. A
  later `v2` can add the descriptor half of `jazz-binding-v0` tag 16.
- **Pending transactions from before the upgrade.** Old rows cannot be
  recovered. They were written without evidence.
- **Evidence lifetime.** Original evidence remains with the transaction history,
  including settled units. This increases retained history size; it does not
  reconstruct proof or make absence of proof authorisation evidence.
- **Wire and binding ABI.** No wire, binding ABI or Groove schema changes.

## Evidence

- Exact bytes (`jazz-node`):
  `node::exclusive_read_evidence::tests::exclusive_read_evidence_v1_bytes_are_pinned`.
- Rejection (`jazz-node`):
  `node::exclusive_read_evidence::tests::exclusive_read_evidence_rejects_malformed_noncanonical_and_unknown_versions`
  and `node::tests::harness::mergeable_transaction_row_with_exclusive_evidence_is_rejected`.
- Write policy (`jazz-node`):
  `node::exclusive_read_evidence::tests::exclusive_read_evidence_is_written_only_when_replayable`.
- Lifecycle (`jazz-node`):
  `node::tests::harness::exclusive_evidence_is_stored_while_pending_and_cleared_at_settlement`.
  It checks that the stored row carries all four slots while pending, that
  `commit_unit_for` after reopen equals the published unit, and that the slots
  are null after the authority's fate is applied.
- View carriers (`jazz-node`):
  `node::tests::harness::view_bundle_for_a_pending_exclusive_transaction_matches_its_stored_evidence`.
  Core's view bundle for an exclusive transaction never carries evidence,
  while its author still holds the pending row with evidence. The view
  receiver's stored-versus-incoming identity checks (`node/views.rs`) compare
  the payload through `known_transaction_payload_matches`, which already
  ignores evidence on either side, instead of exact equality.
- Relay (`jazz-node`):
  `node::tests::harness::relay_stores_only_decodable_exclusive_evidence_and_replays_after_restart`.
  A relay stores a genuine downstream unit with evidence and, after a
  restart, `commit_unit_for` returns it unchanged and Core accepts it. A unit
  with a mismatched `binding_id` is stored without evidence; after the
  restart it still replays, Core rejects it, and the fate lands.
- Binding decoder (`jazz-model`):
  `query::tests::canonical_binding_bytes_decode_round_trips_and_rejects_the_rest`.
- End to end (`jazz-db`, `db::tests::node_runtime`), each through a real
  RocksDB close and reopen:
  - `exclusive_transaction_in_flight_at_restart_replays_without_false_conflict`.
    This is the #3663 regression. It fails with `ExclusiveConflict` when
    evidence is not persisted and passes with it.
  - `exclusive_transaction_replayed_after_restart_still_rejects_a_real_conflict`.
  - `exclusive_transaction_without_persisted_evidence_is_still_rejected_on_replay`.
- Registry: `jazz.exclusive-read-evidence.v1` in
  `crates/jazz/fixtures/persistent_codec_family_registry.json`, verified by
  `crates/jazz/tests/persistent_codec_family_registry.rs`.
