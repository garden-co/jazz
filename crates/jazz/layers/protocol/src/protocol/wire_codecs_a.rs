//! Sync-wire codecs for [`SyncMessage`] and the message, carrier and key
//! types declared ahead of `ShapeAst` in `protocol.rs`.

use super::*;
use crate::codec::{
    Bytes, CodecError, Wire, WireCodec, get_bytes, get_len, put_bytes, record_encode_error,
};
use crate::protocol_limits::MAX_CHUNK_REQUEST_BATCH_ENTRIES;

impl WireCodec for ReservedWireMessage {
    fn put(&self, _: &mut Vec<u8>) {
        match *self {}
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        // Only `SyncMessage`'s retired variants carry this payload, so the
        // byte just read is their one-byte tag.
        Err(CodecError::InvalidTypeTag {
            tag: data[..*offset].last().copied().unwrap_or_default(),
            context: "SyncMessage",
        })
    }
}

crate::wire_enum! {
    for SyncMessage {
        0 => ChunkRequestBatch(batch),
        1 => ChunkResponseBatch(batch),
        2 => SessionClaims { identity, claims },
        3 => CommitUnit { tx, versions },
        4 => FateUpdate { tx_id, fate, global_time, durability },
        5 => RegisterShape { shape_id, ast, opts },
        6 => Subscribe(subscribe),
        7 => SubscribeRejected { subscription, reason },
        8 => Unsubscribe { subscription },
        9 => PublishSchema { author, schema },
        10 => PublishSchemaWithLens { author, catalogue_seq, publication },
        11 => PublishLens { author, lens },
        12 => CatalogueAck(ack),
        13 => ViewUpdate(view),
        14 => Reserved14(reserved),
        15 => Reserved15(reserved),
        16 => CatalogueSnapshot(snapshot),
        17 => AuthorizationScopeIntent { request_id, action, delegated_session },
        18 => AuthorizationScopeView { request_id, key, clause_index, clause_count, view },
        19 => AuthorizationScopeAggregateReceipt { request_id, receipt },
        20 => AuthorizationScopeUnavailable { request_id },
        21 => AuthorizationScopeDecision { request_id, advice },
        22 => ChunkUploadStart(start),
        23 => ChunkUploadNodes(nodes),
        24 => ChunkUploadResult(result),
        25 => CurrentRowsRequest(request),
        26 => CurrentRowsReceipt(receipt),
        27 => CurrentRowsCancel { request_id },
        28 => ViewUpdatePart(view),
        29 => Reserved29(reserved),
        30 => ReadBarrier { writes_through },
    }
}

crate::wire_struct! { for CurrentRowCoordinate { schema, table, physical_table, row } }
crate::wire_struct! { for CurrentRowsRequest { request_id, rows, delegated_session } }
crate::wire_enum! { for CurrentRowOutcome { 0 => Readable, 1 => CurrentUnavailable, 2 => Unknown } }
crate::wire_struct! {
    for CurrentRowsReceipt {
        request_id, rows, outcomes, context, core, core_epoch, claims_revision, policy_epoch,
        settled_through, authorization_progress, version_carriers,
    }
}
crate::wire_struct! {
    for ViewUpdatePayload {
        subscription, settled_through, version_carriers, peer_payload_inventory, supporting_rows,
    }
}
crate::wire_enum! {
    for SupportingRowsUpdate {
        0 => Snapshot { revision, rows },
        1 => Delta { predecessor, revision, adds, removes },
        2 => CatchUp { predecessor, revision, changed, left },
    }
}

impl WireCodec for ChunkRequestBatch {
    fn put(&self, buf: &mut Vec<u8>) {
        self.requests.put(buf);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        // The declared count is checked before anything is allocated.
        let len = get_len(data, offset)?;
        if len > MAX_CHUNK_REQUEST_BATCH_ENTRIES {
            return Err(CodecError::Invalid {
                context: "chunk request batch exceeds cardinality limit",
            });
        }
        let requests = (0..len)
            .map(|_| ChunkRequestEntry::get(data, offset))
            .collect::<Result<_, _>>()?;
        Ok(Self { requests })
    }
}

crate::wire_struct! { for ChunkRequestEntry { request_id, locator, expected_hash, remaining_hops } }
crate::wire_struct! { for ChunkResponseBatch { responses } }
crate::wire_struct! { for ChunkResponseEntry { request_id, result } }
crate::wire_enum! {
    for ChunkResponse {
        0 => Found(bytes: Bytes),
        1 => Unavailable,
        2 => Retryable { retry_after_ms },
    }
}
crate::wire_struct! { for ChunkUploadStart { value_ref } }
crate::wire_struct! { for ChunkUploadNodes { value_ref, chunks } }
crate::wire_struct! { for ChunkUploadResult { value_ref, status } }
crate::wire_enum! {
    for ChunkUploadStatus { 0 => Need(nodes), 1 => Staged, 2 => RateLimited, 3 => Rejected }
}

impl WireCodec for PermissionAdviceRequestId {
    fn put(&self, buf: &mut Vec<u8>) {
        self.0.put(buf);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        WireCodec::get(data, offset).map(Self)
    }
}

crate::wire_enum! {
    for PermissionAdviceAction {
        0 => Insert { table, cells },
        1 => Read { table, row },
        2 => Update { table, row, patch },
        3 => Delete { table, row },
    }
}
crate::wire_enum! {
    for AuthorizationScopeOperation { 0 => Read, 1 => Insert, 2 => Update, 3 => Delete }
}
crate::wire_struct! {
    for AuthorizationSupportScopeKey { support_shape_digest, subject, claims_digest, policy_digest }
}
crate::wire_struct! { for AuthorizationOperationKey { operation, table, row, candidate_digest } }
crate::wire_struct! {
    for AuthorizationScopeReceipt {
        key, authority, link, authority_epoch, claims_revision, policy_epoch, settled_through,
        authorization_progress,
    }
}
crate::wire_enum! { for PermissionAdvice { 0 => Allowed, 1 => Denied, 2 => Unknown } }
crate::wire_struct! {
    for CatalogueSnapshot { genesis_physical_identities, schemas, lineages, current_write_schema }
}
crate::wire_struct! { for RowVersionRef { table, row_uuid, tx_time, tx_node_id } }
crate::wire_struct! {
    for PeerPayloadInventory { complete_tx_payloads, authorization_progress, opening_pending }
}

/// A row as its `JVRR` v3 envelope, written as one length-prefixed byte
/// string (postcard spelled it with `serialize_bytes`).
pub(super) struct WireRow;

impl Wire<OwnedRecord> for WireRow {
    fn put(&self, buf: &mut Vec<u8>, record: &OwnedRecord) {
        match version_record_wire_row::encode(record) {
            Ok(bytes) => put_bytes(buf, &bytes),
            Err(error) => record_encode_error(CodecError::Encode {
                message: error.to_string(),
            }),
        }
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<OwnedRecord, CodecError> {
        version_record_wire_row::decode(get_bytes(data, offset)?).map_err(|error| {
            CodecError::DecodeError {
                message: error.to_string(),
            }
        })
    }
}

crate::wire_struct! {
    for VersionRecord {
        table, schema_version, branch_key, record: WireRow, authored_columns, base,
        counter_signs: Bytes,
    }
}
crate::wire_struct! { for RowBase { seq, pending } }
crate::wire_enum! { for VersionBundleScope { 0 => CompleteTransaction, 1 => ViewScoped } }
crate::wire_struct! { for VersionBundle { tx, versions, scope, fate, global_time, durability } }
crate::wire_enum! { for VersionCarrier { 0 => Bundle(bundle), 1 => Run(run) } }
crate::wire_struct! { for VersionBundleRun { header, bodies, overrides } }
crate::wire_struct! {
    for VersionBundleRunHeader { table, tx, scope, body_count, fate, global_time, durability }
}
crate::wire_struct! { for VersionBundleRunBody { versions } }
crate::wire_struct! {
    for VersionBundleRunOverride { body_index, tx, scope, fate, global_time, durability }
}
crate::wire_struct! { for SubscriptionKey { shape_id, binding_id, read_view } }
crate::wire_struct! { for BindingViewKey { shape_id, binding_id, read_view } }
crate::wire_struct! { for AuthorityResultKey { binding_view, policy_binding } }
crate::wire_struct! { for CoverageKey { shape_id, binding_id, opts, policy_binding } }
crate::wire_struct! { for PolicyBindingKey { identity, canonical_claims } }

impl WireCodec for CanonicalPolicyClaims {
    fn put(&self, buf: &mut Vec<u8>) {
        self.claims.put(buf);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        WireCodec::get(data, offset).map(Self::new)
    }
}

#[cfg(test)]
mod tests {
    // White-box byte tests: the hand codecs must reproduce the frozen
    // postcard layout exactly, which no public API can observe.
    use super::*;
    use crate::codec::{decode_exact, encode, take_encode_error};
    use crate::tx::TxKind;
    use groove::schema::{ColumnSchema, ColumnType};

    /// Encodes `value` in its own message descriptor scope and requires an
    /// exact decode back to `value` that re-encodes to the same bytes. Byte
    /// identity with the postcard layout is pinned by the frozen fixtures.
    fn assert_round_trips<T>(value: &T)
    where
        T: WireCodec + PartialEq + std::fmt::Debug,
    {
        let bytes = with_message_descriptor_table(|| encode(value));
        assert_eq!(take_encode_error(), None);
        let decoded = with_message_descriptor_table(|| decode_exact::<T>(&bytes)).unwrap();
        assert_eq!(&decoded, value);
        assert_eq!(
            with_message_descriptor_table(|| encode(&decoded)),
            bytes,
            "{value:?}"
        );
    }

    fn author(byte: u8) -> AuthorSubject {
        AuthorSubject::for_test_bytes([byte; 16])
    }

    fn tx_id(time: u64) -> TxId {
        TxId::new(TxTime(time), NodeUuid::from_bytes([0x11; 16]))
    }

    fn transaction(time: u64) -> Transaction {
        Transaction {
            tx_id: tx_id(time),
            kind: TxKind::Mergeable,
            n_total_writes: 2,
            made_by: author(0x55),
            permission_subject: None,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: None,
            contribution_merge: None,
        }
    }

    fn version(table: &str, row: u8, value: u8) -> VersionRecord {
        let schema = TableSchema::new(table, [ColumnSchema::new("count", ColumnType::U8)]);
        VersionRecord::from_cells(
            &schema,
            SchemaVersionId::from_bytes([0x44; 16]),
            RowUuid::from_bytes([row; 16]),
            author(1),
            1,
            author(2),
            u64::MAX,
            &BTreeMap::from([("count".to_owned(), Value::U8(value))]),
            None,
        )
        .unwrap()
    }

    /// Two rows of one table (the second references the first's descriptor)
    /// and one of another, with every optional field populated somewhere.
    fn versions() -> Vec<VersionRecord> {
        vec![
            version("todos", 1, 0),
            version("todos", 2, 255)
                .with_authored_columns(Some(BTreeSet::from(["count".to_owned()])))
                .with_base(RowBase {
                    seq: Some(GlobalTime(5)),
                    pending: Some(tx_id(7)),
                })
                .with_counter_signs(vec![0b1]),
            version("notes", 3, 9).with_authored_columns(Some(BTreeSet::new())),
        ]
    }

    fn subscription() -> SubscriptionKey {
        SubscriptionKey {
            shape_id: ShapeId(uuid::Uuid::from_bytes([0x22; 16])),
            binding_id: BindingId(uuid::Uuid::from_bytes([0x33; 16])),
            read_view: Default::default(),
        }
    }

    fn claims() -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("role".to_owned(), Value::String("editor".to_owned())),
            ("level".to_owned(), Value::U64(u64::MAX)),
        ])
    }

    fn bundle(scope: VersionBundleScope, fate: Fate) -> VersionBundle {
        VersionBundle {
            tx: transaction(12),
            versions: versions(),
            scope,
            fate,
            global_time: Some(GlobalTime(300)),
            durability: DurabilityTier::Global,
        }
    }

    fn carriers() -> Vec<VersionCarrier> {
        let run = VersionBundleRun {
            header: VersionBundleRunHeader {
                table: Some(groove::Intern::new("todos".to_owned())),
                tx: transaction(1),
                scope: VersionBundleScope::ViewScoped,
                body_count: 2,
                fate: Fate::Accepted,
                global_time: None,
                durability: DurabilityTier::Local,
            },
            bodies: vec![
                VersionBundleRunBody {
                    versions: vec![version("todos", 4, 1)],
                },
                VersionBundleRunBody {
                    versions: Vec::new(),
                },
            ],
            overrides: vec![
                VersionBundleRunOverride {
                    body_index: 1,
                    tx: Some(transaction(2)),
                    scope: Some(VersionBundleScope::CompleteTransaction),
                    fate: Some(Fate::Accepted),
                    global_time: Some(None),
                    durability: Some(DurabilityTier::Global),
                },
                VersionBundleRunOverride {
                    body_index: u32::MAX,
                    tx: None,
                    scope: None,
                    fate: None,
                    global_time: Some(Some(GlobalTime(9))),
                    durability: None,
                },
            ],
        };
        vec![
            VersionCarrier::Bundle(bundle(
                VersionBundleScope::CompleteTransaction,
                Fate::Accepted,
            )),
            VersionCarrier::Run(run),
        ]
    }

    fn view(supporting_rows: SupportingRowsUpdate) -> ViewUpdatePayload {
        ViewUpdatePayload {
            subscription: subscription(),
            settled_through: GlobalTime(500),
            version_carriers: carriers(),
            peer_payload_inventory: PeerPayloadInventory {
                complete_tx_payloads: vec![tx_id(1), tx_id(2)],
                authorization_progress: Some(3),
                opening_pending: true,
            },
            supporting_rows,
        }
    }

    fn scope_key() -> AuthorizationSupportScopeKey {
        AuthorizationSupportScopeKey {
            support_shape_digest: [1; 32],
            subject: author(3),
            claims_digest: [2; 32],
            policy_digest: [255; 32],
        }
    }

    fn coordinate(row: u8) -> CurrentRowCoordinate {
        CurrentRowCoordinate {
            schema: SchemaVersionId::from_bytes([0x44; 16]),
            table: "todos".to_owned(),
            physical_table: crate::ids::GlobalPhysicalTableId(uuid::Uuid::from_bytes([row; 16])),
            row: RowUuid::from_bytes([row; 16]),
        }
    }

    fn policy_binding() -> PolicyBindingKey {
        PolicyBindingKey::from_canonical_parts(author(4), claims())
    }

    /// The frozen wire message fixtures (written by the postcard codec).
    fn fixture_messages() -> Vec<(String, Vec<u8>, SyncMessage)> {
        let manifest: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/wire_message_frames_v6.json"
        ))
        .unwrap();
        manifest["fixtures"]
            .as_array()
            .unwrap()
            .iter()
            .map(|fixture| {
                let payload = hex::decode(fixture["payload_hex"].as_str().unwrap()).unwrap();
                let message = with_message_descriptor_table(|| {
                    decode_exact::<SyncMessage>(&payload).unwrap()
                });
                (
                    fixture["name"].as_str().unwrap().to_owned(),
                    payload,
                    message,
                )
            })
            .collect()
    }

    /// Every frozen wire message fixture encodes to its pinned payload bytes
    /// and decodes back from them.
    #[test]
    fn wire_message_fixtures_encode_to_their_pinned_bytes() {
        let fixtures = fixture_messages();
        assert!(fixtures.len() >= 20);
        for (name, payload, message) in fixtures {
            let actual = with_message_descriptor_table(|| encode(&message));
            assert_eq!(take_encode_error(), None);
            assert_eq!(actual, payload, "{name}");
            let decoded =
                with_message_descriptor_table(|| decode_exact::<SyncMessage>(&payload)).unwrap();
            assert_eq!(decoded, message, "{name}");
        }
    }

    /// Every live `SyncMessage` variant, and every variant of the enums it
    /// carries from this part, round-trips exactly.
    #[test]
    fn every_sync_message_variant_round_trips() {
        let fixtures = fixture_messages();
        let value_ref = fixtures
            .iter()
            .find_map(|(_, _, message)| match message {
                SyncMessage::ChunkUploadStart(start) => Some(start.value_ref.clone()),
                _ => None,
            })
            .unwrap();
        let request_id = PermissionAdviceRequestId([0xab; 16]);
        let receipt = AuthorizationScopeReceipt {
            key: scope_key(),
            authority: [7; 16],
            link: author(5),
            authority_epoch: 0,
            claims_revision: 1,
            policy_epoch: u64::MAX,
            settled_through: GlobalTime(9),
            authorization_progress: 128,
        };
        let mut messages = vec![
            SyncMessage::ChunkRequestBatch(ChunkRequestBatch {
                requests: Vec::new(),
            }),
            SyncMessage::ChunkRequestBatch(ChunkRequestBatch {
                requests: vec![ChunkRequestEntry {
                    request_id: u64::MAX,
                    locator: groove::large_values::Locator::random(),
                    expected_hash: [9; 32],
                    remaining_hops: 255,
                }],
            }),
            SyncMessage::ChunkResponseBatch(ChunkResponseBatch {
                responses: vec![
                    ChunkResponseEntry {
                        request_id: 0,
                        result: ChunkResponse::Found(vec![1; 200]),
                    },
                    ChunkResponseEntry {
                        request_id: 1,
                        result: ChunkResponse::Found(Vec::new()),
                    },
                    ChunkResponseEntry {
                        request_id: 2,
                        result: ChunkResponse::Unavailable,
                    },
                    ChunkResponseEntry {
                        request_id: 3,
                        result: ChunkResponse::Retryable {
                            retry_after_ms: u32::MAX,
                        },
                    },
                ],
            }),
            SyncMessage::SessionClaims {
                identity: author(6),
                claims: claims(),
            },
            SyncMessage::SessionClaims {
                identity: author(6),
                claims: BTreeMap::new(),
            },
            SyncMessage::CommitUnit {
                tx: transaction(1),
                versions: versions(),
            },
            SyncMessage::FateUpdate {
                tx_id: tx_id(3),
                fate: Fate::Accepted,
                global_time: None,
                durability: None,
            },
            SyncMessage::Unsubscribe {
                subscription: subscription(),
            },
            SyncMessage::ViewUpdate(view(SupportingRowsUpdate::Snapshot {
                revision: [1; 16],
                rows: Vec::new(),
            })),
            SyncMessage::ViewUpdatePart(view(SupportingRowsUpdate::Delta {
                predecessor: [1; 16],
                revision: [2; 16],
                adds: Vec::new(),
                removes: Vec::new(),
            })),
            SyncMessage::ViewUpdate(view(SupportingRowsUpdate::CatchUp {
                predecessor: [2; 16],
                revision: [3; 16],
                changed: Vec::new(),
                left: Vec::new(),
            })),
            SyncMessage::AuthorizationScopeIntent {
                request_id,
                action: PermissionAdviceAction::Insert {
                    table: "todos".to_owned(),
                    cells: claims(),
                },
                delegated_session: None,
            },
            SyncMessage::AuthorizationScopeIntent {
                request_id,
                action: PermissionAdviceAction::Read {
                    table: "todos".to_owned(),
                    row: RowUuid::from_bytes([1; 16]),
                },
                delegated_session: None,
            },
            SyncMessage::AuthorizationScopeIntent {
                request_id,
                action: PermissionAdviceAction::Update {
                    table: String::new(),
                    row: RowUuid::from_bytes([2; 16]),
                    patch: claims(),
                },
                delegated_session: None,
            },
            SyncMessage::AuthorizationScopeIntent {
                request_id,
                action: PermissionAdviceAction::Delete {
                    table: "todos".to_owned(),
                    row: RowUuid::from_bytes([3; 16]),
                },
                delegated_session: None,
            },
            SyncMessage::AuthorizationScopeView {
                request_id,
                key: scope_key(),
                clause_index: 0,
                clause_count: u16::MAX,
                view: view(SupportingRowsUpdate::Snapshot {
                    revision: [4; 16],
                    rows: Vec::new(),
                }),
            },
            SyncMessage::AuthorizationScopeAggregateReceipt {
                request_id,
                receipt,
            },
            SyncMessage::AuthorizationScopeUnavailable { request_id },
            SyncMessage::ChunkUploadStart(ChunkUploadStart {
                value_ref: value_ref.clone(),
            }),
            SyncMessage::ChunkUploadNodes(ChunkUploadNodes {
                value_ref: value_ref.clone(),
                chunks: Vec::new(),
            }),
            SyncMessage::CurrentRowsRequest(CurrentRowsRequest {
                request_id,
                rows: vec![coordinate(1), coordinate(2)],
                delegated_session: None,
            }),
            SyncMessage::CurrentRowsReceipt(CurrentRowsReceipt {
                request_id,
                rows: vec![coordinate(1), coordinate(2), coordinate(3)],
                outcomes: vec![
                    CurrentRowOutcome::Readable,
                    CurrentRowOutcome::CurrentUnavailable,
                    CurrentRowOutcome::Unknown,
                ],
                context: policy_binding(),
                core: NodeUuid::from_bytes([8; 16]),
                core_epoch: 1,
                claims_revision: 2,
                policy_epoch: 3,
                settled_through: GlobalTime(4),
                authorization_progress: 5,
                version_carriers: carriers(),
            }),
            SyncMessage::CurrentRowsCancel { request_id },
            SyncMessage::ReadBarrier { writes_through: 0 },
            SyncMessage::ReadBarrier {
                writes_through: u64::MAX,
            },
        ];
        for advice in [
            PermissionAdvice::Allowed,
            PermissionAdvice::Denied,
            PermissionAdvice::Unknown,
        ] {
            messages.push(SyncMessage::AuthorizationScopeDecision { request_id, advice });
        }
        for status in [
            ChunkUploadStatus::Need(Vec::new()),
            ChunkUploadStatus::Need(vec![value_ref.root.clone()]),
            ChunkUploadStatus::Staged,
            ChunkUploadStatus::RateLimited,
            ChunkUploadStatus::Rejected,
        ] {
            messages.push(SyncMessage::ChunkUploadResult(ChunkUploadResult {
                value_ref: value_ref.clone(),
                status,
            }));
        }
        // Variants whose payloads other parts own come from the fixtures.
        messages.extend(fixtures.into_iter().map(|(_, _, message)| message));
        for message in &messages {
            assert_round_trips(message);
        }
    }

    /// Key and carrier types that also travel outside a `SyncMessage`
    /// round-trip on their own.
    #[test]
    fn standalone_types_round_trip() {
        let binding_view = BindingViewKey::new(
            subscription().shape_id,
            subscription().binding_id,
            Default::default(),
        );
        assert_round_trips(&RowVersionRef::new(
            "todos",
            RowUuid::from_bytes([1; 16]),
            tx_id(u64::MAX),
        ));
        assert_round_trips(&binding_view);
        assert_round_trips(&AuthorityResultKey::unscoped(binding_view));
        assert_round_trips(&AuthorityResultKey::policy_scoped(
            binding_view,
            policy_binding(),
        ));
        assert_round_trips(&CoverageKey {
            shape_id: binding_view.shape_id,
            binding_id: binding_view.binding_id,
            opts: Default::default(),
            policy_binding: Some(policy_binding()),
        });
        assert_round_trips(&PolicyBindingKey::from_canonical_parts(
            author(1),
            BTreeMap::new(),
        ));
        for (operation, row) in [
            (AuthorizationScopeOperation::Read, None),
            (AuthorizationScopeOperation::Insert, None),
            (
                AuthorizationScopeOperation::Update,
                Some(RowUuid::from_bytes([1; 16])),
            ),
            (AuthorizationScopeOperation::Delete, None),
        ] {
            assert_round_trips(&AuthorizationOperationKey {
                operation,
                table: "todos".to_owned(),
                row,
                candidate_digest: [3; 32],
            });
        }
        assert_round_trips(&RowBase::default());
        assert_round_trips(&PeerPayloadInventory::default());
        assert_round_trips(&bundle(VersionBundleScope::ViewScoped, Fate::Accepted));
        // Outside a message scope every row defines its descriptor inline.
        let records = versions();
        assert_eq!(
            decode_exact::<Vec<VersionRecord>>(&encode(&records)).unwrap(),
            records
        );
    }

    /// Retired tags and tags past the last variant fail as an invalid tag;
    /// none of them panics on the uninhabited reserved payload.
    #[test]
    fn retired_and_unknown_sync_message_tags_are_invalid() {
        for tag in [14u8, 15, 29, 31, 127] {
            for payload in [vec![tag], vec![tag, 0]] {
                assert_eq!(
                    decode_exact::<SyncMessage>(&payload).unwrap_err(),
                    CodecError::InvalidTypeTag {
                        tag,
                        context: "SyncMessage"
                    }
                );
            }
        }
    }

    /// A chunk request batch is accepted up to its cardinality limit and a
    /// larger declared count is rejected before any entry is read.
    #[test]
    fn chunk_request_batch_keeps_its_cardinality_limit() {
        let entry = ChunkRequestEntry {
            request_id: 1,
            locator: groove::large_values::Locator::random(),
            expected_hash: [2; 32],
            remaining_hops: 3,
        };
        assert_round_trips(&ChunkRequestBatch {
            requests: vec![entry; MAX_CHUNK_REQUEST_BATCH_ENTRIES],
        });

        for count in [MAX_CHUNK_REQUEST_BATCH_ENTRIES + 1, u32::MAX as usize] {
            let mut over = Vec::new();
            crate::codec::put_len(&mut over, count);
            assert_eq!(
                decode_exact::<ChunkRequestBatch>(&over).unwrap_err(),
                CodecError::Invalid {
                    context: "chunk request batch exceeds cardinality limit"
                }
            );
        }
    }
}
