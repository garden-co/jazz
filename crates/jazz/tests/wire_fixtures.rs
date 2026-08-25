use std::collections::BTreeMap;

use groove::ivm::{TerminalEdit, TerminalOperation, TerminalPathSegment};
use groove::records::{RecordDescriptor, Value, ValueType};
use jazz::binding_codec::{
    RelationSnapshotPayload, RemovedRowPayload, Row, RowBatch, SubscriptionDeltaPayload,
};
use jazz::ids::{AuthorSubject, MigrationLensId, NodeUuid, RowUuid, SchemaVersionId};
use jazz::protocol::{
    CatalogueAck, CatalogueSnapshot, CurrentWriteSchema, LensOp, MigrationLens,
    PeerPayloadInventory, RegisterShapeOptions, ResultRowEntry, RowVersionRef,
    SchemaLineagePublication, SchemaVersion, ShapeAst, Subscribe, SubscribeRejectReason,
    SubscribeServerFailureCode, SubscriptionKey, SyncMessage, TableLens, VersionBundle,
    VersionCarrier, VersionRecord, build_version_bundle_runs_from_singletons,
};
use jazz::query::{
    ArraySubquery, ArraySubqueryRequirement, BindingId, OrderDirection, Query, ShapeId, col, eq,
    lit,
};
use jazz::schema::JazzSchema;
use jazz::time::{GlobalTime, TxTime};
use jazz::tools::{
    ColumnType as PublicColumnType, ObjectId, ResultKey, SchemaBuilder, TableSchemaBuilder,
};
use jazz::tx::{DurabilityTier, Fate, Transaction, TxId, TxKind};
use jazz::wire::{
    FEATURE_SYNC_MESSAGE_PAYLOAD, WIRE_PROTOCOL_VERSION, WireEnvelope, WireFrame,
    decode_sync_message, encode_frame, encode_sync_message,
};
use serde::{Deserialize, Serialize};

const FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/wire_message_frames.json"
);
const NATIVE_ROW_CODEC_FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/native_row_codec.json"
);
const NATIVE_QUERY_CODEC_FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/native_query_codec.json"
);
const BINDING_CODEC_GOLDEN_FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/binding_codec_golden.json"
);

#[derive(Deserialize, Serialize)]
struct Manifest {
    fixture_set: &'static str,
    codec: &'static str,
    protocol_version: u16,
    features: u64,
    fixtures: Vec<Fixture>,
}

#[derive(Deserialize, Serialize)]
struct Fixture {
    name: &'static str,
    message_family: &'static str,
    frame_hex: String,
    frame_base64: String,
    payload_hex: String,
}

#[derive(Deserialize, Serialize)]
struct NativeRowCodecFixture {
    cases: Vec<NativeRowCodecCase>,
}

#[derive(Deserialize, Serialize)]
struct NativeRowCodecCase {
    name: String,
    descriptor_hex: Vec<String>,
    record_hex: Vec<String>,
    fields: Vec<NativeRowCodecField>,
}

#[derive(Deserialize, Serialize)]
struct NativeRowCodecField {
    name: String,
    encoded_hex: String,
    decoded_hex: Option<String>,
}

/// Rust owns this fixture because the concrete wire values are emitted by the
/// native Rust bindings.  NAPI and WASM intentionally have duplicate adapter
/// structs today; TypeScript consumes these bytes too.  Keep the compact
/// binary cases here until those adapters share one production encoder.
#[derive(Deserialize, Serialize)]
struct BindingCodecGoldenFixture {
    format: String,
    relation_snapshots: Vec<BindingCodecGoldenBinaryCase>,
    subscription_deltas: Vec<BindingCodecGoldenBinaryCase>,
    terminal: BindingCodecGoldenTerminal,
}

#[derive(Deserialize, Serialize)]
struct BindingCodecGoldenBinaryCase {
    name: String,
    payload_hex: String,
}

#[derive(Deserialize, Serialize)]
struct BindingCodecGoldenTerminal {
    events: serde_json::Value,
    rejections: serde_json::Value,
}

#[derive(Deserialize, Serialize)]
struct NativeQueryCodecFixture {
    cases: Vec<NativeQueryCodecCase>,
}

#[derive(Deserialize, Serialize)]
struct NativeQueryCodecCase {
    name: String,
    query_hex: String,
}

fn compiled_todos_schema(columns: &[&str]) -> JazzSchema {
    let table = columns
        .iter()
        .fold(TableSchemaBuilder::new("todos"), |table, column| {
            table.column(column, PublicColumnType::Text)
        });
    let source = SchemaBuilder::new().table(table).build();
    jazz::schema::JazzSchema::new(&source).expect("wire fixture source schema compiles")
}

fn wire_fixture_messages() -> Vec<(&'static str, &'static str, SyncMessage)> {
    let node = NodeUuid::from_bytes([0x11; 16]);
    let tx_id = TxId::new(TxTime(12), node);
    let shape_id = ShapeId(uuid::Uuid::from_bytes([0x22; 16]));
    let binding_id = BindingId(uuid::Uuid::from_bytes([0x33; 16]));
    let schema_version = SchemaVersionId::from_bytes([0x44; 16]);
    let target_schema_version = SchemaVersionId::from_bytes([0x45; 16]);
    let author = AuthorSubject::for_test_bytes([0x55; 16]);
    let row = RowUuid::from_bytes([0x77; 16]);
    let subscription = SubscriptionKey {
        shape_id,
        binding_id,
        read_view: Default::default(),
    };
    let lineage_source = SchemaVersion::new(compiled_todos_schema(&["title"]));
    let lineage_target = SchemaVersion::new(compiled_todos_schema(&["title", "body"]));
    let lineage_target_id = lineage_target.id;
    let lineage_lens = MigrationLens::new(
        lineage_source.id,
        lineage_target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![
                LensOp::CopyColumn {
                    from: "title".to_owned(),
                    to: "title".to_owned(),
                },
                LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::Bytes(Vec::new()),
                },
            ],
        }],
    );
    let lineage_publication = SchemaLineagePublication::new(
        lineage_target.clone(),
        lineage_lens,
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let mut large_value = groove::large_values::prepare(
        groove::large_values::LargeValueKind::Bytes,
        &vec![0x5a; groove::large_values::INLINE_VALUE_MAX_BYTES + 1],
    )
    .expect("large-value wire fixture prepares");
    assert_eq!(
        large_value.staged_chunks.len(),
        1,
        "fixture stays a single leaf so replacing its retrieval capability cannot alter encoded nodes"
    );
    let locator_bytes = large_value.value_ref.root.object_hash.0.to_vec();
    let locator: groove::large_values::Locator = postcard::from_bytes(
        &postcard::to_allocvec(&locator_bytes).expect("encode deterministic fixture locator"),
    )
    .expect("decode deterministic fixture locator through the public wire contract");
    large_value.value_ref.root.locator = locator;
    large_value.staged_chunks[0].node_ref.locator = locator;
    let root_chunk = large_value
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == large_value.value_ref.root)
        .expect("prepared fixture has its root")
        .clone();

    vec![
        (
            "chunk_upload_start_root_descriptor",
            "ChunkUploadStart",
            SyncMessage::ChunkUploadStart(jazz::protocol::ChunkUploadStart {
                value_ref: large_value.value_ref.clone(),
            }),
        ),
        (
            "chunk_upload_nodes_requested_root",
            "ChunkUploadNodes",
            SyncMessage::ChunkUploadNodes(jazz::protocol::ChunkUploadNodes {
                value_ref: large_value.value_ref,
                chunks: vec![root_chunk],
            }),
        ),
        (
            "session_claims_role_editor",
            "SessionClaims",
            SyncMessage::SessionClaims {
                identity: author,
                claims: BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
            },
        ),
        (
            "fate_update_accepted_global",
            "FateUpdate",
            SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(7)),
                durability: Some(DurabilityTier::Global),
            },
        ),
        (
            "register_shape_todos",
            "RegisterShape",
            SyncMessage::RegisterShape {
                shape_id,
                ast: ShapeAst::new(Query::from("todos"), schema_version),
                opts: RegisterShapeOptions::default(),
            },
        ),
        (
            "subscribe_empty_todos_binding",
            "Subscribe",
            SyncMessage::Subscribe(Subscribe {
                shape_id,
                subscription,
                values: Vec::new(),
                known_state: None,
            }),
        ),
        (
            "subscribe_fast_known_state_authorization_progress",
            "Subscribe",
            SyncMessage::Subscribe(Subscribe {
                shape_id,
                subscription,
                values: Vec::new(),
                known_state: Some(
                    jazz::protocol::KnownStateDeclaration::FastWithAuthorizationProgress {
                        completeness: jazz::protocol::KnownStateCompleteness::FastCurrentMembership,
                        position: GlobalTime(7),
                        authorization_progress: 9,
                    },
                ),
            }),
        ),
        (
            "unsubscribe_todos_binding",
            "Unsubscribe",
            SyncMessage::Unsubscribe { subscription },
        ),
        (
            "subscribe_rejected_unsupported_shape",
            "SubscribeRejected",
            SyncMessage::SubscribeRejected {
                subscription,
                reason: SubscribeRejectReason::UnsupportedShapeCapability {
                    detail: "SourceGap::BranchOverlay".to_owned(),
                },
            },
        ),
        (
            "subscribe_rejected_server_table_not_found",
            "SubscribeRejected",
            SyncMessage::SubscribeRejected {
                subscription,
                reason: SubscribeRejectReason::ServerFailure {
                    code: SubscribeServerFailureCode::TableNotFound,
                },
            },
        ),
        (
            "view_update_reset_with_row_add",
            "ViewUpdate",
            SyncMessage::ViewUpdate(jazz::protocol::ViewUpdatePayload {
                subscription,
                settled_through: GlobalTime(7),
                reset_result_set: true,
                version_carriers: Vec::new(),
                version_bundles: Vec::new(),
                peer_payload_inventory: PeerPayloadInventory {
                    complete_tx_payloads: vec![tx_id],
                    authorization_progress: Some(9),
                    opening_pending: false,
                },
                result_member_adds: vec![result_row_entry(tx_id).into()],
                result_member_removes: Vec::new(),
                terminal_operations: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            }),
        ),
        (
            "view_update_mixed_version_carrier_runs",
            "ViewUpdate",
            SyncMessage::ViewUpdate(jazz::protocol::ViewUpdatePayload {
                subscription,
                settled_through: GlobalTime(8),
                reset_result_set: false,
                version_carriers: mixed_version_carriers(schema_version, author),
                version_bundles: Vec::new(),
                peer_payload_inventory: PeerPayloadInventory::default(),
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                terminal_operations: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            }),
        ),
        (
            "view_update_terminal_patch",
            "ViewUpdate",
            SyncMessage::ViewUpdate(jazz::protocol::ViewUpdatePayload {
                subscription,
                settled_through: GlobalTime(9),
                reset_result_set: false,
                version_carriers: Vec::new(),
                version_bundles: Vec::new(),
                peer_payload_inventory: PeerPayloadInventory::default(),
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                terminal_operations: vec![TerminalOperation {
                    root_descriptor: RecordDescriptor::new([("enabled", ValueType::Bool)]),
                    root_key: vec![10; 17],
                    path: vec![TerminalPathSegment::Collection("children".to_owned())],
                    edit: TerminalEdit::Move {
                        key: vec![11; 17],
                        index: 3,
                    },
                }],
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            }),
        ),
        (
            "commit_unit_mergeable_empty",
            "CommitUnit",
            SyncMessage::CommitUnit {
                tx: Transaction {
                    tx_id,
                    kind: TxKind::Mergeable,
                    n_total_writes: 0,
                    made_by: author,
                    permission_subject: None,
                    base_snapshot: None,
                    row_read_set: None,
                    absent_read_set: None,
                    predicate_read_set: None,
                    user_metadata_json: Some("{\"fixture\":\"wire\"}".to_owned()),
                    contribution_merge: None,
                },
                versions: Vec::new(),
            },
        ),
        (
            "publish_schema_todos_body",
            "PublishSchema",
            SyncMessage::PublishSchema {
                author,
                schema: Box::new(SchemaVersion::new(compiled_todos_schema(&[
                    "title", "body",
                ]))),
            },
        ),
        (
            "publish_lens_todos_body_identity",
            "PublishLens",
            SyncMessage::PublishLens {
                author,
                lens: MigrationLens::new(
                    schema_version,
                    target_schema_version,
                    vec![TableLens {
                        source_table: "todos".to_owned(),
                        target_table: "todos".to_owned(),
                        ops: vec![
                            LensOp::CopyColumn {
                                from: "title".to_owned(),
                                to: "title".to_owned(),
                            },
                            LensOp::AddColumn {
                                column: "body".to_owned(),
                                default: Value::Bytes(Vec::new()),
                            },
                        ],
                    }],
                ),
            },
        ),
        (
            "publish_schema_with_lens_todos_body",
            "PublishSchemaWithLens",
            SyncMessage::PublishSchemaWithLens {
                author,
                catalogue_seq: 9,
                publication: Box::new(lineage_publication.clone()),
            },
        ),
        (
            "set_current_write_schema_revision",
            "SetCurrentWriteSchema",
            SyncMessage::SetCurrentWriteSchema {
                author,
                pointer: CurrentWriteSchema {
                    revision: 9,
                    schema: target_schema_version,
                },
            },
        ),
        (
            "catalogue_ack_schema_applied",
            "CatalogueAck",
            SyncMessage::CatalogueAck(CatalogueAck {
                revision: Some(3),
                schema: Some(schema_version),
                lens: Some(MigrationLensId::from_bytes([0x66; 16])),
                applied: true,
            }),
        ),
        (
            "catalogue_snapshot_todos_lineage",
            "CatalogueSnapshot",
            SyncMessage::CatalogueSnapshot(Box::new(CatalogueSnapshot {
                schemas: vec![lineage_source, lineage_target],
                lineages: vec![(9, lineage_publication)],
                current_write_schema: CurrentWriteSchema {
                    revision: 9,
                    schema: lineage_target_id,
                },
            })),
        ),
        (
            "fetch_row_versions_todos",
            "FetchRowVersions",
            SyncMessage::FetchRowVersions {
                requests: vec![RowVersionRef::new("todos", row, tx_id)],
            },
        ),
        (
            "row_version_payloads_empty",
            "RowVersionPayloads",
            SyncMessage::RowVersionPayloads {
                version_bundles: Vec::new(),
            },
        ),
    ]
}

fn result_row_entry(tx_id: TxId) -> ResultRowEntry {
    (
        groove::Intern::new("todos".to_owned()),
        RowUuid::from_bytes([0x77; 16]),
        tx_id,
    )
}

fn mixed_version_carriers(
    schema_version: SchemaVersionId,
    author: AuthorSubject,
) -> Vec<VersionCarrier> {
    let schema = compiled_todos_schema(&["title"]);
    let table = &schema.tables()[0];
    let node = NodeUuid::from_bytes([0x88; 16]);
    let bundles = (0..4)
        .map(|index| {
            let tx_id = TxId::new(TxTime(100 + index), node);
            VersionBundle {
                tx: Transaction {
                    tx_id,
                    kind: TxKind::Mergeable,
                    n_total_writes: 1,
                    made_by: author,
                    permission_subject: None,
                    base_snapshot: None,
                    row_read_set: None,
                    absent_read_set: None,
                    predicate_read_set: None,
                    user_metadata_json: None,
                    contribution_merge: None,
                },
                versions: vec![
                    VersionRecord::from_cells(
                        table,
                        schema_version,
                        RowUuid::from_bytes([0x90 + index as u8; 16]),
                        Vec::new(),
                        author,
                        100 + index,
                        author,
                        100 + index,
                        &BTreeMap::from([("title".to_owned(), format!("run-{index}"))]),
                        None,
                    )
                    .expect("fixture row encodes"),
                ],
                scope: jazz::protocol::VersionBundleScope::CompleteTransaction,
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(100 + index)),
                durability: DurabilityTier::Global,
            }
        })
        .collect::<Vec<_>>();
    vec![
        VersionCarrier::Run(
            build_version_bundle_runs_from_singletons(&bundles[..1])
                .expect("length-one run builds")
                .remove(0),
        ),
        VersionCarrier::Run(
            build_version_bundle_runs_from_singletons(&bundles[1..])
                .expect("multi-row run builds")
                .remove(0),
        ),
    ]
}

fn fixture_manifest() -> Manifest {
    let fixtures = wire_fixture_messages()
        .into_iter()
        .map(|(name, message_family, message)| {
            let payload = encode_sync_message(&message).expect("sync message encodes");
            let frame = WireFrame::Message(WireEnvelope::new(
                WIRE_PROTOCOL_VERSION,
                FEATURE_SYNC_MESSAGE_PAYLOAD,
                payload.clone(),
            ));
            let frame_bytes = encode_frame(&frame).expect("wire frame encodes");
            Fixture {
                name,
                message_family,
                frame_hex: hex(&frame_bytes),
                frame_base64: base64(&frame_bytes),
                payload_hex: hex(&payload),
            }
        })
        .collect();

    Manifest {
        fixture_set: "jazz-wire-message-frames-v14",
        codec: "postcard WireFrame::Message(WireEnvelope { payload: encode_sync_message(..) })",
        protocol_version: WIRE_PROTOCOL_VERSION,
        features: FEATURE_SYNC_MESSAGE_PAYLOAD,
        fixtures,
    }
}

#[test]
fn wire_message_frame_fixtures_are_current() {
    let actual = serde_json::to_string_pretty(&fixture_manifest())
        .expect("fixture manifest serializes")
        + "\n";

    if std::env::var_os("JAZZ_UPDATE_WIRE_FIXTURES").is_some() {
        std::fs::write(FIXTURE_PATH, actual).expect("fixture manifest writes");
        return;
    }

    let expected = include_str!("../fixtures/wire_message_frames.json");
    assert_eq!(
        actual, expected,
        "wire fixtures changed; review compatibility and run \
         `JAZZ_UPDATE_WIRE_FIXTURES=1 cargo test -p jazz --test wire_fixtures \
         wire_message_frame_fixtures_are_current -- --exact` to accept"
    );
}

#[test]
fn wire_message_frame_fixtures_decode_to_expected_messages() {
    let fixture_manifest: Manifest =
        serde_json::from_str(include_str!("../fixtures/wire_message_frames.json"))
            .expect("wire fixture manifest deserializes");

    for (fixture, (name, message_family, expected)) in fixture_manifest
        .fixtures
        .into_iter()
        .zip(wire_fixture_messages())
    {
        assert_eq!(fixture.name, name);
        assert_eq!(fixture.message_family, message_family);
        let frame_bytes = parse_hex(&fixture.frame_hex);
        assert_eq!(base64(&frame_bytes), fixture.frame_base64);
        let WireFrame::Message(envelope) =
            jazz::wire::decode_frame(&frame_bytes).expect("fixture frame decodes")
        else {
            panic!("expected message fixture {}", fixture.name);
        };

        assert_eq!(envelope.protocol_version, WIRE_PROTOCOL_VERSION);
        assert_eq!(envelope.features, FEATURE_SYNC_MESSAGE_PAYLOAD);
        assert_eq!(envelope.session, None);
        assert_eq!(hex(&envelope.payload), fixture.payload_hex);
        let decoded = decode_sync_message(&envelope.payload)
            .unwrap_or_else(|error| panic!("fixture {name} fails to decode: {error}"));
        assert_eq!(decoded, expected);
    }
}

// This is intentionally a codec-level integration fixture: TypeScript creates
// and reads these exact records, while this Rust test independently creates and
// decodes them. That is the narrowest useful cross-language test for a raw
// record-layout contract, which is not observable through the public API.
#[test]
fn native_row_codec_fixture_round_trips_every_groove_value_type() {
    if std::env::var_os("JAZZ_UPDATE_NATIVE_CODEC_FIXTURES").is_some() {
        let (descriptor, values) = exhaustive_native_row_codec_case();
        let descriptor_fields = descriptor
            .fields()
            .iter()
            .map(|field| (field.name.clone(), field.value_type.clone()))
            .collect::<Vec<_>>();
        let descriptor_bytes =
            postcard::to_allocvec(&descriptor_fields).expect("descriptor encodes");
        let record = descriptor.create(&values).expect("record encodes");
        let fields = descriptor
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let span = descriptor
                    .field_span(&record, index)
                    .expect("field span resolves");
                let encoded = &record[span];
                NativeRowCodecField {
                    name: field.name.clone().expect("fixture fields are named"),
                    encoded_hex: hex(encoded),
                    decoded_hex: fixture_decoded_hex(encoded, &field.value_type),
                }
            })
            .collect();
        let fixture = NativeRowCodecFixture {
            cases: vec![NativeRowCodecCase {
                name: "all_value_types_depth_three".to_owned(),
                descriptor_hex: vec![hex(&descriptor_bytes)],
                record_hex: vec![hex(&record)],
                fields,
            }],
        };
        std::fs::write(
            NATIVE_ROW_CODEC_FIXTURE_PATH,
            serde_json::to_string_pretty(&fixture).expect("native row fixture serializes") + "\n",
        )
        .expect("native row fixture writes");
        return;
    }
    let fixture: NativeRowCodecFixture =
        serde_json::from_str(include_str!("../fixtures/native_row_codec.json"))
            .expect("native row codec fixture parses");
    let (descriptor, values) = exhaustive_native_row_codec_case();
    let case = fixture
        .cases
        .iter()
        .find(|case| case.name == "all_value_types_depth_three")
        .expect("all ValueType fixture is present");
    let descriptor_fields = descriptor
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.value_type.clone()))
        .collect::<Vec<_>>();
    let descriptor_bytes = postcard::to_allocvec(&descriptor_fields).expect("descriptor encodes");
    let record = descriptor.create(&values).expect("record encodes");

    assert_eq!(hex(&descriptor_bytes), case.descriptor_hex.concat());
    assert_eq!(hex(&record), case.record_hex.concat());
    assert_eq!(
        descriptor
            .bind(&record)
            .to_values()
            .expect("record decodes"),
        values
    );

    for (index, field) in case.fields.iter().enumerate() {
        assert_eq!(
            descriptor.fields()[index].name.as_deref(),
            Some(field.name.as_str())
        );
        let span = descriptor
            .field_span(&record, index)
            .expect("field span resolves");
        assert_eq!(
            hex(&record[span]),
            field.encoded_hex,
            "{} encoded",
            field.name
        );
        assert_eq!(
            descriptor
                .bind(&record)
                .get_idx(index)
                .expect("field decodes"),
            values[index],
            "{} decoded",
            field.name
        );
    }
}

// This is intentionally a codec-level integration fixture: TypeScript emits
// these exact postcard Query values and Rust independently decodes and emits
// them. Query preparation is the public boundary; a raw fixture is the
// narrowest way to protect its positional layout contract.
#[test]
fn native_query_codec_fixture_round_trips_relation_shapes() {
    if std::env::var_os("JAZZ_UPDATE_NATIVE_CODEC_FIXTURES").is_some() {
        let fixture = NativeQueryCodecFixture {
            cases: native_query_codec_cases()
                .into_iter()
                .map(|(name, query)| NativeQueryCodecCase {
                    name: name.to_owned(),
                    query_hex: hex(&postcard::to_allocvec(&query).expect("query encodes")),
                })
                .collect(),
        };
        std::fs::write(
            NATIVE_QUERY_CODEC_FIXTURE_PATH,
            serde_json::to_string_pretty(&fixture).expect("native query fixture serializes") + "\n",
        )
        .expect("native query fixture writes");
        return;
    }
    let fixture: NativeQueryCodecFixture =
        serde_json::from_str(include_str!("../fixtures/native_query_codec.json"))
            .expect("native query codec fixture parses");

    for (name, expected) in native_query_codec_cases() {
        let case = fixture
            .cases
            .iter()
            .find(|case| case.name == name)
            .unwrap_or_else(|| panic!("{name} fixture is present"));
        assert_eq!(
            hex(&postcard::to_allocvec(&expected).expect("query encodes")),
            case.query_hex,
            "{name} fixture encodes from Rust"
        );
        let bytes = parse_hex(&case.query_hex);
        let decoded: Query = postcard::from_bytes(&bytes).unwrap_or_else(|error| {
            panic!("{name} fixture decodes: {error}");
        });
        assert_eq!(
            decoded, expected,
            "{name} fixture decodes to the expected query"
        );
    }
}

// The native NAPI and WASM bindings currently own duplicate postcard adapter
// structs.  This fixture freezes their common byte contract before the later
// extraction makes them thin adapters.  It is intentionally lower-level than
// a public DB test: the thing under test is the binding representation itself.
#[test]
fn binding_codec_golden_fixture_is_current() {
    let actual = binding_codec_golden_fixture();

    if std::env::var_os("JAZZ_UPDATE_BINDING_CODEC_GOLDENS").is_some() {
        std::fs::write(
            BINDING_CODEC_GOLDEN_FIXTURE_PATH,
            serde_json::to_string_pretty(&actual).expect("binding codec fixture serializes") + "\n",
        )
        .expect("binding codec fixture writes");
        return;
    }

    let expected = include_str!("../fixtures/binding_codec_golden.json");
    // `oxfmt` owns JSON whitespace and deliberately compacts short byte
    // arrays. The fixture contract is its parsed, ordered JSON value; compare
    // that value so the explicit Rust updater and the repository formatter can
    // both be canonical without creating a born-red formatting loop.
    let expected: serde_json::Value =
        serde_json::from_str(expected).expect("binding codec fixture parses");
    assert_eq!(
        serde_json::to_value(actual).expect("binding codec fixture value serializes"),
        expected,
        "binding codec goldens changed; review the NAPI/WASM compatibility contract and run \\
         `JAZZ_UPDATE_BINDING_CODEC_GOLDENS=1 cargo test -p jazz --test wire_fixtures \\
         binding_codec_golden_fixture_is_current -- --exact` to accept"
    );
}

fn binding_codec_golden_fixture() -> BindingCodecGoldenFixture {
    use groove::records::Value;

    let current_descriptor = RecordDescriptor::new([
        ("row_uuid", ValueType::Uuid),
        (
            "user_title",
            ValueType::Nullable(Box::new(ValueType::String)),
        ),
    ]);
    let logical_descriptor =
        RecordDescriptor::new([("row_uuid", ValueType::Uuid), ("title", ValueType::String)]);
    let todo_one_id = RowUuid::from_bytes([0x11; 16]);
    let todo_two_id = RowUuid::from_bytes([0x12; 16]);
    let note_id = RowUuid::from_bytes([0x21; 16]);
    let deleted_todo_id = RowUuid::from_bytes([0x13; 16]);
    let todo_one = current_descriptor
        .create(&[
            Value::Uuid(todo_one_id.0),
            Value::Nullable(Some(Box::new(Value::String("first".to_owned())))),
        ])
        .expect("golden current row encodes");
    let todo_two = current_descriptor
        .create(&[
            Value::Uuid(todo_two_id.0),
            Value::Nullable(Some(Box::new(Value::String("second".to_owned())))),
        ])
        .expect("golden current row encodes");
    let todo_updated = current_descriptor
        .create(&[
            Value::Uuid(todo_one_id.0),
            Value::Nullable(Some(Box::new(Value::String("updated".to_owned())))),
        ])
        .expect("golden updated row encodes");
    let note = logical_descriptor
        .create(&[Value::Uuid(note_id.0), Value::String("note".to_owned())])
        .expect("golden logical row encodes");
    let deleted_todo = current_descriptor
        .create(&[Value::Uuid(deleted_todo_id.0), Value::Nullable(None)])
        .expect("golden deleted row encodes");

    let empty_snapshot = RelationSnapshotPayload {
        root_count: 0,
        rows: Vec::new(),
    };
    let batching_snapshot = RelationSnapshotPayload {
        root_count: 4,
        rows: vec![
            RowBatch {
                table: "todos",
                descriptor: current_descriptor,
                rows: vec![
                    Row {
                        row_id: todo_one_id,
                        deleted: false,
                        raw: &todo_one,
                    },
                    Row {
                        row_id: todo_two_id,
                        deleted: false,
                        raw: &todo_two,
                    },
                ],
            },
            RowBatch {
                table: "notes",
                descriptor: logical_descriptor,
                rows: vec![Row {
                    row_id: note_id,
                    deleted: false,
                    raw: &note,
                }],
            },
            // Batching is contiguous only: returning to `todos` after `notes`
            // must create a new batch, even though its descriptor is identical.
            RowBatch {
                table: "todos",
                descriptor: current_descriptor,
                rows: vec![Row {
                    row_id: deleted_todo_id,
                    deleted: true,
                    raw: &deleted_todo,
                }],
            },
        ],
    };
    let v1 = ResultKey::from(ObjectId::from_uuid(uuid::Uuid::from_bytes([0xa1; 16])));
    let v2 = ResultKey::from_union_occurrence(
        ObjectId::from_uuid(uuid::Uuid::from_bytes([0xb1; 16])),
        [ObjectId::from_uuid(uuid::Uuid::from_bytes([0xb2; 16]))],
        [(0, "matched-arm".to_owned())],
    )
    .expect("typed golden occurrence is valid");
    let delta = SubscriptionDeltaPayload {
        added: vec![RowBatch {
            table: "todos",
            descriptor: current_descriptor,
            rows: vec![Row {
                row_id: todo_one_id,
                deleted: false,
                raw: &todo_one,
            }],
        }],
        updated: vec![RowBatch {
            table: "notes",
            descriptor: logical_descriptor,
            rows: vec![Row {
                row_id: note_id,
                deleted: false,
                raw: &note,
            }],
        }],
        removed: vec![RemovedRowPayload {
            table: "todos".to_owned(),
            row_id: deleted_todo_id,
        }],
        added_occurrence_keys: vec![v1],
        updated_occurrence_keys: vec![v2.clone()],
        removed_occurrence_keys: vec![v2],
    };

    let current_layout = jazz::db::TerminalRootLayout {
        id: "current-row-v1".to_owned(),
        root_descriptor: current_descriptor,
        root_key_slot: 0,
        root_key_field_name: "row_uuid".to_owned(),
        public_fields: vec![jazz::db::TerminalRootPublicField {
            name: "title".to_owned(),
            descriptor_field_name: "user_title".to_owned(),
            slot: 1,
            carrier: jazz::db::TerminalRootCarrier::CurrentRow,
        }],
        carrier: jazz::db::TerminalRootCarrier::CurrentRow,
    };
    let logical_layout = jazz::db::TerminalRootLayout {
        id: "logical-v1".to_owned(),
        root_descriptor: logical_descriptor,
        root_key_slot: 0,
        root_key_field_name: "row_uuid".to_owned(),
        public_fields: vec![jazz::db::TerminalRootPublicField {
            name: "title".to_owned(),
            descriptor_field_name: "title".to_owned(),
            slot: 1,
            carrier: jazz::db::TerminalRootCarrier::Logical,
        }],
        carrier: jazz::db::TerminalRootCarrier::Logical,
    };
    let current_key = std::iter::once(10)
        .chain(todo_one_id.0.as_bytes().iter().copied())
        .collect::<Vec<_>>();
    let logical_key = std::iter::once(10)
        .chain(note_id.0.as_bytes().iter().copied())
        .collect::<Vec<_>>();
    let current_insert = TerminalOperation {
        root_descriptor: current_descriptor,
        root_key: current_key.clone(),
        path: Vec::new(),
        edit: TerminalEdit::Insert {
            index: 0,
            key: current_key.clone(),
            value: todo_one.clone(),
        },
    };
    let logical_insert = TerminalOperation {
        root_descriptor: logical_descriptor,
        root_key: logical_key.clone(),
        path: Vec::new(),
        edit: TerminalEdit::Insert {
            index: 0,
            key: logical_key.clone(),
            value: note.clone(),
        },
    };
    let current_update = TerminalOperation {
        root_descriptor: current_descriptor,
        root_key: current_key.clone(),
        path: Vec::new(),
        edit: TerminalEdit::Update {
            key: current_key.clone(),
            value: todo_updated,
        },
    };
    let logical_remove = TerminalOperation {
        root_descriptor: logical_descriptor,
        root_key: logical_key.clone(),
        path: Vec::new(),
        edit: TerminalEdit::Remove {
            key: logical_key.clone(),
        },
    };
    let logical_move = TerminalOperation {
        root_descriptor: logical_descriptor,
        root_key: logical_key.clone(),
        path: Vec::new(),
        edit: TerminalEdit::Move {
            key: logical_key,
            index: 1,
        },
    };
    BindingCodecGoldenFixture {
        format: "jazz-binding-codec-golden-v1".to_owned(),
        relation_snapshots: vec![
            BindingCodecGoldenBinaryCase {
                name: "empty_root_count_zero".to_owned(),
                payload_hex: hex(
                    &postcard::to_allocvec(&empty_snapshot).expect("empty snapshot encodes")
                ),
            },
            BindingCodecGoldenBinaryCase {
                name: "adjacent_and_nonadjacent_batches_with_deleted_row".to_owned(),
                payload_hex: hex(
                    &postcard::to_allocvec(&batching_snapshot).expect("snapshot encodes")
                ),
            },
        ],
        subscription_deltas: vec![BindingCodecGoldenBinaryCase {
            name: "added_updated_removed_with_v1_and_v2_occurrence_keys".to_owned(),
            payload_hex: hex(&postcard::to_allocvec(&delta).expect("subscription delta encodes")),
        }],
        terminal: BindingCodecGoldenTerminal {
            // These are the exact NAPI/WASM event field names.  Publication is
            // represented by a non-empty `terminalLayouts` list, not an
            // invented flag on a layout object.
            events: serde_json::json!([
                {
                    "type": "delta",
                    "terminalLayouts": [jazz::binding_codec::terminal_layout_to_json(&current_layout).expect("current layout encodes")],
                    "terminalOperations": jazz::binding_codec::terminal_operations_to_json(&[current_insert], &current_layout.id).expect("current insert encodes")
                },
                {
                    "type": "delta",
                    "terminalLayouts": [jazz::binding_codec::terminal_layout_to_json(&logical_layout).expect("logical layout encodes")],
                    "terminalOperations": jazz::binding_codec::terminal_operations_to_json(&[logical_insert], &logical_layout.id).expect("logical insert encodes")
                },
                {
                    "type": "delta",
                    "terminalLayouts": [],
                    "terminalOperations": jazz::binding_codec::terminal_operations_to_json(&[current_update], &current_layout.id).expect("current update encodes")
                },
                {
                    "type": "delta",
                    "terminalLayouts": [],
                    "terminalOperations": jazz::binding_codec::terminal_operations_to_json(&[logical_move, logical_remove], &logical_layout.id).expect("logical move/remove encodes")
                }
            ]),
            rejections: serde_json::json!([
                { "type": "UnsupportedShapeCapability", "detail": "terminal layout missing" },
                { "type": "ServerFailure", "code": "TableNotFound" }
            ]),
        },
    }
}

fn fixture_decoded_hex(bytes: &[u8], value_type: &groove::records::ValueType) -> Option<String> {
    match value_type {
        groove::records::ValueType::Nullable(inner) => match bytes.first() {
            Some(0) => None,
            Some(1) => fixture_decoded_hex(&bytes[1..], inner),
            _ => Some(hex(bytes)),
        },
        _ => Some(hex(bytes)),
    }
}

fn native_query_codec_cases() -> Vec<(&'static str, Query)> {
    let forward = Query::from("accounts")
        .select(["label"])
        .order_by("label", OrderDirection::Asc);
    let mut forward = forward;
    forward.array_subqueries.push(
        ArraySubquery::new("entries", "entries", "account_id", "id")
            .select(["label"])
            .order_by("label", OrderDirection::Asc)
            .limit(3)
            .offset(1),
    );

    let mut reverse = Query::from("groups");
    reverse.array_subqueries.push(
        ArraySubquery::new("members", "members", "group_id", "id")
            .filter(eq(col("state"), lit("active")))
            .select(["name"])
            .limit(4)
            .requirement(ArraySubqueryRequirement::AtLeastOne)
            .nested(
                ArraySubquery::new("notes", "notes", "member_id", "id")
                    .select(["body"])
                    .limit(2)
                    .requirement(ArraySubqueryRequirement::MatchCorrelationCardinality),
            ),
    );

    let mut unbounded = Query::from("teams");
    unbounded
        .array_subqueries
        .push(ArraySubquery::new("participants", "participants", "team_id", "id").offset(2));

    vec![
        ("forward_include_projected_optional", forward),
        ("reverse_include_required_nested_projection", reverse),
        ("unbounded_reverse_include_with_offset", unbounded),
    ]
}

fn exhaustive_native_row_codec_case() -> (
    groove::records::RecordDescriptor,
    Vec<groove::records::Value>,
) {
    use groove::records::{OwnedRecord, RecordDescriptor, ScalarEnumSchema, Value, ValueType};

    let mode = ScalarEnumSchema::new("mode", ["low", "high"]).expect("enum schema is valid");
    let child = RecordDescriptor::new([
        ("child_count", ValueType::I32),
        (
            "child_label",
            ValueType::Nullable(Box::new(ValueType::String)),
        ),
    ]);
    let child_record = |count, label: Option<&str>| {
        OwnedRecord::new(
            child
                .create(&[
                    Value::I32(count),
                    Value::Nullable(label.map(|label| Box::new(Value::String(label.to_owned())))),
                ])
                .expect("child record encodes"),
            child,
        )
    };
    let descriptor = RecordDescriptor::new([
        ("u8_value", ValueType::U8),
        ("u16_value", ValueType::U16),
        ("u32_value", ValueType::U32),
        ("u64_value", ValueType::U64),
        ("f64_value", ValueType::F64),
        ("bool_value", ValueType::Bool),
        ("string_value", ValueType::String),
        ("bytes_value", ValueType::Bytes),
        ("uuid_value", ValueType::Uuid),
        ("enum_value", ValueType::EnumTag(mode)),
        (
            "mixed_tuple",
            ValueType::Tuple(vec![
                ValueType::U8,
                ValueType::I64,
                ValueType::Nullable(Box::new(ValueType::Bool)),
                ValueType::I32,
            ]),
        ),
        ("fixed_array", ValueType::Array(Box::new(ValueType::I32))),
        (
            "variable_array",
            ValueType::Array(Box::new(ValueType::String)),
        ),
        (
            "nullable_array_depth_three",
            ValueType::Nullable(Box::new(ValueType::Array(Box::new(ValueType::Nullable(
                Box::new(ValueType::I32),
            ))))),
        ),
        ("inline_record", ValueType::Record(Box::new(child))),
        (
            "record_array",
            ValueType::Array(Box::new(ValueType::Record(Box::new(child)))),
        ),
        (
            "empty_fixed_array",
            ValueType::Array(Box::new(ValueType::U16)),
        ),
        (
            "empty_variable_array",
            ValueType::Array(Box::new(ValueType::String)),
        ),
        (
            "null_fixed_i32",
            ValueType::Nullable(Box::new(ValueType::I32)),
        ),
        ("i64_value", ValueType::I64),
        ("i32_value", ValueType::I32),
        ("i32_min", ValueType::I32),
        ("i32_negative_one", ValueType::I32),
        ("i32_zero", ValueType::I32),
        ("i32_max", ValueType::I32),
        ("i64_min", ValueType::I64),
        ("i64_negative_one", ValueType::I64),
        ("i64_zero", ValueType::I64),
        ("i64_max", ValueType::I64),
        (
            "nullable_negative_i32",
            ValueType::Nullable(Box::new(ValueType::I32)),
        ),
        ("i64_negatives", ValueType::Array(Box::new(ValueType::I64))),
    ]);
    let values = vec![
        Value::U8(0xa1),
        Value::U16(0xb2c3),
        Value::U32(0xd4e5_f607),
        Value::U64(0x1020_3040_5060_7080),
        Value::F64(12.5),
        Value::Bool(false),
        Value::String("synthetic".to_owned()),
        Value::Bytes(vec![0xde, 0xad]),
        Value::Uuid(uuid::Uuid::from_bytes([0x11; 16])),
        Value::EnumTag(1),
        Value::Tuple(vec![
            Value::U8(9),
            Value::I64(-3),
            Value::Nullable(None),
            Value::I32(-17),
        ]),
        Value::Array(vec![Value::I32(-3), Value::I32(5)]),
        Value::Array(vec![
            Value::String("x".to_owned()),
            Value::String("yz".to_owned()),
        ]),
        Value::Nullable(Some(Box::new(Value::Array(vec![
            Value::Nullable(None),
            Value::Nullable(Some(Box::new(Value::I32(-7)))),
        ])))),
        Value::Record(child_record(4, Some("one"))),
        Value::Array(vec![
            Value::Record(child_record(5, Some("two"))),
            Value::Record(child_record(6, None)),
        ]),
        Value::Array(Vec::new()),
        Value::Array(Vec::new()),
        Value::Nullable(None),
        Value::I64(-42),
        Value::I32(-13),
        Value::I32(i32::MIN),
        Value::I32(-1),
        Value::I32(0),
        Value::I32(i32::MAX),
        Value::I64(i64::MIN),
        Value::I64(-1),
        Value::I64(0),
        Value::I64(i64::MAX),
        Value::Nullable(Some(Box::new(Value::I32(-42)))),
        Value::Array(vec![
            Value::I64(i64::MIN),
            Value::I64(-1),
            Value::I64(0),
            Value::I64(i64::MAX),
        ]),
    ];
    (descriptor, values)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

fn parse_hex(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks_exact(2)
        .map(|chunk| {
            let high = hex_digit(chunk[0]);
            let low = hex_digit(chunk[1]);
            (high << 4) | low
        })
        .collect()
}

fn hex_digit(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => panic!("invalid hex digit {byte}"),
    }
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);

    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        let n = ((b0 as u32) << 16) | ((b1 as u32) << 8) | b2 as u32;

        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }

    out
}
