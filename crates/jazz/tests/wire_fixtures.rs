use std::collections::BTreeMap;

use groove::records::Value;
use groove::schema::ColumnType;
use jazz::ids::{AuthorId, BranchId, MigrationLensId, NodeUuid, RowUuid, SchemaVersionId};
use jazz::node::content_store::Extent;
use jazz::protocol::{
    BranchMetadata, CatalogueAck, ContentExtent, CurrentWriteSchema, LargeValueOwnerRef, LensOp,
    MigrationLens, PeerPayloadInventory, RegisterShapeOptions, ResultRowEntry, RowVersionRef,
    SchemaVersion, ShapeAst, Subscribe, SubscribeRejectReason, SubscribeServerFailureCode,
    SubscriptionKey, SyncMessage, TableLens, VersionBundle, VersionCarrier, VersionRecord,
    build_version_bundle_runs_from_singletons,
};
use jazz::query::{
    ArraySubquery, ArraySubqueryRequirement, BindingId, OrderDirection, Query, ShapeId, col, eq,
    lit,
};
use jazz::schema::{ColumnSchema, JazzSchema, TableSchema};
use jazz::time::{GlobalSeq, TxTime};
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

#[derive(Serialize)]
struct Manifest {
    fixture_set: &'static str,
    codec: &'static str,
    protocol_version: u16,
    features: u64,
    fixtures: Vec<Fixture>,
}

#[derive(Serialize)]
struct Fixture {
    name: &'static str,
    message_family: &'static str,
    frame_hex: String,
    frame_base64: String,
    payload_hex: String,
    decoded_debug: String,
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

#[derive(Deserialize, Serialize)]
struct NativeQueryCodecFixture {
    cases: Vec<NativeQueryCodecCase>,
}

#[derive(Deserialize, Serialize)]
struct NativeQueryCodecCase {
    name: String,
    query_hex: String,
}

fn wire_fixture_messages() -> Vec<(&'static str, &'static str, SyncMessage)> {
    let node = NodeUuid::from_bytes([0x11; 16]);
    let tx_id = TxId::new(TxTime(12), node);
    let shape_id = ShapeId(uuid::Uuid::from_bytes([0x22; 16]));
    let binding_id = BindingId(uuid::Uuid::from_bytes([0x33; 16]));
    let schema_version = SchemaVersionId::from_bytes([0x44; 16]);
    let target_schema_version = SchemaVersionId::from_bytes([0x45; 16]);
    let author = AuthorId::from_bytes([0x55; 16]);
    let row = RowUuid::from_bytes([0x77; 16]);
    let subscription = SubscriptionKey {
        shape_id,
        binding_id,
        read_view: Default::default(),
    };
    let content_extent = Extent {
        writer: author,
        row,
        column: "body".to_owned(),
        offset: 16,
        len: 12,
    };

    vec![
        (
            "branch_metadata_root_open",
            "BranchMetadata",
            SyncMessage::BranchMetadata(BranchMetadata {
                branch_id: BranchId::from_bytes([0x42; 16]),
                created_by: AuthorId::from_bytes([0x43; 16]),
                parent: None,
                base: None,
                open: true,
            }),
        ),
        (
            "create_branch",
            "CreateBranch",
            SyncMessage::CreateBranch {
                branch_id: BranchId::from_bytes([0x42; 16]),
            },
        ),
        (
            "fetch_branch_metadata",
            "FetchBranchMetadata",
            SyncMessage::FetchBranchMetadata {
                branches: vec![BranchId::from_bytes([0x42; 16])],
            },
        ),
        (
            "fate_update_accepted_global",
            "FateUpdate",
            SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Accepted,
                global_seq: Some(GlobalSeq(7)),
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
                        position: GlobalSeq(7),
                        authorization_progress: 9,
                    },
                ),
            }),
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
            SyncMessage::ViewUpdate {
                subscription,
                settled_through: GlobalSeq(7),
                reset_result_set: true,
                version_carriers: Vec::new(),
                version_bundles: Vec::new(),
                peer_payload_inventory: PeerPayloadInventory {
                    complete_tx_payloads: vec![tx_id],
                    authorization_progress: Some(9),
                },
                result_member_adds: vec![result_row_entry(tx_id).into()],
                result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            },
        ),
        (
            "view_update_chunk_final_with_row_add",
            "ViewUpdateChunk",
            SyncMessage::ViewUpdateChunk {
                subscription,
                settled_through: GlobalSeq(7),
                reset_result_set: true,
                final_chunk: true,
                version_carriers: Vec::new(),
                version_bundles: Vec::new(),
                peer_payload_inventory: PeerPayloadInventory {
                    complete_tx_payloads: vec![tx_id],
                    authorization_progress: Some(9),
                },
                result_member_adds: vec![result_row_entry(tx_id).into()],
                result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            },
        ),
        (
            "view_update_mixed_version_carrier_runs",
            "ViewUpdate",
            SyncMessage::ViewUpdate {
                subscription,
                settled_through: GlobalSeq(8),
                reset_result_set: false,
                version_carriers: mixed_version_carriers(schema_version, author),
                version_bundles: Vec::new(),
                peer_payload_inventory: PeerPayloadInventory::default(),
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            },
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
                    target_lineage: jazz::tx::BranchLineage::Root,
                    branch_merge: None,
                    merge_strategy: None,
                },
                versions: Vec::new(),
            },
        ),
        (
            "commit_unit_branch_target_empty",
            "CommitUnit",
            SyncMessage::CommitUnit {
                tx: Transaction {
                    tx_id: TxId::new(TxTime(43), node),
                    kind: TxKind::Mergeable,
                    n_total_writes: 0,
                    made_by: author,
                    permission_subject: None,
                    base_snapshot: None,
                    row_read_set: None,
                    absent_read_set: None,
                    predicate_read_set: None,
                    user_metadata_json: None,
                    target_lineage: jazz::tx::BranchLineage::Branch(BranchId::from_bytes(
                        [0x42; 16],
                    )),
                    branch_merge: None,
                    merge_strategy: None,
                },
                versions: Vec::new(),
            },
        ),
        (
            "publish_schema_todos_body",
            "PublishSchema",
            SyncMessage::PublishSchema {
                author,
                schema: Box::new(SchemaVersion::new(JazzSchema::new([TableSchema::new(
                    "todos",
                    [
                        ColumnSchema::new("title", ColumnType::String),
                        ColumnSchema::text("body"),
                    ],
                )]))),
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
            "fetch_content_extent_body",
            "FetchContentExtent",
            SyncMessage::FetchContentExtent {
                owner: LargeValueOwnerRef::current_row(row),
                extent: content_extent.clone(),
            },
        ),
        (
            "content_extents_body_bytes",
            "ContentExtents",
            SyncMessage::ContentExtents {
                extents: vec![ContentExtent {
                    owner: LargeValueOwnerRef::current_row(row),
                    extent: content_extent,
                    bytes: b"hello world!".to_vec(),
                }],
            },
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
    author: AuthorId,
) -> Vec<VersionCarrier> {
    let table = TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]);
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
                    target_lineage: jazz::tx::BranchLineage::Root,
                    branch_merge: None,
                    merge_strategy: None,
                },
                versions: vec![
                    VersionRecord::from_cells(
                        &table,
                        schema_version,
                        RowUuid::from_bytes([0x90 + index as u8; 16]),
                        Vec::new(),
                        author,
                        TxTime(100 + index),
                        author,
                        TxTime(100 + index),
                        &BTreeMap::from([("title".to_owned(), format!("run-{index}"))]),
                        None,
                    )
                    .expect("fixture row encodes"),
                ],
                fate: Fate::Accepted,
                global_seq: Some(GlobalSeq(100 + index)),
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
            let decoded = decode_sync_message(&payload).expect("fixture payload decodes");

            Fixture {
                name,
                message_family,
                frame_hex: hex(&frame_bytes),
                frame_base64: base64(&frame_bytes),
                payload_hex: hex(&payload),
                decoded_debug: format!("{decoded:?}"),
            }
        })
        .collect();

    Manifest {
        fixture_set: "jazz-wire-message-frames-v4",
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
         `JAZZ_UPDATE_WIRE_FIXTURES=1 cargo test -p jazz --test wire_fixtures` to accept"
    );
}

#[test]
fn wire_message_frame_fixtures_decode_to_expected_messages() {
    for (fixture, (_, _, expected)) in fixture_manifest()
        .fixtures
        .into_iter()
        .zip(wire_fixture_messages())
    {
        let frame_bytes = parse_hex(&fixture.frame_hex);
        let WireFrame::Message(envelope) =
            jazz::wire::decode_frame(&frame_bytes).expect("fixture frame decodes")
        else {
            panic!("expected message fixture {}", fixture.name);
        };

        assert_eq!(envelope.protocol_version, WIRE_PROTOCOL_VERSION);
        assert_eq!(envelope.features, FEATURE_SYNC_MESSAGE_PAYLOAD);
        assert_eq!(envelope.session, None);
        assert_eq!(decode_sync_message(&envelope.payload).unwrap(), expected);
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
    unbounded.array_subqueries.push(
        ArraySubquery::new("participants", "participants", "team_id", "id")
            .unbounded()
            .offset(2),
    );

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
    use groove::records::{EnumSchema, OwnedRecord, RecordDescriptor, Value, ValueType};

    let mode = EnumSchema::new("mode", ["low", "high"]).expect("enum schema is valid");
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
        ("enum_value", ValueType::Enum(mode)),
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
        Value::Enum(1),
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
