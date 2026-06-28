use groove::records::Value;
use groove::schema::ColumnType;
use jazz::ids::{AuthorId, MigrationLensId, NodeUuid, RowUuid, SchemaVersionId};
use jazz::node::content_store::Extent;
use jazz::protocol::{
    CatalogueAck, ContentExtent, CurrentWriteSchema, LensOp, MigrationLens, PeerPayloadInventory,
    RegisterShapeOptions, ResultRowEntry, SchemaVersion, ShapeAst, Subscribe, SubscriptionKey,
    SyncMessage, TableLens,
};
use jazz::query::{BindingId, Query, ShapeId};
use jazz::schema::{ColumnSchema, JazzSchema, TableSchema};
use jazz::time::{GlobalSeq, TxTime};
use jazz::tx::{DurabilityTier, Fate, Transaction, TxId, TxKind};
use jazz::wire::{
    FEATURE_SYNC_MESSAGE_PAYLOAD, WIRE_PROTOCOL_VERSION, WireEnvelope, WireFrame,
    decode_sync_message, encode_frame, encode_sync_message,
};
use serde::Serialize;

const FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/wire_message_frames.json"
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
            }),
        ),
        (
            "view_update_reset_with_row_add",
            "ViewUpdate",
            SyncMessage::ViewUpdate {
                subscription,
                reset_result_set: true,
                version_bundles: Vec::new(),
                peer_payload_inventory: PeerPayloadInventory {
                    complete_tx_payloads: vec![tx_id],
                },
                result_row_adds: vec![result_row_entry(tx_id)],
                result_row_removes: Vec::new(),
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
                    source_branch: None,
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
                row,
                extent: content_extent.clone(),
            },
        ),
        (
            "content_extents_body_bytes",
            "ContentExtents",
            SyncMessage::ContentExtents {
                extents: vec![ContentExtent {
                    extent: content_extent,
                    bytes: b"hello world!".to_vec(),
                }],
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
        fixture_set: "jazz-wire-message-frames-v1",
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
