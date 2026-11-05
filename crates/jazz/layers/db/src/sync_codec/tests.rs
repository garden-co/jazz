//! Codec fixtures need byte-level tests: public row equality cannot pin enum
//! tags, field order, opaque chunk payloads or canonical varint spelling.
use super::*;
use crate::db::sync::{Cell, Image, WireChunk, WireChunkRef, WireError, WireKind};
use jazz_protocol::protocol::{PermissionAdvice, PermissionAdviceAction};
use jazz_types::large_values::Locator;
use std::collections::{BTreeMap, BTreeSet};

fn corpus() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../../fixtures/sqlite_sync_artifact_corpus_v3.json"
    ))
    .unwrap()
}
fn bytes(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}
fn variant(message: &SyncMessage) -> &'static str {
    match message {
        SyncMessage::Subscribe { .. } => "Subscribe",
        SyncMessage::Unsubscribe { .. } => "Unsubscribe",
        SyncMessage::Hold { .. } => "Hold",
        SyncMessage::Fetch { .. } => "Fetch",
        SyncMessage::Commit { .. } => "Commit",
        SyncMessage::ChunkRequests { .. } => "ChunkRequests",
        SyncMessage::PushChunks { .. } => "PushChunks",
        SyncMessage::Chunks { .. } => "Chunks",
        SyncMessage::Rows { .. } => "Rows",
        SyncMessage::Evict { .. } => "Evict",
        SyncMessage::Unlist { .. } => "Unlist",
        SyncMessage::Settled { .. } => "Settled",
        SyncMessage::Answer { .. } => "Answer",
        SyncMessage::Refused { .. } => "Refused",
        SyncMessage::Fate { .. } => "Fate",
        SyncMessage::Advise { .. } => "Advise",
        SyncMessage::Advice { .. } => "Advice",
        SyncMessage::Schema { .. } => "Schema",
        SyncMessage::Origins { .. } => "Origins",
        SyncMessage::ScopedFetchV1 { .. } => "ScopedFetchV1",
        SyncMessage::ContentWitnessesV1 { .. } => "ContentWitnessesV1",
        SyncMessage::AcceptedRowsV1 { .. } => "AcceptedRowsV1",
        SyncMessage::AcceptedFetchV1 { .. } => "AcceptedFetchV1",
        SyncMessage::AcceptedAnswerV1 { .. } => "AcceptedAnswerV1",
        SyncMessage::CatalogueMetadataV1 { .. } => "CatalogueMetadataV1",
        SyncMessage::CatalogueFetchV1 { .. } => "CatalogueFetchV1",
        SyncMessage::CatalogueCoveredV1 { .. } => "CatalogueCoveredV1",
        SyncMessage::FateV1 { .. } => "FateV1",
        SyncMessage::OwnerStoredV1 { .. } => "OwnerStoredV1",
        SyncMessage::OwnerProjectionV1 { .. } => "OwnerProjectionV1",
        SyncMessage::OwnerInputsV1 { .. } => "OwnerInputsV1",
        SyncMessage::OwnerReadyV1 { .. } => "OwnerReadyV1",
        SyncMessage::PolicyDescriptorV1(_) => "PolicyDescriptorV1",
        SyncMessage::OwnerInitializationReserveV1 { .. } => "OwnerInitializationReserveV1",
        SyncMessage::OwnerInitializationReservedV1 { .. } => "OwnerInitializationReservedV1",
        SyncMessage::OwnerInitializationPublishV1 { .. } => "OwnerInitializationPublishV1",
        SyncMessage::ScopedEvidenceAnswerV1 { .. } => "ScopedEvidenceAnswerV1",
        SyncMessage::AcceptedScopedEvidenceAnswerV1 { .. } => "AcceptedScopedEvidenceAnswerV1",
        SyncMessage::AcceptedOverlayCheckV1 { .. } => "AcceptedOverlayCheckV1",
        SyncMessage::AcceptedOverlayCheckedV1 { .. } => "AcceptedOverlayCheckedV1",
        SyncMessage::SubscribeReadablePolicyV1 { .. } => "SubscribeReadablePolicyV1",
        SyncMessage::SchemaAdmission { .. } => "SchemaAdmission",
        SyncMessage::RowsPart { .. } => "RowsPart",
        SyncMessage::UnionAnswerV1 { .. } => "UnionAnswerV1",
        SyncMessage::ScopedUnionSubscribeV1 { .. } => "ScopedUnionSubscribeV1",
        SyncMessage::OriginFateV2 { .. } => "OriginFateV2",
        SyncMessage::Stored { .. } => "Stored",
        SyncMessage::WriterOwnerV1 { .. } => "WriterOwnerV1",
    }
}
fn source(name: &str) -> serde_json::Value {
    serde_json::from_str(match name {
        "sqlite_accepted_overlay_v1.json" => {
            include_str!("../../../../fixtures/sqlite_accepted_overlay_v1.json")
        }
        "sqlite_accepted_scoped_evidence_v1.json" => {
            include_str!("../../../../fixtures/sqlite_accepted_scoped_evidence_v1.json")
        }
        "sqlite_accepted_v1.json" => include_str!("../../../../fixtures/sqlite_accepted_v1.json"),
        "sqlite_catalogue_v1.json" => include_str!("../../../../fixtures/sqlite_catalogue_v1.json"),
        "sqlite_content_origins_v1.json" => {
            include_str!("../../../../fixtures/sqlite_content_origins_v1.json")
        }
        "sqlite_content_witness_v1.json" => {
            include_str!("../../../../fixtures/sqlite_content_witness_v1.json")
        }
        "sqlite_initialization_v1.json" => {
            include_str!("../../../../fixtures/sqlite_initialization_v1.json")
        }
        "sqlite_owner_initialization_v1.json" => {
            include_str!("../../../../fixtures/sqlite_owner_initialization_v1.json")
        }
        "sqlite_owner_inputs_v1.json" => {
            include_str!("../../../../fixtures/sqlite_owner_inputs_v1.json")
        }
        "sqlite_owner_read_content_v1.json" => {
            include_str!("../../../../fixtures/sqlite_owner_read_content_v1.json")
        }
        "sqlite_owner_ready_v1.json" => {
            include_str!("../../../../fixtures/sqlite_owner_ready_v1.json")
        }
        "sqlite_owner_stored_v1.json" => {
            include_str!("../../../../fixtures/sqlite_owner_stored_v1.json")
        }
        "sqlite_policy_descriptor_v1.json" => {
            include_str!("../../../../fixtures/sqlite_policy_descriptor_v1.json")
        }
        "sqlite_read_settlement_v1.json" => {
            include_str!("../../../../fixtures/sqlite_read_settlement_v1.json")
        }
        "sqlite_readable_policy_subscription_v1.json" => {
            include_str!("../../../../fixtures/sqlite_readable_policy_subscription_v1.json")
        }
        "sqlite_readonly_commit_v1.json" => {
            include_str!("../../../../fixtures/sqlite_readonly_commit_v1.json")
        }
        "sqlite_relay_upload_v1.json" => {
            include_str!("../../../../fixtures/sqlite_relay_upload_v1.json")
        }
        "sqlite_scoped_evidence_v1.json" => {
            include_str!("../../../../fixtures/sqlite_scoped_evidence_v1.json")
        }
        "sqlite_scoped_fetch_v1.json" => {
            include_str!("../../../../fixtures/sqlite_scoped_fetch_v1.json")
        }
        other => panic!("unlisted fixture {other}"),
    })
    .unwrap()
}
fn base_messages() -> Vec<(&'static str, SyncMessage)> {
    let locator: Locator = serde_json::from_value(serde_json::json!(vec![17u8; 32])).unwrap();
    let reference = WireChunkRef {
        locator,
        hash: [51; 32],
    };
    let chunk = WireChunk {
        locator,
        hash: [51; 32],
        bytes: vec![0, 1, 254, 255],
    };
    let image = Image {
        table: "notes".into(),
        id: [34; 16],
        key: vec![],
        seq: 128,
        deleted: false,
        content: true,
        cells: vec![
            ("none".into(), Cell::Null),
            ("count".into(), Cell::Int(-7)),
            ("ratio".into(), Cell::Real(1.5)),
            ("text".into(), Cell::Text(b"hi".to_vec())),
            ("bytes".into(), Cell::Blob(vec![0, 255])),
        ],
    };
    let branch = Image {
        table: "notes".into(),
        id: [34; 16],
        key: vec![1, 255],
        seq: 127,
        deleted: true,
        content: false,
        cells: vec![],
    };
    vec![
        ("unsubscribe_128", SyncMessage::Unsubscribe { id: 128 }),
        (
            "hold_branch_row",
            SyncMessage::Hold {
                rows: vec![("notes".into(), [34; 16], vec![1, 255], 128)],
            },
        ),
        (
            "commit_mergeable_empty",
            SyncMessage::Commit {
                node: [17; 16],
                tx: 128,
                kind: WireKind::Mergeable,
                ops: vec![],
                author: None,
                session: None,
                expect: Vec::new(),
            },
        ),
        (
            "chunk_request",
            SyncMessage::ChunkRequests {
                requests: vec![reference.clone()],
            },
        ),
        (
            "push_chunks_64k",
            SyncMessage::PushChunks {
                nodes: vec![WireChunk {
                    bytes: vec![90; 65536],
                    ..chunk.clone()
                }],
            },
        ),
        (
            "chunk_reply_and_missing",
            SyncMessage::Chunks {
                nodes: vec![chunk],
                missing: vec![reference],
            },
        ),
        (
            "mixed_current_and_branch_images",
            SyncMessage::Rows {
                images: vec![image, branch],
            },
        ),
        (
            "evict_branch",
            SyncMessage::Evict {
                table: "notes".into(),
                id: [34; 16],
                key: vec![1, 255],
            },
        ),
        (
            "unlist_branch",
            SyncMessage::Unlist {
                table: "notes".into(),
                id: [34; 16],
                key: vec![1, 255],
            },
        ),
        (
            "global_window",
            SyncMessage::Settled {
                id: 128,
                through: 256,
                window: Some(vec![[34; 16]]),
                global: true,
            },
        ),
        (
            "answer_at_cut",
            SyncMessage::Answer {
                id: 128,
                through: 256,
                rows: vec![[34; 16]],
            },
        ),
        (
            "refused_unsupported_shape",
            SyncMessage::Refused {
                id: 128,
                code: ErrorCode::Query,
                message: "unsupported shape".into(),
            },
        ),
        (
            "refused_missing_table",
            SyncMessage::Refused {
                id: 129,
                code: ErrorCode::Query,
                message: "unknown table".into(),
            },
        ),
        (
            "fate_accepted",
            SyncMessage::Fate {
                tx: 128,
                through: 256,
                result: Ok(()),
                held: Vec::new(),
                global: true,
            },
        ),
        (
            "fate_conflict",
            SyncMessage::Fate {
                tx: 128,
                through: 256,
                result: Err(WireError::Conflict("conflict".into())),
                held: Vec::new(),
                global: true,
            },
        ),
        (
            "advise_insert",
            SyncMessage::Advise {
                author: None,
                session: None,
                id: 128,
                action: PermissionAdviceAction::Insert {
                    table: "notes".into(),
                    cells: BTreeMap::new(),
                },
            },
        ),
        (
            "advice_allowed",
            SyncMessage::Advice {
                id: 128,
                advice: PermissionAdvice::Allowed,
            },
        ),
        (
            "advice_denied",
            SyncMessage::Advice {
                id: 128,
                advice: PermissionAdvice::Denied,
            },
        ),
        (
            "advice_unknown",
            SyncMessage::Advice {
                id: 128,
                advice: PermissionAdvice::Unknown,
            },
        ),
        ("schema_identity", SyncMessage::Schema { version: [68; 16] }),
    ]
}
/// Alice's independently assembled base pins must match typed Rust encoders,
/// including populated branches, all SQL cell arms and a 64KiB opaque chunk.
/// A codec unit test is needed because row queries cannot pin encoded bytes.
#[test]
fn base_message_constructors_match_independent_pins() {
    let fixture = corpus();
    let cases = fixture["base_messages"].as_array().unwrap();
    let messages = base_messages();
    assert_eq!(messages.len(), cases.len());
    for (name, message) in messages {
        let expected = cases.iter().find(|case| case["name"] == name).unwrap();
        assert_eq!(variant(&message), expected["variant"].as_str().unwrap());
        assert_eq!(
            crate::db::encode_message(&message).unwrap(),
            bytes(expected["frame_hex"].as_str().unwrap()),
            "{name}"
        );
    }
}
/// Alice's complete current corpus covers every SQLite variant. Mallory's
/// suffix, truncation and noncanonical tag fail the production exact decoder.
/// A codec unit test is needed to check tags and malformed bytes directly.
#[test]
fn every_variant_has_canonical_complete_input_coverage() {
    let fixture = corpus();
    let mut seen = BTreeSet::new();
    for case in fixture["base_messages"]
        .as_array()
        .unwrap()
        .iter()
        .chain(fixture["existing_messages"].as_array().unwrap())
    {
        let hex = if let Some(hex) = case["frame_hex"].as_str() {
            hex.to_owned()
        } else {
            source(case["source"].as_str().unwrap())
                .pointer(case["pointer"].as_str().unwrap())
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned()
        };
        let frame = bytes(&hex);
        let message = decode_sync_message(&frame).unwrap();
        assert_eq!(variant(&message), case["variant"].as_str().unwrap());
        seen.insert(variant(&message));
        assert_eq!(crate::db::encode_message(&message).unwrap(), frame);
        let mut trailing = frame.clone();
        trailing.push(0);
        let mut overlong = frame[..5].to_vec();
        overlong.extend_from_slice(&[frame[5] | 128, 0]);
        overlong.extend_from_slice(&frame[6..]);
        for invalid in [trailing, frame[..frame.len() - 1].to_vec(), overlong] {
            assert_eq!(
                decode_sync_message(&invalid).unwrap_err().code,
                ErrorCode::Protocol
            );
        }
    }
    let expected = fixture["variants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(seen, expected);
    assert_eq!(seen.len(), 47);
}

/// Alice's old corpus remains byte-for-byte useful as rejected input. A host
/// must refuse those unversioned bodies before interpreting their old fields.
#[test]
fn historical_artifact_corpus_is_refused_without_rewriting_its_sources() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../fixtures/sqlite_sync_artifact_corpus_v1.json"
    ))
    .unwrap();
    for case in fixture["base_messages"]
        .as_array()
        .unwrap()
        .iter()
        .chain(fixture["existing_messages"].as_array().unwrap())
    {
        let hex = case["frame_hex"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| {
                source(case["source"].as_str().unwrap())
                    .pointer(case["pointer"].as_str().unwrap())
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_owned()
            });
        assert_eq!(
            decode_sync_message(&bytes(&hex)).unwrap_err().code,
            ErrorCode::Protocol
        );
    }
}
