use std::collections::HashMap;

use jazz_tools::batch_fate::{BatchSettlement, VisibleBatchMember};
use jazz_tools::metadata::{MetadataKey, RowProvenance};
use jazz_tools::object::{BranchName, ObjectId};
use jazz_tools::query_manager::types::SchemaHash;
use jazz_tools::row_histories::{BatchId, RowState, StoredRowBatch};
use jazz_tools::sync_manager::{DurabilityTier, RowMetadata, SyncPayload};
use jazz_tools::sync_payload_telemetry::{
    SyncPayloadTelemetryContext, SyncPayloadTelemetryDirection, SyncPayloadTelemetryScope,
    records_for_payload,
};

fn alice_row() -> StoredRowBatch {
    let row_id = ObjectId::from_uuid(uuid::Uuid::from_u128(1));
    StoredRowBatch::new_with_batch_id(
        BatchId::from_uuid(uuid::Uuid::from_u128(2)),
        row_id,
        "main",
        [],
        b"hello".to_vec(),
        RowProvenance::for_insert("alice", 1_775_000_000),
        HashMap::from([
            (MetadataKey::Table.to_string(), "todos".to_string()),
            (
                MetadataKey::OriginSchemaHash.to_string(),
                SchemaHash::from_bytes([7; 32]).to_string(),
            ),
        ]),
        RowState::VisibleDirect,
        Some(DurabilityTier::EdgeServer),
    )
}

fn websocket_context() -> SyncPayloadTelemetryContext {
    SyncPayloadTelemetryContext {
        app_id: Some("telemetry-app".to_string()),
        scope: SyncPayloadTelemetryScope::Websocket,
        direction: SyncPayloadTelemetryDirection::ClientToServer,
        client_id: Some("alice-client".to_string()),
        connection_id: Some("42".to_string()),
        sequence: Some(7),
        source_frame_id: Some("frame-1".to_string()),
        source_payload_index: Some(0),
        source_payload_count: Some(1),
        source_frame_bytes: Some(128),
        message_bytes: Some(64),
        message_encoding: Some("binary".to_string()),
    }
}

#[test]
fn row_batch_record_includes_table_schema_branch_and_batch_metadata() {
    let row = alice_row();
    let payload = SyncPayload::RowBatchCreated {
        metadata: Some(RowMetadata {
            id: row.row_id,
            metadata: HashMap::from([("custom".to_string(), "ignored".to_string())]),
        }),
        row: row.clone(),
    };

    let records = records_for_payload(&websocket_context(), &payload);

    assert_eq!(records.len(), 1);
    let record = &records[0];
    let expected_row_id = row.row_id.to_string();
    let expected_schema_hash = SchemaHash::from_bytes([7; 32]).to_string();
    let expected_batch_id = row.batch_id.to_string();
    assert_eq!(record.app_id.as_deref(), Some("telemetry-app"));
    assert_eq!(record.scope.as_str(), "websocket");
    assert_eq!(record.direction.as_str(), "client_to_server");
    assert_eq!(record.payload_variant.as_deref(), Some("RowBatchCreated"));
    assert_eq!(record.row_id.as_deref(), Some(expected_row_id.as_str()));
    assert_eq!(record.table_name.as_deref(), Some("todos"));
    assert_eq!(
        record.schema_hash.as_deref(),
        Some(expected_schema_hash.as_str())
    );
    assert_eq!(record.branch_name.as_deref(), Some("main"));
    assert_eq!(record.batch_id.as_deref(), Some(expected_batch_id.as_str()));
    assert!(record.log_body.is_none());
}

#[test]
fn rejected_settlements_include_log_body_without_raw_payload_bytes() {
    let batch_id = BatchId::from_uuid(uuid::Uuid::from_u128(3));
    let payload = SyncPayload::BatchSettlement {
        settlement: BatchSettlement::Rejected {
            batch_id,
            code: "policy_denied".to_string(),
            reason: "alice cannot write todos".to_string(),
        },
    };

    let records = records_for_payload(&websocket_context(), &payload);

    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.payload_variant.as_deref(), Some("BatchSettlement"));
    assert_eq!(record.error_variant.as_deref(), Some("Rejected"));
    assert_eq!(record.error_code.as_deref(), Some("policy_denied"));
    let log_body = record
        .log_body
        .as_ref()
        .expect("rejected settlement log body");
    assert!(log_body.get("settlement").is_some());
    assert!(log_body.get("raw").is_none());
    assert!(log_body.get("base64").is_none());
}

#[test]
fn accepted_transaction_emits_one_record_per_member() {
    let first = ObjectId::from_uuid(uuid::Uuid::from_u128(4));
    let second = ObjectId::from_uuid(uuid::Uuid::from_u128(5));
    let payload = SyncPayload::BatchSettlement {
        settlement: BatchSettlement::AcceptedTransaction {
            batch_id: BatchId::from_uuid(uuid::Uuid::from_u128(6)),
            confirmed_tier: DurabilityTier::GlobalServer,
            visible_members: vec![
                VisibleBatchMember {
                    object_id: first,
                    branch_name: BranchName::new("main"),
                    batch_id: BatchId::from_uuid(uuid::Uuid::from_u128(7)),
                },
                VisibleBatchMember {
                    object_id: second,
                    branch_name: BranchName::new("draft"),
                    batch_id: BatchId::from_uuid(uuid::Uuid::from_u128(8)),
                },
            ],
        },
    };

    let records = records_for_payload(&websocket_context(), &payload);

    let first_id = first.to_string();
    let second_id = second.to_string();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].member_index, Some(0));
    assert_eq!(records[0].member_count, Some(2));
    assert_eq!(records[0].row_id.as_deref(), Some(first_id.as_str()));
    assert_eq!(records[1].member_index, Some(1));
    assert_eq!(records[1].member_count, Some(2));
    assert_eq!(records[1].row_id.as_deref(), Some(second_id.as_str()));
}
