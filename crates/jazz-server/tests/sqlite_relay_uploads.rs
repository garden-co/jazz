//! Native admission failures must preserve origin-qualified relay receipts.
use jazz::db::{SyncMessage, WireError};
use jazz::ids::AuthorSubject;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz_server::runtime::{
    CommitUnitTrust, ServerLinkAdmission, ServerRuntimeHandle, StorageConfig,
};
use std::{collections::BTreeMap, time::Duration};

/// Alice's two nodes use the same transaction number while Bob's authority is
/// awaiting policies. Both native rejection frames retain the original node;
/// a relay must never guess which pending writer the rejection belongs to.
#[tokio::test]
async fn permissions_gate_rejects_each_original_writer_explicitly() {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("notes").column("text", ColumnType::Text))
            .build(),
    )
    .unwrap();
    let runtime =
        ServerRuntimeHandle::start_awaiting_permissions(schema, StorageConfig::InMemory).unwrap();
    let session = runtime
        .open_with_session_context(
            AuthorSubject::authenticated("https://issuer.example", "alice").unwrap(),
            BTreeMap::new(),
            CommitUnitTrust::Session,
            0,
            None,
            ServerLinkAdmission::OrdinarySession,
        )
        .await
        .unwrap();
    let mut stream = runtime.open_wire_stream(session).unwrap();
    // Frozen public Commit bytes avoid depending on private wire-kind types.
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../jazz/fixtures/sqlite-sync-v2/relay-upload.json"
    ))
    .unwrap();
    let bytes = hex::decode(fixture["request"].as_str().unwrap()).unwrap();
    assert_eq!(&bytes[..4], b"JRU\x02");
    // The pinned journal has its own envelope; send its canonical Commit body
    // through the current semantic envelope and public decoder.
    let mut frame = b"JSQL\x02".to_vec();
    frame.extend_from_slice(&bytes[4..]);
    let request = jazz::sync_codec::decode_sync_message(&frame).unwrap();
    let mut other = request.clone();
    if let SyncMessage::Commit { node, .. } = &mut other {
        *node = [18; 16];
    }
    runtime
        .receive_wire_frames(
            session,
            [request, other]
                .iter()
                .map(|m| jazz::db::encode_message(m).unwrap())
                .collect(),
        )
        .unwrap();
    let mut nodes = Vec::new();
    tokio::time::timeout(Duration::from_secs(5),async {
        while nodes.len()<2 {
            let frames=stream.recv().await.unwrap().unwrap();
            for frame in frames {
                if let SyncMessage::OriginFateV2 {node,tx,through,result,..}=jazz::sync_codec::decode_sync_message(&frame).unwrap() {
                    assert_eq!(tx,127);assert_eq!(through,0);
                    assert!(matches!(result,Err(WireError::Rejected { ref message, .. }) if message.contains("permissions_head_missing")));
                    nodes.push(node);
                }
            }
        }
    }).await.unwrap();
    nodes.sort();
    assert_eq!(nodes, [[17; 16], [18; 16]]);
    runtime.close(session);
    runtime.shutdown().await.unwrap();
}
