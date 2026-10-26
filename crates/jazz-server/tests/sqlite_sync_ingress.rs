//! Malformed semantic frames must close only their own server session.
use jazz::db::SyncMessage;
use jazz::ids::AuthorSubject;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz_server::runtime::{
    CommitUnitTrust, ServerLinkAdmission, ServerRuntimeHandle, StorageConfig,
};
use std::{collections::BTreeMap, time::Duration};

/// Mallory appends bytes or uses an overlong varint in an otherwise valid
/// unsubscribe. The server refuses her session before processing the following
/// catalogue request; Alice's independent connection remains usable.
#[tokio::test]
async fn malformed_sqlite_message_closes_only_its_session() {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    )
    .unwrap();
    let runtime =
        ServerRuntimeHandle::start_with_storage(schema.clone(), StorageConfig::InMemory).unwrap();
    let hello = jazz::db::encode_message(&SyncMessage::Schema {
        version: *schema.version_id().as_bytes(),
    })
    .unwrap();
    let fetch = jazz::db::encode_message(&SyncMessage::CatalogueFetchV1 {
        id: 71,
        table: "notes".into(),
        opened: schema.version_id(),
        scope: None,
    })
    .unwrap();
    for malformed in [
        vec![1, 128, 1, 0],
        vec![129, 0, 128, 1],
        vec![1, 128, 129, 0],
    ]
    .map(|body| {
        let mut frame = b"JSQL\x02".to_vec();
        frame.extend(body);
        frame
    }) {
        let mallory = AuthorSubject::authenticated("https://issuer.example", "mallory").unwrap();
        let session = runtime
            .open_with_session_context(
                mallory,
                BTreeMap::new(),
                CommitUnitTrust::Session,
                0,
                None,
                ServerLinkAdmission::OrdinarySession,
            )
            .await
            .unwrap();
        let mut stream = runtime.open_wire_stream(session).unwrap();
        runtime
            .receive_wire_frames(session, vec![hello.clone(), malformed, fetch.clone()])
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let batch = stream
                    .recv()
                    .await
                    .expect("session reports its terminal error");
                match batch {
                    Err(error) => break error,
                    Ok(frames) => {
                        for frame in frames {
                            assert!(
                                !matches!(
                                    jazz::sync_codec::decode_sync_message(&frame).unwrap(),
                                    SyncMessage::CatalogueCoveredV1 { id: 71, .. }
                                ),
                                "malformed prefix must not admit the following request"
                            );
                        }
                    }
                }
            }
        })
        .await
        .unwrap();
        assert!(result.contains("invalid SQLite sync frame"), "{result}");
        assert!(
            tokio::time::timeout(Duration::from_secs(5), stream.recv())
                .await
                .unwrap()
                .is_none()
        );
    }
    let alice = AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
    let session = runtime
        .open_with_session_context(
            alice,
            BTreeMap::new(),
            CommitUnitTrust::Session,
            0,
            None,
            ServerLinkAdmission::OrdinarySession,
        )
        .await
        .unwrap();
    let mut stream = runtime.open_wire_stream(session).unwrap();
    runtime
        .receive_wire_frames(session, vec![hello, fetch])
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let frames = stream.recv().await.unwrap().unwrap();
            if frames.iter().any(|frame| {
                matches!(
                    jazz::sync_codec::decode_sync_message(frame).unwrap(),
                    SyncMessage::CatalogueCoveredV1 { id: 71, .. }
                )
            }) {
                break;
            }
        }
    })
    .await
    .unwrap();
    runtime.close(session);
    runtime.shutdown().await.unwrap();
}
