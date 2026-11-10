//! Malformed semantic frames must close only their own server session.
use jazz::db::{ReadTier, SyncMessage};
use jazz::ids::AuthorSubject;
use jazz::protocol::ReadViewSourceSpec;
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz_server::runtime::{
    CommitUnitTrust, ServerLinkAdmission, ServerRuntimeHandle, StorageConfig,
};
use std::{collections::BTreeMap, time::Duration};

/// Mallory appends bytes or uses an overlong varint in an otherwise valid
/// unsubscribe. The server refuses her session before processing the following
/// ordinary read; Alice's independent connection remains usable.
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
    // Publish this exact fixture's existing permissions through the real
    // catalogue API, then pin both ordinary sessions to its returned ID.
    let write_schema = runtime.catalogue().await.unwrap().genesis().unwrap().hash;
    runtime
        .apply_catalogue(jazz::catalogue::CatalogueChange {
            put: jazz::catalogue::CataloguePut {
                bundles: vec![(
                    write_schema,
                    jazz::catalogue::permissions_of(schema.public_schema()),
                    1,
                )],
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    let published = runtime.catalogue().await.unwrap();
    assert_eq!(published.bundles.len(), 1);
    let bundle = published.bundles[0].id;
    let hello = postcard::to_allocvec(&SyncMessage::Join {
        known: Vec::new(),
        version: *schema.version_id().as_bytes(),
    })
    .unwrap();
    let fetch = postcard::to_allocvec(&SyncMessage::Read {
        id: 71,
        query: Box::new(Query::from("notes")),
        binding: BTreeMap::new(),
        span: jazz::db::ReadSpan::Once { at: None },
        since: None,
        include_deleted: false,
        tier: ReadTier::Remote,
        wait: None,
        view: Box::new(ReadViewSourceSpec::Current),
    })
    .unwrap();
    for malformed in [
        vec![1, 128, 1, 0],
        vec![129, 0, 128, 1],
        vec![1, 128, 129, 0],
    ] {
        let mallory = AuthorSubject::authenticated("https://issuer.example", "mallory").unwrap();
        let session = runtime
            .open_with_session_context(
                mallory,
                BTreeMap::new(),
                Some(bundle),
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
                                    jazz::db::decode_message(&frame).unwrap(),
                                    SyncMessage::Settled { id: 71, .. }
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
        assert!(
            result.message.contains("malformed sync frame"),
            "{result:?}"
        );
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
            Some(bundle),
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
                    jazz::db::decode_message(frame).unwrap(),
                    SyncMessage::Settled { id: 71, .. }
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
