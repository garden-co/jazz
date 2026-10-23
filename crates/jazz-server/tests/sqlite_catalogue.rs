//! Real native runtime catalogue transport and permissions-head admission.
use jazz::db::SyncMessage;
use jazz::ids::AuthorSubject;
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz_server::runtime::{
    CommitUnitTrust, ServerLinkAdmission, ServerRuntimeFrameStream, ServerRuntimeHandle,
    ServerSession, StorageConfig,
};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    )
    .unwrap()
}
async fn connect(runtime: &ServerRuntimeHandle) -> (ServerSession, ServerRuntimeFrameStream) {
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
    (session, runtime.open_wire_stream(session).unwrap())
}
fn send(runtime: &ServerRuntimeHandle, session: ServerSession, messages: Vec<SyncMessage>) {
    runtime
        .receive_wire_frames(
            session,
            messages
                .iter()
                .map(|m| postcard::to_allocvec(m).unwrap())
                .collect(),
        )
        .unwrap();
}
async fn batch(stream: &mut ServerRuntimeFrameStream) -> Vec<SyncMessage> {
    tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .iter()
        .map(|f| postcard::from_bytes(f).unwrap())
        .collect()
}
/// Alice receives exactly the authority's authored UUIDs on its native stream,
/// including after a durable server restart; the SQLite port never remints them.
#[tokio::test]
async fn server_exposes_existing_catalogue_and_preserves_it_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let schema = schema();
    let storage = StorageConfig::data_dir(dir.path());
    let mut expected = None;
    for _ in 0..2 {
        let runtime =
            ServerRuntimeHandle::start_with_storage(schema.clone(), storage.clone()).unwrap();
        let snapshot = runtime.trusted_catalogue_snapshot().await.unwrap();
        let identities = snapshot.genesis_physical_identities.clone();
        if let Some(previous) = &expected {
            assert_eq!(previous, &identities);
        } else {
            expected = Some(identities.clone());
        }
        let (session, mut stream) = connect(&runtime).await;
        send(
            &runtime,
            session,
            vec![
                SyncMessage::Schema {
                    version: *schema.version_id().as_bytes(),
                },
                SyncMessage::CatalogueFetchV1 {
                    id: 77,
                    table: "notes".into(),
                    opened: schema.version_id(),
                    scope: None,
                },
            ],
        );
        let mut metadata = None;
        'receive: loop {
            for message in batch(&mut stream).await {
                match message {
                    SyncMessage::CatalogueMetadataV1 { head, schemas } => {
                        assert_eq!(schemas.len(), 1);
                        assert_eq!(schemas[0].identities, identities);
                        metadata = Some(head);
                    }
                    SyncMessage::CatalogueCoveredV1 {
                        id: 77,
                        head,
                        opened,
                    } => {
                        assert_eq!(Some(head), metadata);
                        assert_eq!(opened, schema.version_id());
                        break 'receive;
                    }
                    SyncMessage::Refused { .. } => panic!("native catalogue request refused"),
                    _ => {}
                }
            }
        }
        runtime.close(session);
        runtime.shutdown().await.unwrap();
    }
}
/// Alice's newer read messages must remain behind the same initial permissions
/// gate as ordinary reads: request -> held -> activate policies -> response.
#[tokio::test]
async fn server_holds_scoped_accepted_and_catalogue_reads_until_permissions() {
    let schema = schema();
    let runtime =
        ServerRuntimeHandle::start_awaiting_permissions(schema.clone(), StorageConfig::InMemory)
            .unwrap();
    let (session, mut stream) = connect(&runtime).await;
    let alice = AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
    send(
        &runtime,
        session,
        vec![
            SyncMessage::Schema {
                version: *schema.version_id().as_bytes(),
            },
            SyncMessage::CatalogueFetchV1 {
                id: 1,
                table: "notes".into(),
                opened: schema.version_id(),
                scope: None,
            },
            SyncMessage::AcceptedFetchV1 {
                id: 2,
                query: Query::from("notes"),
                binding: BTreeMap::new(),
                at: None,
                include_deleted: false,
                scope: None,
            },
            SyncMessage::ScopedFetchV1 {
                id: 3,
                query: Query::from("notes"),
                binding: BTreeMap::new(),
                author: alice.canonical().into(),
                claims: BTreeMap::new(),
                include_deleted: false,
            },
        ],
    );
    // This owner command is an ordering barrier after ingress processing.
    runtime.trusted_catalogue_snapshot().await.unwrap();
    while let Ok(Some(frames)) =
        tokio::time::timeout(Duration::from_millis(30), stream.recv()).await
    {
        for frame in frames.unwrap() {
            let message: SyncMessage = postcard::from_bytes(&frame).unwrap();
            assert!(!matches!(
                message,
                SyncMessage::CatalogueCoveredV1 { .. }
                    | SyncMessage::AcceptedAnswerV1 { .. }
                    | SyncMessage::Answer { .. }
                    | SyncMessage::Refused { .. }
                    | SyncMessage::CatalogueMetadataV1 { .. }
            ));
        }
    }
    runtime
        .activate_schema(
            1,
            schema,
            HashMap::from([(
                "notes".into(),
                TablePolicies::new().with_select(PolicyExpr::True),
            )]),
        )
        .await
        .unwrap();
    let mut answered = std::collections::BTreeSet::new();
    while answered.len() < 3 {
        for message in batch(&mut stream).await {
            match message {
                SyncMessage::CatalogueCoveredV1 { id: 1, .. } => {
                    answered.insert(1);
                }
                SyncMessage::AcceptedAnswerV1 { id: 2, .. } => {
                    answered.insert(2);
                }
                // ScopedFetchV1 is only valid on explicitly trusted SYSTEM links.
                SyncMessage::Refused { id: 3, .. } => {
                    answered.insert(3);
                }
                SyncMessage::Refused { .. } => panic!("authorized read refused after activation"),
                _ => {}
            }
        }
    }
    runtime.close(session);
    runtime.shutdown().await.unwrap();
}
