//! Policy-schema UUID resolution through the actual native runtime stream.
use jazz::db::SyncMessage;
use jazz::ids::AuthorSubject;
use jazz::protocol::{
    CurrentWriteSchema, MigrationLens, SchemaLineagePublication, SchemaVersion, TableLens,
};
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz_server::runtime::{
    CommitUnitTrust, ServerLinkAdmission, ServerRuntimeFrameStream, ServerRuntimeHandle,
    StorageConfig,
};
use std::{collections::BTreeMap, time::Duration};
fn schema(table: &str) -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(table)
                    .column("scopeId", ColumnType::Uuid)
                    .policies(TablePolicies::new().with_select(PolicyExpr::eq_session(
                        "scopeId",
                        vec!["__jazz_table_identity_v1".into(), table.into()],
                    ))),
            )
            .build(),
    )
    .unwrap()
}
async fn batch(stream: &mut ServerRuntimeFrameStream) -> Vec<SyncMessage> {
    tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .iter()
        .map(|frame| jazz::sync_codec::decode_sync_message(frame).unwrap())
        .collect()
}
/// Alice's live old-schema subscription must survive a complete deployment whose
/// policies use renamed scope names; manifests must be available during activation.
#[tokio::test]
async fn deployment_installs_policy_manifests_before_resetting_live_subscriptions() {
    let source = schema("documents");
    let target = schema("tasks");
    let runtime =
        ServerRuntimeHandle::start_with_storage(source.clone(), StorageConfig::InMemory).unwrap();
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
    let send = |messages: Vec<SyncMessage>| {
        runtime
            .receive_wire_frames(
                session,
                messages
                    .iter()
                    .map(|m| jazz::db::encode_message(m).unwrap())
                    .collect(),
            )
            .unwrap()
    };
    send(vec![
        SyncMessage::Schema {
            version: *source.version_id().as_bytes(),
        },
        SyncMessage::Subscribe {
            tier: jazz::db::ReadTier::Remote,
            wait: None,
            id: 1,
            query: Query::from("documents"),
            binding: BTreeMap::new(),
            since: None,
            include_deleted: false,
        },
    ]);
    'initial: loop {
        for message in batch(&mut stream).await {
            match message {
                SyncMessage::Settled { id: 1, .. } => break 'initial,
                SyncMessage::Refused { message, .. } => panic!("initial policy refused: {message}"),
                _ => {}
            }
        }
    }
    let mut snapshot = runtime.trusted_catalogue_snapshot().await.unwrap();
    let lens = MigrationLens::new(
        source.version_id(),
        target.version_id(),
        vec![TableLens {
            source_table: "documents".into(),
            target_table: "tasks".into(),
            ops: vec![],
        }],
    )
    .unwrap();
    let publication = SchemaLineagePublication::author_from_prior(
        &source,
        &snapshot.genesis_physical_identities,
        SchemaVersion::new(target.clone()),
        lens,
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    snapshot.schemas.push(SchemaVersion::new(target.clone()));
    snapshot.lineages.push((1, publication));
    snapshot.current_write_schema = CurrentWriteSchema {
        revision: 1,
        schema: target.version_id(),
    };
    runtime.apply_deployment_snapshot(snapshot).await.unwrap();
    send(vec![SyncMessage::CatalogueFetchV1 {
        id: 2,
        table: "documents".into(),
        opened: source.version_id(),
        scope: None,
    }]);
    'updated: loop {
        for message in batch(&mut stream).await {
            match message {
                SyncMessage::Refused { message, .. } => {
                    panic!("live policy refused during valid deployment: {message}")
                }
                SyncMessage::CatalogueCoveredV1 { id: 2, .. } => break 'updated,
                _ => {}
            }
        }
    }
    runtime.close(session);
    runtime.shutdown().await.unwrap();
}
