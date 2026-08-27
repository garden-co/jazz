use jazz_testkit as support;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;

use jazz::db::block_on;
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::protocol::{
    CurrentWriteSchema, LensOp, MigrationLens, SchemaLineagePublication, SchemaVersion,
    SyncMessage, TableLens,
};
use jazz::row_input;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::SchemaHash;
use jazz::tools::schema_lens::{Lens, LensTransform};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate, RejectionReason};
use jazz_server::JazzServer;
use support::{publish_allow_all_permissions, push_catalogue_in_memory, wait_for_edge_query_ready};

fn author(byte: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([byte; 16])
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn v1_schema() -> JazzSchema {
    compile_public_schema(
        SchemaBuilder::new().table(
            TableSchemaBuilder::new("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text)
                .column("owner", ColumnType::Text)
                .policies(owner_write_policies(PolicyExpr::True)),
        ),
    )
}

fn v2_schema() -> JazzSchema {
    let owner = owner_policy();
    compile_public_schema(
        SchemaBuilder::new().table(
            TableSchemaBuilder::new("people")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text)
                .column("owner", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::False)
                        .with_update(Some(owner.clone()), owner.clone())
                        .with_delete(owner),
                ),
        ),
    )
}

fn compile_public_schema(builder: SchemaBuilder) -> JazzSchema {
    jazz::schema::JazzSchema::new(&builder.build()).expect("test public schema compiles")
}

fn owner_policy() -> PolicyExpr {
    PolicyExpr::eq_session("owner", vec!["user".to_owned()])
}

fn install_claims(node: &mut NodeState<MemoryStorage>, author: AuthorSubject) {
    node.admit_test_session_claims(author, BTreeMap::new());
}

fn owner_write_policies(select: PolicyExpr) -> TablePolicies {
    let owner = owner_policy();
    TablePolicies::new()
        .with_select(select)
        .with_insert(owner.clone())
        .with_update(Some(owner.clone()), owner.clone())
        .with_delete(owner)
}

fn open_node(node_uuid: NodeUuid, schema: JazzSchema) -> NodeState<MemoryStorage> {
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(NodeState::new(node_uuid, schema, MemoryStorage::new(&refs)))
        .expect("open memory node")
}

fn rename_lens(v1: &SchemaVersion, v2: &SchemaVersion) -> MigrationLens {
    MigrationLens::new(
        v1.id,
        v2.id,
        vec![TableLens {
            source_table: "users".to_string(),
            target_table: "people".to_string(),
            ops: vec![LensOp::RenameTable {
                from: "users".to_string(),
                to: "people".to_string(),
            }],
        }],
    )
}

fn cells(id: RowUuid, email: &str, owner: AuthorSubject) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("id".to_string(), Value::Uuid(id.0)),
        ("email".to_string(), Value::String(email.to_string())),
        (
            "owner".to_string(),
            Value::String(owner.canonical().to_owned()),
        ),
    ])
}

fn client_person_values(
    id: jazz::tools::ObjectId,
    email: &str,
) -> HashMap<String, jazz::tools::Value> {
    row_input!("id" => id, "email" => email)
}

fn client_v1_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("users")
                .column("id", jazz::tools::ColumnType::Uuid)
                .column("email", jazz::tools::ColumnType::Text),
        )
        .build()
}

fn client_v2_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("people")
                .column("id", jazz::tools::ColumnType::Uuid)
                .column("email", jazz::tools::ColumnType::Text),
        )
        .build()
}

fn client_rename_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&client_v1_schema()),
        SchemaHash::compute(&client_v2_schema()),
        LensTransform::with_ops(vec![jazz::tools::LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        }]),
    )
}

/// Exercises write authorization for a v2 update whose parent version was
/// stored under the v1 table name.
///
/// ```text
/// writer(v1) --users insert--> authority(v1)
/// authority  --rename lens----> authority(v2)
/// writer(v2) --people update--> authority(v2) --policy over projected v1 parent--> accepted
/// ```
#[test]
fn renamed_table_update_policy_uses_projected_parent_version() {
    let alice = author(0xa1);
    let v1 = SchemaVersion::new(v1_schema());
    let v2 = SchemaVersion::new(v2_schema());
    let lens = rename_lens(&v1, &v2);

    let mut authority = open_node(node(0x90), v1.schema.clone());
    let mut writer_v1 = open_node(node(0x10), v1.schema.clone());
    install_claims(&mut authority, alice);
    install_claims(&mut writer_v1, alice);
    let user_row = row(0x77);

    let (insert_tx, insert_unit) = block_on(async {
        let (published, unit) = writer_v1
            .commit_mergeable_unit(
                MergeableCommit::new("users", user_row, 1_000)
                    .made_by(alice)
                    .cells(cells(user_row, "alice@example.com", alice)),
            )
            .await?;
        let tx_id = writer_v1.persist_and_settle_transaction(published).await?;
        Ok::<_, jazz::node::Error>((tx_id, unit))
    })
    .expect("writer stages v1 insert");
    let SyncMessage::CommitUnit {
        tx: insert_tx_record,
        versions: insert_versions,
    } = insert_unit
    else {
        panic!("expected insert commit unit");
    };
    let insert_updates = block_on(async {
        let outcome = authority
            .ingest_commit_unit(insert_tx_record, insert_versions, 1_000)
            .await?;
        authority.persist_and_settle_outcome(outcome).await
    })
    .expect("authority ingests v1 insert");
    assert!(insert_updates.iter().any(|message| {
        matches!(
            message,
            SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Accepted,
                durability: Some(DurabilityTier::Global),
                ..
            } if *tx_id == insert_tx
        )
    }));

    // Catalogue evolution is a trusted administrative lane, distinct from
    // the untrusted writer whose policy-scoped update we exercise below.
    let catalogue_seq = authority.active_catalogue_seq().saturating_add(1);
    block_on(async {
        let outcome = authority
            .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
                author: AuthorSubject::SYSTEM,
                catalogue_seq,
                publication: Box::new(SchemaLineagePublication::new(
                    v2.clone(),
                    lens.clone(),
                    Vec::<String>::new(),
                    Vec::<String>::new(),
                )),
            })
            .await?;
        authority.persist_and_settle_outcome(outcome).await
    })
    .expect("publish v2 rename lineage");
    block_on(async {
        let outcome = authority
            .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
                author: AuthorSubject::SYSTEM,
                pointer: CurrentWriteSchema {
                    revision: 1,
                    schema: v2.id,
                },
            })
            .await?;
        authority.persist_and_settle_outcome(outcome).await
    })
    .expect("select v2 write schema");

    let mallory = author(0xa2);
    let mut non_owner_writer_v2 = open_node(node(0x11), v2.schema.clone());
    install_claims(&mut authority, mallory);
    install_claims(&mut non_owner_writer_v2, mallory);
    let (_rejected_tx, rejected_unit) = block_on(async {
        let (published, unit) = non_owner_writer_v2
            .commit_mergeable_unit(
                MergeableCommit::new("people", user_row, 2_000)
                    .made_by(mallory)
                    .parents(vec![insert_tx])
                    .cells(cells(user_row, "mallory+renamed@example.com", alice)),
            )
            .await?;
        let tx_id = non_owner_writer_v2
            .persist_and_settle_transaction(published)
            .await?;
        Ok::<_, jazz::node::Error>((tx_id, unit))
    })
    .expect("non-owner stages v2 update");
    let SyncMessage::CommitUnit {
        tx: rejected_tx_record,
        versions: rejected_versions,
    } = rejected_unit
    else {
        panic!("expected rejected update commit unit");
    };
    let rejected_tx = rejected_tx_record.tx_id;
    let rejected_updates = block_on(async {
        let outcome = authority
            .ingest_commit_unit(rejected_tx_record, rejected_versions, 2_000)
            .await?;
        authority.persist_and_settle_outcome(outcome).await
    })
    .expect("authority rejects non-owner v2 update");
    assert!(rejected_updates.iter().any(|message| {
        matches!(
            message,
            SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
                ..
            } if *tx_id == rejected_tx
        )
    }));

    let mut writer_v2 = open_node(node(0x10), v2.schema.clone());
    install_claims(&mut writer_v2, alice);
    let (_update_tx, update_unit) = block_on(async {
        let (published, unit) = writer_v2
            .commit_mergeable_unit(
                MergeableCommit::new("people", user_row, 2_000)
                    .made_by(alice)
                    .parents(vec![insert_tx])
                    .cells(cells(user_row, "alice+renamed@example.com", alice)),
            )
            .await?;
        let tx_id = writer_v2.persist_and_settle_transaction(published).await?;
        Ok::<_, jazz::node::Error>((tx_id, unit))
    })
    .expect("writer stages v2 update");
    let SyncMessage::CommitUnit {
        tx: update_tx_record,
        versions: update_versions,
    } = update_unit
    else {
        panic!("expected update commit unit");
    };

    let update_tx = update_tx_record.tx_id;
    let update_updates = block_on(async {
        let outcome = authority
            .ingest_commit_unit(update_tx_record, update_versions, 2_000)
            .await?;
        authority.persist_and_settle_outcome(outcome).await
    })
    .expect("authority ingests v2 update");

    assert!(update_updates.iter().any(|message| {
        matches!(
            message,
            SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Accepted,
                durability: Some(DurabilityTier::Global),
                ..
            } if *tx_id == update_tx
        )
    }));
}

/// Exercises write authorization for a new row authored after the catalogue
/// evolves from `users` to `people`.
///
/// ```text
/// admin --publish v1 users----> server
/// admin --publish v2 people---> server
/// bob   --insert people-------> server --write policy on v2 table--> accepted
/// ```
#[tokio::test(flavor = "current_thread")]
async fn renamed_table_insert_after_schema_evolution_reaches_edge() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let v1 = client_v1_schema();
            let v2 = client_v2_schema();

            push_catalogue_in_memory(
                server.server_state(),
                server.app_id(),
                "dev",
                std::slice::from_ref(&v1),
                &[],
            )
            .await
            .expect("push initial v1 catalogue");
            publish_allow_all_permissions(
                &server.base_url(),
                server.app_id(),
                server.admin_secret(),
                &v1,
            )
            .await;

            push_catalogue_in_memory(
                server.server_state(),
                server.app_id(),
                "dev",
                &[v1.clone(), v2.clone()],
                &[client_rename_lens()],
            )
            .await
            .expect("push evolved v2 catalogue");
            publish_allow_all_permissions(
                &server.base_url(),
                server.app_id(),
                server.admin_secret(),
                &v2,
            )
            .await;

            let bob = jazz_testkit::connect(
                server.make_client_context_for_user(v2, "bob-sequential-rename-write-auth"),
            )
            .await
            .expect("connect bob");
            wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;

            let user_id = jazz::tools::ObjectId::new();
            let (_, _, transaction_id) = bob
                .insert("people", client_person_values(user_id, "bob@example.com"))
                .expect("bob creates v2 person");
            support::wait_for_edge_txs(
                &bob,
                &[transaction_id.expect("ordinary mutation commits immediately")],
            )
            .await;

            bob.shutdown().await.expect("shutdown bob");
            server.shutdown().await;
        })
        .await;
}
