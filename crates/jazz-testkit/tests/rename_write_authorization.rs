use jazz_testkit as support;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;

use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::protocol::{
    CurrentWriteSchema, LensOp, MigrationLens, SchemaLineagePublication, SchemaVersion,
    SyncMessage, TableLens,
};
use jazz::query::{claim, col, eq};
use jazz::row_input;
use jazz::schema::{JazzSchema, Policy, TableSchema, WritePolicies};
use jazz::tools::SchemaBuilder;
use jazz::tools::public_schema::SchemaHash;
use jazz::tools::schema_lens::{Lens, LensTransform};
use jazz::tx::{DurabilityTier, Fate, RejectionReason};
use jazz_server::JazzServer;
use support::{publish_allow_all_permissions, push_catalogue_in_memory, wait_for_edge_query_ready};

fn author(byte: u8) -> AuthorId {
    AuthorId::from_bytes([byte; 16])
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn v1_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "users",
        [
            ColumnSchema::new("id", ColumnType::Uuid),
            ColumnSchema::new("email", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policies(WritePolicies {
        insert_check: Some(QueryPolicy::owner("users")),
        update_using: Some(QueryPolicy::owner("users")),
        update_check: Some(QueryPolicy::owner("users")),
        delete_using: Some(QueryPolicy::owner("users")),
    })])
}

fn v2_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "people",
        [
            ColumnSchema::new("id", ColumnType::Uuid),
            ColumnSchema::new("email", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policies(WritePolicies {
        insert_check: Some(
            jazz::query::Query::from("people").filter(jazz::query::Predicate::Not(Box::new(
                jazz::query::Predicate::Eq(
                    jazz::query::Operand::Claim("sub".to_string()),
                    jazz::query::Operand::Claim("sub".to_string()),
                ),
            ))),
        ),
        update_using: Some(QueryPolicy::owner("people")),
        update_check: Some(QueryPolicy::owner("people")),
        delete_using: Some(QueryPolicy::owner("people")),
    })])
}

struct QueryPolicy;

impl QueryPolicy {
    fn owner(table: &str) -> jazz::query::Query {
        jazz::query::Query::from(table).filter(eq(col("owner"), claim("sub")))
    }
}

fn open_node(node_uuid: NodeUuid, schema: JazzSchema) -> NodeState<MemoryStorage> {
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    NodeState::new(node_uuid, schema, MemoryStorage::new(&refs)).expect("open memory node")
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

fn cells(id: RowUuid, email: &str, owner: AuthorId) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("id".to_string(), Value::Uuid(id.0)),
        ("email".to_string(), Value::String(email.to_string())),
        ("owner".to_string(), Value::Uuid(owner.0)),
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
            jazz::tools::TableSchema::builder("users")
                .column("id", jazz::tools::ColumnType::Uuid)
                .column("email", jazz::tools::ColumnType::Text),
        )
        .build()
}

fn client_v2_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            jazz::tools::TableSchema::builder("people")
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
    let user_row = row(0x77);

    let (insert_tx, insert_unit) = writer_v1
        .commit_mergeable_unit(
            MergeableCommit::new("users", user_row, 1_000)
                .made_by(alice)
                .cells(cells(user_row, "alice@example.com", alice)),
        )
        .expect("writer stages v1 insert");
    let SyncMessage::CommitUnit {
        tx: insert_tx_record,
        versions: insert_versions,
    } = insert_unit
    else {
        panic!("expected insert commit unit");
    };
    let insert_updates = authority
        .ingest_commit_unit(insert_tx_record, insert_versions, 1_000)
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
    authority
        .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq,
            publication: Box::new(SchemaLineagePublication::new(
                v2.clone(),
                lens.clone(),
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .expect("publish v2 rename lineage");
    authority
        .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: v2.id,
            },
        })
        .expect("select v2 write schema");

    let mallory = author(0xa2);
    let mut non_owner_writer_v2 = open_node(node(0x11), v2.schema.clone());
    let (_rejected_tx, rejected_unit) = non_owner_writer_v2
        .commit_mergeable_unit(
            MergeableCommit::new("people", user_row, 2_000)
                .made_by(mallory)
                .parents(vec![insert_tx])
                .cells(cells(user_row, "mallory+renamed@example.com", alice)),
        )
        .expect("non-owner stages v2 update");
    let SyncMessage::CommitUnit {
        tx: rejected_tx_record,
        versions: rejected_versions,
    } = rejected_unit
    else {
        panic!("expected rejected update commit unit");
    };
    let rejected_tx = rejected_tx_record.tx_id;
    let rejected_updates = authority
        .ingest_commit_unit(rejected_tx_record, rejected_versions, 2_000)
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
    let (_update_tx, update_unit) = writer_v2
        .commit_mergeable_unit(
            MergeableCommit::new("people", user_row, 2_000)
                .made_by(alice)
                .parents(vec![insert_tx])
                .cells(cells(user_row, "alice+renamed@example.com", alice)),
        )
        .expect("writer stages v2 update");
    let SyncMessage::CommitUnit {
        tx: update_tx_record,
        versions: update_versions,
    } = update_unit
    else {
        panic!("expected update commit unit");
    };

    let update_tx = update_tx_record.tx_id;
    let update_updates = authority
        .ingest_commit_unit(update_tx_record, update_versions, 2_000)
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
                "main",
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
                "main",
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
            let (_, _, batch_id) = bob
                .insert("people", client_person_values(user_id, "bob@example.com"))
                .expect("bob creates v2 person");
            bob.wait_for_batch(
                batch_id.expect("ordinary mutation commits immediately"),
                jazz::tools::DurabilityTier::EdgeServer,
            )
            .await
            .expect("bob person reaches edge");

            bob.shutdown().await.expect("shutdown bob");
            server.shutdown().await;
        })
        .await;
}
