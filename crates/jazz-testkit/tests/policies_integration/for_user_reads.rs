//! Reads a trusted backend makes *for a user* (`JazzClient::for_session`,
//! which jazz-app's `Jazz::for_user` and TS `forSession`/`forUser` sit on).
//!
//! A remote read carries no identity: the core answers it as the backend's
//! trusted link (SYSTEM) and the backend evaluates the user's read policy
//! locally, over the rows the core ships plus "policy support" rows
//! (`Inner::policy_support`). Each test here compares what the backend
//! returns for a user with what the core itself grants that user (an
//! end-user client of the same user, policy evaluated at the core).
//!
//! They assert the *correct* behaviour.

use std::time::Duration;

use jazz::tools::{DurabilityTier, ReadTier};
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_client, wait_for_query};

use super::*;

const WAIT: Duration = Duration::from_secs(10);

fn session(subject: &str, account: u128) -> Session {
    let mut session = Session::new("urn:jazz:test", subject);
    session.account_id = Some(jazz::account_registry::AccountId(uuid::Uuid::from_u128(
        account,
    )));
    session
}

fn bob() -> Session {
    session(BOB_ID, 2)
}

/// A table nobody writes: a fresh backend waits on it so it holds no rows
/// of the tables under test before its first read for a user.
fn probe_table() -> jazz::tools::TableSchemaBuilder {
    TableSchema::builder("probe")
        .column("x", ColumnType::Text)
        .policies(permissions(|p| p.allow_read().always()))
}

/// The ids one remote read returns (the first answer).
async fn remote_ids(client: &JazzClient, table: &str) -> Vec<ObjectId> {
    wait_for_query(
        client,
        Query::from(table),
        ReadTier::Remote,
        WAIT,
        format!("remote read of {table}"),
        |rows| Some(rows.into_iter().map(|(id, _)| id).collect::<Vec<_>>()),
    )
    .await
}

async fn fresh_backend(server: &JazzServer, schema: &Schema, name: &str) -> JazzClient {
    connect_ready_client(server, schema, name, "probe", WAIT).await
}

/// Each handle acting for a user reads with its own session's claims: a
/// live subscription for Bob in org `o1` keeps showing `o1`'s doc while
/// another handle reads for Bob in org `o2`.
#[tokio::test]
async fn for_user_claims_do_not_bleed_between_handles_of_one_user() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("docs")
                        .column("org", ColumnType::Text)
                        .policies(permissions(|p| {
                            p.allow_read()
                                .where_(pe::eq("org", pe::session(vec!["claims", "org"])));
                        })),
                )
                .table(probe_table())
                .build();
            let server = JazzServer::start_with_schema(schema.clone()).await.unwrap();
            let seeder = connect_ready_client(&server, &schema, "seeder", "docs", WAIT).await;
            let d1 = seeder.insert("docs", row_input!("org" => "o1")).unwrap();
            let d2 = seeder.insert("docs", row_input!("org" => "o2")).unwrap();
            for tx in [d1.2, d2.2] {
                seeder
                    .wait_for_transaction(tx.unwrap(), DurabilityTier::GlobalServer)
                    .await
                    .unwrap();
            }
            let (d1, d2) = (d1.0, d2.0);

            let backend = fresh_backend(&server, &schema, "backend").await;
            let in_o1 = backend.for_session(bob().with_claims(serde_json::json!({ "org": "o1" })));
            let in_o2 = backend.for_session(bob().with_claims(serde_json::json!({ "org": "o2" })));

            let mut o1_live = in_o1.subscribe(Query::from("docs")).await.unwrap();
            let mut seen = std::collections::BTreeSet::new();
            let apply = |seen: &mut std::collections::BTreeSet<ObjectId>, item| match item {
                Some(jazz::tools::SubscriptionStreamItem::Delta(delta)) => {
                    for added in delta.added {
                        seen.insert(added.id.row_id().unwrap());
                    }
                    for removed in delta.removed {
                        seen.remove(&removed.id.row_id().unwrap());
                    }
                }
                other => panic!("unexpected subscription item: {other:?}"),
            };
            let first = tokio::time::timeout(WAIT, o1_live.next()).await.unwrap();
            apply(&mut seen, first);
            assert_eq!(seen.iter().copied().collect::<Vec<_>>(), vec![d1]);

            // Another request for Bob, in o2: it reads its own doc.
            assert_eq!(remote_ids(&in_o2, "docs").await, vec![d2]);

            // The o1 subscription must still show o1's doc only.
            while let Ok(item) = tokio::time::timeout(Duration::from_secs(2), o1_live.next()).await
            {
                apply(&mut seen, item);
            }
            assert_eq!(
                seen.iter().copied().collect::<Vec<_>>(),
                vec![d1],
                "a handle acting for Bob in o1 must keep o1's claims"
            );
            server.shutdown().await;
        })
        .await;
}

/// A schema of projects (readable by everyone) and their docs (readable in
/// the session's `org`), with one project and a doc in each of two orgs.
async fn projects_with_docs(
    server: &JazzServer,
    schema: &Schema,
) -> (ObjectId, ObjectId, ObjectId) {
    let seeder = connect_ready_client(server, schema, "seeder", "docs", WAIT).await;
    let project = seeder
        .insert("projects", row_input!("name" => "p"))
        .unwrap();
    seeder
        .wait_for_transaction(project.2.unwrap(), DurabilityTier::GlobalServer)
        .await
        .unwrap();
    let project = project.0;
    let d1 = seeder
        .insert(
            "docs",
            row_input!("project_id" => Value::Uuid(project), "org" => "o1"),
        )
        .unwrap();
    let d2 = seeder
        .insert(
            "docs",
            row_input!("project_id" => Value::Uuid(project), "org" => "o2"),
        )
        .unwrap();
    for tx in [d1.2, d2.2] {
        seeder
            .wait_for_transaction(tx.unwrap(), DurabilityTier::GlobalServer)
            .await
            .unwrap();
    }
    (project, d1.0, d2.0)
}

fn projects_and_docs_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .policies(permissions(|p| p.allow_read().always())),
        )
        .table(
            TableSchema::builder("docs")
                .fk_column("project_id", "projects")
                .column("org", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_read()
                        .where_(pe::eq("org", pe::session(vec!["claims", "org"])));
                })),
        )
        .table(probe_table())
        .build()
}

/// How many related rows the first result nests under `relation`.
fn nested_count(rows: &[jazz::tools::QueryResult], relation: &str) -> usize {
    rows.first()
        .and_then(|row| row.fields.iter().find(|field| field.name == relation))
        .map(|field| match &field.value {
            Value::Array(items) => items.len(),
            other => panic!("{relation} is not nested rows: {other:?}"),
        })
        .unwrap_or_else(|| panic!("no {relation} in {rows:?}"))
}

/// A local one-shot read for a user nests only the related rows that
/// user's read policy grants, as their subscription does.
#[tokio::test]
async fn for_user_local_read_nests_only_related_rows_the_user_may_read() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = projects_and_docs_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await.unwrap();
            let (_, d1, d2) = projects_with_docs(&server, &schema).await;

            let backend = fresh_backend(&server, &schema, "backend").await;
            // The backend holds both docs (it read them as itself).
            let mut held = remote_ids(&backend, "docs").await;
            held.sort();
            let mut both = vec![d1, d2];
            both.sort();
            assert_eq!(held, both);
            let _ = remote_ids(&backend, "projects").await;

            let in_o1 = backend.for_session(bob().with_claims(serde_json::json!({ "org": "o1" })));
            let query = Query::from("projects").array_subquery(jazz::query::ArraySubquery::new(
                "docs",
                "docs",
                "project_id",
                "id",
            ));
            let local = in_o1.query(query, ReadTier::LocalFirst).await.unwrap();
            assert_eq!(nested_count(&local, "docs"), 1, "local one-shot read");

            server.shutdown().await;
        })
        .await;
}

/// A read inside a transaction opened for a user reads by that user's read
/// policy and claims.
#[tokio::test]
async fn for_user_transaction_read_reads_as_the_user() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = projects_and_docs_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await.unwrap();
            let (_, d1, _) = projects_with_docs(&server, &schema).await;

            let backend = fresh_backend(&server, &schema, "backend").await;
            let _ = remote_ids(&backend, "docs").await;

            let in_o1 = backend.for_session(bob().with_claims(serde_json::json!({ "org": "o1" })));
            let tx = in_o1.begin_mergeable_transaction().unwrap();
            let rows = tx
                .client()
                .query(Query::from("docs"), ReadTier::LocalFirst)
                .await
                .unwrap();
            let ids: Vec<_> = rows
                .into_iter()
                .map(|row| row.key.row_id().unwrap())
                .collect();
            assert_eq!(ids, vec![d1]);
            tx.rollback().unwrap();
            server.shutdown().await;
        })
        .await;
}
