//! A client's view of a table's catalogue identity: what generated code
//! writes into a member row's `scopeId` to name a space table (E2EE
//! protocol §1.3), so the app never handles the identity itself.
//!
//! ```text
//! server ──catalogue──► alice ─┐
//!        ──catalogue──► bob ───┴─ table_identity("projects"): one uuid
//! offline client ── table_identity ──✗ NotObserved
//! ```

use std::time::Duration;

use jazz::tools::{ColumnType, JazzClient, ObjectId, Schema, SchemaBuilder, TableSchema};
use jazz_server::JazzServer;
use jazz_testkit::publish_allow_all_permissions;
use uuid::Uuid;

fn schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("projects").column("name", ColumnType::Text))
        .table(TableSchema::builder("boards").column("name", ColumnType::Text))
        .build()
}

/// The identity once the client has the server's catalogue.
async fn observed(client: &JazzClient, table: &str) -> ObjectId {
    for _ in 0..200 {
        match client.table_identity(table) {
            Ok(identity) => return identity,
            Err(error) if error.to_string().contains("NotObserved") => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => panic!("{error}"),
        }
    }
    panic!("the client never observed {table}'s identity");
}

/// Two clients of one server read the same identity for a table, and
/// different ones for different tables.
#[tokio::test]
async fn clients_of_one_server_read_the_same_table_identity() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await.expect("start test server");
            publish_allow_all_permissions(
                &server.base_url(),
                server.app_id(),
                server.admin_secret(),
                &schema(),
            )
            .await;
            let connect = |name: &str| {
                jazz_testkit::connect(server.make_client_context_for_user(
                    schema(),
                    Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_bytes()).to_string(),
                ))
            };
            let alice = connect("table-identity-alice").await.expect("alice");
            let bob = connect("table-identity-bob").await.expect("bob");
            let projects = observed(&alice, "projects").await;
            assert_eq!(observed(&bob, "projects").await, projects);
            assert_ne!(observed(&alice, "boards").await, projects);
            let error = alice
                .table_identity("nope")
                .expect_err("a table the schema lacks");
            assert!(error.to_string().contains("no table nope"), "{error}");
        })
        .await;
}

/// Before a client has seen the authority's catalogue it can't name a
/// table, and says so instead of guessing.
#[tokio::test]
async fn an_offline_client_has_not_observed_the_identity() {
    let client = JazzClient::test_client(schema()).await;
    let error = client
        .table_identity("projects")
        .expect_err("no catalogue yet");
    assert!(error.to_string().contains("NotObserved"), "{error}");
}
