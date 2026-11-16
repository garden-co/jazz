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
//! They assert the *correct* behaviour. All but
//! `for_user_read_after_membership_revoked_does_not_leak` fail on main
//! (f5a1990b), each documenting a gap; that one passes and guards the
//! common correlated-membership case.

use std::time::Duration;

use jazz::tools::{DurabilityTier, ReadTier};
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_client, connect_ready_user, wait_for_query};

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

/// The ids the core grants `user` (an end-user client: its policy is
/// evaluated at the core), once they equal `expected`.
async fn core_grants(client: &JazzClient, table: &str, expected: &[ObjectId]) {
    let mut expected = expected.to_vec();
    expected.sort();
    wait_for_query(
        client,
        Query::from(table),
        ReadTier::Remote,
        WAIT,
        format!("the core's answer for {table}"),
        |rows| {
            let mut ids: Vec<_> = rows.into_iter().map(|(id, _)| id).collect();
            ids.sort();
            (ids == expected).then_some(())
        },
    )
    .await
}

async fn fresh_backend(server: &JazzServer, schema: &Schema, name: &str) -> JazzClient {
    connect_ready_client(server, schema, name, "probe", WAIT).await
}

/// Gap 1a (wrong deny): "docs are readable by admins", with an
/// uncorrelated `EXISTS admins WHERE user_id = @session.sub`. The core
/// grants Bob the doc. `exists_support` ships no support for an
/// uncorrelated `EXISTS`, and `remote_shape` does not refuse the read, so
/// a backend that holds no `admins` row denies it.
#[tokio::test]
async fn for_user_read_with_uncorrelated_exists_policy_matches_core() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("admins")
                        .column("user_id", ColumnType::Text)
                        .policies(permissions(|p| p.allow_read().always())),
                )
                .table(
                    TableSchema::builder("docs")
                        .column("title", ColumnType::Text)
                        .policies(permissions(|p| {
                            p.allow_read().where_(pe::exists(
                                pe::table("admins")
                                    .where_(pe::eq("user_id", pe::session(vec!["claims", "sub"]))),
                            ));
                        })),
                )
                .table(probe_table())
                .build();
            let server = JazzServer::start_with_schema(schema.clone()).await.unwrap();
            let seeder = connect_ready_client(&server, &schema, "seeder", "docs", WAIT).await;
            seeder
                .insert("admins", row_input!("user_id" => BOB_ID))
                .unwrap();
            let doc = seeder.insert("docs", row_input!("title" => "d")).unwrap().0;
            let as_bob = connect_ready_user(&server, &schema, BOB_ID, "docs", WAIT).await;
            core_grants(&as_bob, "docs", &[doc]).await;

            let backend = fresh_backend(&server, &schema, "backend").await;
            let read = remote_ids(&backend.for_session(bob()), "docs").await;
            assert_eq!(
                read,
                vec![doc],
                "the backend's for_user read must return what the core grants Bob"
            );
            server.shutdown().await;
        })
        .await;
}

/// Control for gap 1e (passes on main): the common membership pattern,
/// `exists members(doc_id = this.id, user_id = session.user)`. Correlated
/// `EXISTS` rows are shipped as policy support with each read, and a
/// revoked membership reaches the backend, so a later for_user read no
/// longer grants the doc.
#[tokio::test]
async fn for_user_read_after_membership_revoked_does_not_leak() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("members")
                        .column("user_id", ColumnType::Text)
                        .fk_column("doc_id", "docs")
                        .policies(permissions(|p| {
                            p.allow_read()
                                .where_(pe::eq("user_id", pe::session(vec!["claims", "sub"])));
                        })),
                )
                .table(
                    TableSchema::builder("docs")
                        .column("title", ColumnType::Text)
                        .policies(permissions(|p| {
                            p.allow_read()
                                .where_(pe::exists(pe::table("members").where_(pe::all_of([
                                    pe::eq("doc_id", pe::session(vec!["__jazz_outer_row", "id"])),
                                    pe::eq("user_id", pe::session(vec!["claims", "sub"])),
                                ]))));
                        })),
                )
                .table(probe_table())
                .build();
            let server = JazzServer::start_with_schema(schema.clone()).await.unwrap();
            let seeder = connect_ready_client(&server, &schema, "seeder", "docs", WAIT).await;
            let doc = seeder.insert("docs", row_input!("title" => "d")).unwrap().0;
            let (member, _, _) = seeder
                .insert("members", row_input!("user_id" => BOB_ID, "doc_id" => doc))
                .unwrap();
            let as_bob = connect_ready_user(&server, &schema, BOB_ID, "docs", WAIT).await;
            core_grants(&as_bob, "docs", &[doc]).await;

            let backend = fresh_backend(&server, &schema, "backend").await;
            assert_eq!(
                remote_ids(&backend.for_session(bob()), "docs").await,
                vec![doc],
                "while Bob is a member, the backend grants the doc"
            );

            let revoke = seeder.delete("members", member).unwrap().unwrap();
            seeder
                .wait_for_transaction(revoke, DurabilityTier::GlobalServer)
                .await
                .unwrap();
            core_grants(&as_bob, "docs", &[]).await;

            let read = remote_ids(&backend.for_session(bob()), "docs").await;
            assert_eq!(
                read,
                Vec::<ObjectId>::new(),
                "after Bob's membership is deleted, the backend must not grant the doc"
            );
            server.shutdown().await;
        })
        .await;
}

/// Gap 1e (wrong ALLOW from a stale replica row): Bob is an admin, the
/// backend reads `admins` once (as itself, as any admin screen would),
/// then Bob's admin row is deleted at the core. The core now grants Bob
/// nothing, but the backend still holds the deleted `admins` row (a
/// one-shot read is not kept current) and keeps granting the doc.
#[tokio::test]
async fn for_user_read_does_not_rest_on_stale_policy_rows() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("admins")
                        .column("user_id", ColumnType::Text)
                        .policies(permissions(|p| p.allow_read().always())),
                )
                .table(
                    TableSchema::builder("docs")
                        .column("title", ColumnType::Text)
                        .policies(permissions(|p| {
                            p.allow_read().where_(pe::exists(
                                pe::table("admins")
                                    .where_(pe::eq("user_id", pe::session(vec!["claims", "sub"]))),
                            ));
                        })),
                )
                .table(probe_table())
                .build();
            let server = JazzServer::start_with_schema(schema.clone()).await.unwrap();
            let seeder = connect_ready_client(&server, &schema, "seeder", "docs", WAIT).await;
            let (admin, _, _) = seeder
                .insert("admins", row_input!("user_id" => BOB_ID))
                .unwrap();
            let doc = seeder.insert("docs", row_input!("title" => "d")).unwrap().0;
            let as_bob = connect_ready_user(&server, &schema, BOB_ID, "docs", WAIT).await;
            core_grants(&as_bob, "docs", &[doc]).await;

            let backend = fresh_backend(&server, &schema, "backend").await;
            // The backend's own read of the admins list.
            assert_eq!(remote_ids(&backend, "admins").await, vec![admin]);
            assert_eq!(
                remote_ids(&backend.for_session(bob()), "docs").await,
                vec![doc],
                "while Bob is an admin, the backend grants the doc"
            );

            let revoke = seeder.delete("admins", admin).unwrap().unwrap();
            seeder
                .wait_for_transaction(revoke, DurabilityTier::GlobalServer)
                .await
                .unwrap();
            core_grants(&as_bob, "docs", &[]).await;

            let read = remote_ids(&backend.for_session(bob()), "docs").await;
            assert_eq!(
                read,
                Vec::<ObjectId>::new(),
                "after Bob's admin row is deleted, the backend must not grant the doc"
            );
            server.shutdown().await;
        })
        .await;
}

/// Gap 1c (claims bleed across handles): Rust `for_session` registers a
/// session's claims per *author* in the shared Db
/// (`Db::set_identity_claims`), not per handle or request. A live
/// subscription for Bob acting in org `o1` is re-evaluated under the
/// claims of a later handle for Bob in org `o2`, and starts returning
/// `o2`'s doc.
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

/// Gap 1a, relation form (wrong deny): `policy.exists(relation)` lowers to
/// `PolicyExpr::ExistsRel`, which `exists_support` does not look at, even
/// when it is correlated with the read row. The core grants Bob the doc; a
/// fresh backend holds no `members` row and denies it.
#[tokio::test]
async fn for_user_read_with_exists_rel_policy_matches_core() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("members")
                        .column("user_id", ColumnType::Text)
                        .fk_column("doc_id", "docs")
                        .policies(permissions(|p| p.allow_read().always())),
                )
                .table(
                    TableSchema::builder("docs")
                        .column("title", ColumnType::Text)
                        .policies(permissions(|p| {
                            p.allow_read()
                                .where_(pe::exists(pe::table("members").where_(pe::rel::all_of(
                                    [
                                        pe::rel::eq_outer("doc_id", "id"),
                                        pe::rel::eq_session("user_id", vec!["claims", "sub"]),
                                    ],
                                ))));
                        })),
                )
                .table(probe_table())
                .build();
            let server = JazzServer::start_with_schema(schema.clone()).await.unwrap();
            let seeder = connect_ready_client(&server, &schema, "seeder", "docs", WAIT).await;
            let doc = seeder.insert("docs", row_input!("title" => "d")).unwrap().0;
            seeder
                .insert("members", row_input!("user_id" => BOB_ID, "doc_id" => doc))
                .unwrap();
            let as_bob = connect_ready_user(&server, &schema, BOB_ID, "docs", WAIT).await;
            core_grants(&as_bob, "docs", &[doc]).await;

            let backend = fresh_backend(&server, &schema, "backend").await;
            let read = remote_ids(&backend.for_session(bob()), "docs").await;
            assert_eq!(read, vec![doc]);
            server.shutdown().await;
        })
        .await;
}

/// Reverse inheritance (`allowedTo.read` from referencing rows,
/// `PolicyExpr::InheritsReferencing`) is not in `inherited_support`
/// either: a doc readable because Bob may read a `shares` row naming it.
#[tokio::test]
async fn for_user_read_with_inherits_referencing_policy_matches_core() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("docs")
                        .column("title", ColumnType::Text)
                        .policies(permissions(|p| {
                            p.allow_read()
                                .where_(pe::allowed_to_read_referencing("shares", "doc_id"));
                        })),
                )
                .table(
                    TableSchema::builder("shares")
                        .column("user_id", ColumnType::Text)
                        .fk_column("doc_id", "docs")
                        .policies(permissions(|p| {
                            p.allow_read()
                                .where_(pe::eq("user_id", pe::session(vec!["claims", "sub"])));
                        })),
                )
                .table(probe_table())
                .build();
            let server = JazzServer::start_with_schema(schema.clone()).await.unwrap();
            let seeder = connect_ready_client(&server, &schema, "seeder", "docs", WAIT).await;
            let doc = seeder.insert("docs", row_input!("title" => "d")).unwrap().0;
            seeder
                .insert("shares", row_input!("user_id" => BOB_ID, "doc_id" => doc))
                .unwrap();
            let as_bob = connect_ready_user(&server, &schema, BOB_ID, "docs", WAIT).await;
            core_grants(&as_bob, "docs", &[doc]).await;

            let backend = fresh_backend(&server, &schema, "backend").await;
            let read = remote_ids(&backend.for_session(bob()), "docs").await;
            assert_eq!(read, vec![doc]);
            server.shutdown().await;
        })
        .await;
}
