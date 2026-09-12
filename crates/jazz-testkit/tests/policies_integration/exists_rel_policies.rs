use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_client, connect_ready_user, wait_for_query};

use super::*;

/// Verifies that a nested EXISTS_REL policy survives public-schema conversion
/// and authorises a correlated insert when its scanned tables expose SELECT.
#[tokio::test]
async fn local_insert_with_nested_exists_rel_policy_allows_correlated_insert() {
    tokio::task::LocalSet::new()
        .run_until(local_insert_with_nested_exists_rel_policy_allows_correlated_insert_inner(true))
        .await;
}

/// Alice's correlated write can use private policy evidence, without exposing it.
#[tokio::test]
async fn nested_exists_rel_insert_reads_private_raw_evidence() {
    tokio::task::LocalSet::new()
        .run_until(local_insert_with_nested_exists_rel_policy_allows_correlated_insert_inner(false))
        .await;
}

async fn local_insert_with_nested_exists_rel_policy_allows_correlated_insert_inner(
    expose_evidence: bool,
) {
    let projects_policies = permissions(|p| {
        p.allow_insert()
            .where_(pe::exists(pe::table("admins").where_(pe::all_of([
                pe::eq("id", pe::session(vec!["__jazz_outer_row", "admin_id"])),
                pe::eq("user_id", pe::session(vec!["claims", "sub"])),
                pe::exists(pe::table("team_memberships").where_(pe::rel::all_of([
                    pe::rel::eq_outer("id", "membership_id"),
                    pe::rel::eq_session("user_id", vec!["claims", "sub"]),
                ]))),
            ]))));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .policies(permissions(|p| {
                    if expose_evidence {
                        p.allow_read().always();
                    } else {
                        p.allow_insert().always();
                    }
                })),
        )
        .table(
            TableSchema::builder("team_memberships")
                .column("user_id", ColumnType::Text)
                .policies(permissions(|p| {
                    if expose_evidence {
                        p.allow_read().always();
                    } else {
                        p.allow_insert().always();
                    }
                })),
        )
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .fk_column("admin_id", "admins")
                .fk_column("membership_id", "team_memberships")
                .policies(projects_policies),
        )
        .build();
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let client = connect_ready_client(
        &server,
        &schema,
        "exists-rel-admin",
        "projects",
        Duration::from_secs(30),
    )
    .await;

    let admin_id = client
        .insert("admins", crate::row_input!("user_id" => super::ALICE_ID))
        .expect("seed admin row")
        .0;
    let membership_id = client
        .insert(
            "team_memberships",
            crate::row_input!("user_id" => super::ALICE_ID),
        )
        .expect("seed membership row")
        .0;

    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "projects",
        Duration::from_secs(30),
    )
    .await;
    let transaction = alice
        .insert(
            "projects",
            crate::row_input!(
                "name" => "alice project",
                "admin_id" => admin_id,
                "membership_id" => membership_id,
            ),
        )
        .expect("insert should be accepted optimistically")
        .2
        .expect("insert should be pending server policy evaluation");
    alice
        .wait_for_transaction(transaction, jazz::tools::DurabilityTier::EdgeServer)
        .await
        .expect("nested EXISTS_REL policy should authorise the correlated insert");

    if !expose_evidence {
        for table in ["admins", "team_memberships"] {
            wait_for_query(
                &alice,
                Query::from(table),
                Some(jazz::tools::DurabilityTier::EdgeServer),
                Duration::from_secs(5),
                "policy evidence stays private",
                |rows| rows.is_empty().then_some(()),
            )
            .await;
        }
    }
    alice.shutdown().await.expect("shutdown Alice client");
    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}

/// Verifies local INSERT enforcement for an EXISTS_REL admin policy: sessions
/// without a matching admin row are denied and admins are allowed.
#[tokio::test]
async fn local_insert_with_exists_rel_policy_denies_non_admin() {
    tokio::task::LocalSet::new()
        .run_until(local_insert_with_exists_rel_policy_denies_non_admin_inner())
        .await;
}

async fn local_insert_with_exists_rel_policy_denies_non_admin_inner() {
    let projects_policies = permissions(|p| {
        p.allow_insert()
            .where_(pe::exists(pe::table("admins").where_(pe::rel::all_of([
                pe::rel::eq_outer("id", "admin_id"),
                pe::rel::eq_session("user_id", vec!["claims", "sub"]),
            ]))));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .policies(permissions(|p| p.allow_read().always())),
        )
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .fk_column("admin_id", "admins")
                .policies(projects_policies),
        )
        .build();
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let client = connect_ready_client(
        &server,
        &schema,
        "exists-rel-admin",
        "projects",
        Duration::from_secs(30),
    )
    .await;

    let admin_id = client
        .insert("admins", crate::row_input!("user_id" => super::ALICE_ID))
        .expect("seed admin row")
        .0;

    let bob = connect_ready_user(
        &server,
        &schema,
        super::BOB_ID,
        "projects",
        Duration::from_secs(30),
    )
    .await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "projects",
        Duration::from_secs(30),
    )
    .await;

    let bob_transaction = bob
        .insert(
            "projects",
            crate::row_input!("name" => "bob project", "admin_id" => admin_id),
        )
        .expect("non-admin insert should be accepted optimistically")
        .2
        .expect("non-admin insert should be pending server policy evaluation");
    assert!(
        bob.wait_for_transaction(bob_transaction, jazz::tools::DurabilityTier::EdgeServer)
            .await
            .is_err(),
        "non-admin insert should be denied by the server"
    );

    let alice_transaction = alice
        .insert(
            "projects",
            crate::row_input!("name" => "alice project", "admin_id" => admin_id),
        )
        .expect("admin insert should be accepted")
        .2;
    if let Some(transaction) = alice_transaction {
        alice
            .wait_for_transaction(transaction, jazz::tools::DurabilityTier::EdgeServer)
            .await
            .expect("admin insert should be allowed by the server");
    }

    bob.shutdown().await.expect("shutdown Bob client");
    alice.shutdown().await.expect("shutdown Alice client");
    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}

/// Verifies that EXISTS_REL scans require an explicit SELECT policy on the
/// scanned table under enforcing mode.
#[tokio::test]
#[ignore = "#1759: schema conversion requires ExistsRel policies to include an outer-row equality"]
async fn local_insert_with_exists_rel_policy_requires_explicit_select_on_scanned_table() {
    tokio::task::LocalSet::new()
        .run_until(
            local_insert_with_exists_rel_policy_requires_explicit_select_on_scanned_table_inner(),
        )
        .await;
}

async fn local_insert_with_exists_rel_policy_requires_explicit_select_on_scanned_table_inner() {
    let projects_policies = permissions(|p| {
        p.allow_insert().where_(pe::exists(
            pe::table("admins").where_(pe::rel::eq_session("user_id", vec!["claims", "sub"])),
        ));
    });
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("admins").column("user_id", ColumnType::Text))
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .policies(projects_policies),
        )
        .build();
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let client = connect_ready_client(
        &server,
        &schema,
        "exists-rel-admin",
        "projects",
        Duration::from_secs(30),
    )
    .await;

    client
        .insert("admins", crate::row_input!("user_id" => super::ALICE_ID))
        .expect("seed admin row");

    let err = client
        .for_session(Session::new("urn:jazz:test", super::ALICE_ID))
        .insert("projects", crate::row_input!("name" => "alice project"))
        .expect_err(
            "enforcing mode should deny EXISTS_REL scans when the scanned table lacks an explicit SELECT policy",
        );
    assert_client_policy_denied(err, "projects", Operation::Insert);

    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}

/// Verifies that relation predicates compare NULL literals correctly inside
/// EXISTS_REL, allowing active rows and denying revoked rows.
#[tokio::test]
async fn local_insert_with_exists_rel_null_literal_predicate_matches_null_rows() {
    tokio::task::LocalSet::new()
        .run_until(local_insert_with_exists_rel_null_literal_predicate_matches_null_rows_inner())
        .await;
}

async fn local_insert_with_exists_rel_null_literal_predicate_matches_null_rows_inner() {
    let projects_policies = permissions(|p| {
        p.allow_insert()
            .where_(pe::exists(pe::table("admins").where_(pe::rel::all_of([
                pe::rel::eq_outer("id", "admin_id"),
                pe::rel::eq_session("user_id", vec!["claims", "sub"]),
                pe::rel::eq_literal("revoked_at", Value::Null),
            ]))));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .nullable_column("revoked_at", ColumnType::Text)
                .policies(permissions(|p| p.allow_read().always())),
        )
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .fk_column("admin_id", "admins")
                .policies(projects_policies),
        )
        .build();
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let client = connect_ready_client(
        &server,
        &schema,
        "exists-rel-admin",
        "projects",
        Duration::from_secs(30),
    )
    .await;

    let alice_admin_id = client
        .insert(
            "admins",
            crate::row_input!("user_id" => super::ALICE_ID, "revoked_at" => Value::Null),
        )
        .expect("seed active admin row")
        .0;
    let carol_admin_id = client
        .insert(
            "admins",
            crate::row_input!("user_id" => super::CAROL_ID, "revoked_at" => "2026-03-30T12:00:00Z"),
        )
        .expect("seed revoked admin row")
        .0;

    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "projects",
        Duration::from_secs(30),
    )
    .await;
    let carol = connect_ready_user(
        &server,
        &schema,
        super::CAROL_ID,
        "projects",
        Duration::from_secs(30),
    )
    .await;

    let alice_transaction = alice
        .insert(
            "projects",
            crate::row_input!("name" => "alice project", "admin_id" => alice_admin_id),
        )
        .expect("active admin row should satisfy revoked_at = NULL predicate")
        .2;
    if let Some(transaction) = alice_transaction {
        alice
            .wait_for_transaction(transaction, jazz::tools::DurabilityTier::EdgeServer)
            .await
            .expect("active admin row should satisfy revoked_at = NULL predicate");
    }

    let carol_transaction = carol
        .insert(
            "projects",
            crate::row_input!("name" => "carol project", "admin_id" => carol_admin_id),
        )
        .expect("revoked admin insert should be accepted optimistically")
        .2
        .expect("revoked admin insert should be pending server policy evaluation");
    assert!(
        carol
            .wait_for_transaction(carol_transaction, jazz::tools::DurabilityTier::EdgeServer)
            .await
            .is_err(),
        "revoked admin row should fail revoked_at = NULL predicate"
    );

    alice.shutdown().await.expect("shutdown Alice client");
    carol.shutdown().await.expect("shutdown Carol client");
    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}

/// Verifies local DELETE enforcement for an EXISTS_REL admin policy, including
/// that an already-deleted row cannot be deleted a second time.
#[tokio::test]
async fn local_delete_with_exists_rel_policy_allows_admin_and_denies_non_admin() {
    tokio::task::LocalSet::new()
        .run_until(local_delete_with_exists_rel_policy_allows_admin_and_denies_non_admin_inner())
        .await;
}

async fn local_delete_with_exists_rel_policy_allows_admin_and_denies_non_admin_inner() {
    let protected_policies = permissions(|p| {
        p.allow_read().always();
        p.allow_delete()
            .where_(pe::exists(pe::table("admins").where_(pe::rel::all_of([
                pe::rel::eq_outer("id", "admin_id"),
                pe::rel::eq_session("user_id", vec!["claims", "sub"]),
            ]))));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .policies(permissions(|p| p.allow_read().always())),
        )
        .table(
            TableSchema::builder("protected")
                .column("data", ColumnType::Text)
                .fk_column("admin_id", "admins")
                .policies(protected_policies),
        )
        .build();
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let client = connect_ready_client(
        &server,
        &schema,
        "exists-rel-admin",
        "protected",
        Duration::from_secs(30),
    )
    .await;

    let admin_id = client
        .insert("admins", crate::row_input!("user_id" => super::ALICE_ID))
        .expect("seed admin row")
        .0;
    let protected = client
        .insert(
            "protected",
            crate::row_input!("data" => "initial", "admin_id" => admin_id),
        )
        .expect("seed protected row")
        .0;

    let bob = connect_ready_user(
        &server,
        &schema,
        super::BOB_ID,
        "protected",
        Duration::from_secs(30),
    )
    .await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "protected",
        Duration::from_secs(30),
    )
    .await;

    let bob_transaction = bob
        .delete(protected)
        .expect("non-admin delete should be accepted optimistically")
        .expect("non-admin delete should be pending server policy evaluation");
    assert!(
        bob.wait_for_transaction(bob_transaction, jazz::tools::DurabilityTier::EdgeServer)
            .await
            .is_err(),
        "non-admin delete should be denied"
    );

    let alice_transaction = alice
        .delete(protected)
        .expect("admin delete should be accepted optimistically");
    if let Some(transaction) = alice_transaction {
        alice
            .wait_for_transaction(transaction, jazz::tools::DurabilityTier::EdgeServer)
            .await
            .expect("admin delete should be allowed");
    }
    let second_delete = alice
        .delete(protected)
        .expect_err("deleted row should not be deleted again");
    assert!(format!("{second_delete:?}").contains("row already deleted"));

    bob.shutdown().await.expect("shutdown Bob client");
    alice.shutdown().await.expect("shutdown Alice client");
    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}

/// An independent, session-filtered grant gates every protected row. Duplicate
/// grants do not duplicate results; removing the last grant revokes the
/// existing subscription even though the protected rows themselves do not change.
#[tokio::test]
async fn uncorrelated_exists_rel_select_tracks_private_grants() {
    uncorrelated_select_tracks_private_grants(pe::exists(
        pe::table("grants").where_(pe::rel::eq_session("user_id", vec!["claims", "sub"])),
    ))
    .await;
}

#[tokio::test]
async fn uncorrelated_exists_select_tracks_private_grants() {
    uncorrelated_select_tracks_private_grants(pe::exists(
        pe::table("grants").where_(pe::eq("user_id", pe::session(vec!["claims", "sub"]))),
    ))
    .await;
}

async fn uncorrelated_select_tracks_private_grants(policy: jazz::tools::PolicyExpr) {
    tokio::task::LocalSet::new()
        .run_until(async {
            use jazz::tools::DurabilityTier;
            use jazz_testkit::{
                connect_ready_user, has_added_id, has_removed, wait_for_edge_txs, wait_for_query,
                wait_for_subscription_update,
            };
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("grants")
                        .column("user_id", ColumnType::Text)
                        .policies(permissions(|p| {
                            p.allow_read().never();
                            p.allow_insert().always();
                            p.allow_delete().always();
                        })),
                )
                .table(
                    TableSchema::builder("protected")
                        .column("data", ColumnType::Text)
                        .policies(permissions(|p| {
                            p.allow_read().where_(policy);
                            p.allow_insert().always();
                        })),
                )
                .build();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let admin = connect_ready_client(
                &server,
                &schema,
                "exists-select-admin",
                "grants",
                Duration::from_secs(30),
            )
            .await;
            let alice = connect_ready_user(
                &server,
                &schema,
                ALICE_ID,
                "grants",
                Duration::from_secs(30),
            )
            .await;
            let bob =
                connect_ready_user(&server, &schema, BOB_ID, "grants", Duration::from_secs(30))
                    .await;
            let (row, _, tx) = admin
                .insert("protected", row_input!("data" => "secret"))
                .unwrap();
            wait_for_edge_txs(&admin, &[tx.unwrap()]).await;
            let query = Query::from("protected").select(["data"]);
            let mut stream = alice.subscribe(query.clone()).await.unwrap();
            let mut log = Vec::new();
            wait_for_query(
                &alice,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(5),
                "empty grant table denies SELECT",
                |rows| rows.is_empty().then_some(()),
            )
            .await;
            let (grant1, _, tx1) = admin
                .insert("grants", row_input!("user_id" => ALICE_ID))
                .unwrap();
            let (grant2, _, tx2) = admin
                .insert("grants", row_input!("user_id" => ALICE_ID))
                .unwrap();
            wait_for_edge_txs(&admin, &[tx1.unwrap(), tx2.unwrap()]).await;
            wait_for_subscription_update(
                &mut stream,
                &mut log,
                Duration::from_secs(5),
                "adding a grant makes the row visible to the existing subscription",
                |log| has_added_id(log, row),
            )
            .await;
            wait_for_query(
                &alice,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(5),
                "duplicate grants produce one row",
                |rows| (rows == [(row, vec![Value::Text("secret".into())])]).then_some(()),
            )
            .await;
            wait_for_query(
                &bob,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(5),
                "Alice's grant does not authorize Bob",
                |rows| rows.is_empty().then_some(()),
            )
            .await;
            let tx = admin.delete(grant1).unwrap();
            wait_for_edge_txs(&admin, &[tx.unwrap()]).await;
            wait_for_query(
                &alice,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(5),
                "second grant keeps access",
                |rows| (rows.len() == 1).then_some(()),
            )
            .await;
            wait_for_query(
                &alice,
                Query::from("grants"),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(5),
                "private grant rows stay hidden",
                |rows| rows.is_empty().then_some(()),
            )
            .await;
            log.clear();
            let tx = admin.delete(grant2).unwrap();
            wait_for_edge_txs(&admin, &[tx.unwrap()]).await;
            wait_for_query(
                &alice,
                query,
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(5),
                "no remaining grants denies SELECT",
                |rows| rows.is_empty().then_some(()),
            )
            .await;
            wait_for_subscription_update(
                &mut stream,
                &mut log,
                Duration::from_secs(5),
                "last grant removed revokes subscription",
                |log| has_removed(log, row),
            )
            .await;
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
            admin.shutdown().await.unwrap();
            server.shutdown().await;
        })
        .await;
}

/// Boolean clause order must not remove the owner check after an EXISTS-bearing OR.
#[tokio::test]
async fn exists_rel_disjunction_preserves_following_owner_predicate() {
    tokio::task::LocalSet::new().run_until(async {
        for owner_first in [false, true] {
            let policies = permissions(|p| {
                let owner = pe::eq("owner_id", pe::session(vec!["claims", "sub"]));
                let alternative = pe::any_of([
                    pe::eq("name", pe::literal("shortcut")),
                    pe::exists(pe::table("admins").where_(pe::rel::all_of([
                        pe::rel::eq_outer("id", "admin_id"),
                        pe::rel::eq_session("user_id", vec!["claims", "sub"]),
                    ]))),
                ]);
                p.allow_insert().where_(pe::all_of(if owner_first {
                    [owner, alternative]
                } else {
                    [alternative, owner]
                }));
            });
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("admins")
                    .column("user_id", ColumnType::Text)
                    .policies(permissions(|p| p.allow_read().always())))
                .table(TableSchema::builder("projects")
                    .column("name", ColumnType::Text)
                    .column("owner_id", ColumnType::Text)
                    .fk_column("admin_id", "admins")
                    .policies(policies))
                .build();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let backend = connect_ready_client(&server, &schema, "boolean-policy-seed", "projects", Duration::from_secs(30)).await;
            let admin_id = backend.insert("admins", crate::row_input!("user_id" => super::ALICE_ID)).expect("seed admin").0;
            let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "projects", Duration::from_secs(30)).await;
            for name in ["shortcut", "admin path"] {
                for (owner, allowed) in [(super::ALICE_ID, true), (super::CAROL_ID, false)] {
                    let result = alice.insert("projects", crate::row_input!("name" => name, "owner_id" => owner, "admin_id" => admin_id));
                    match result {
                        Ok((_, _, transaction)) => {
                            let transaction = transaction.expect("write requires authority settlement");
                            let settled = alice.wait_for_transaction(transaction, jazz::tools::DurabilityTier::EdgeServer).await;
                            assert_eq!(settled.is_ok(), allowed, "owner_first={owner_first}, name={name}, owner={owner}: {settled:?}");
                        }
                        Err(error) if !allowed => assert_client_policy_denied(error, "projects", Operation::Insert),
                        Err(error) => panic!("allowed write rejected: {error:?}"),
                    }
                }
            }
            alice.shutdown().await.expect("shutdown Alice");
            backend.shutdown().await.expect("shutdown backend");
            server.shutdown().await;
        }
    }).await;
}
