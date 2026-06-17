use crate::JazzClient;

use super::*;

/// Verifies that enforcing mode propagates into nested EXISTS_REL scans, so a
/// missing explicit SELECT policy on a nested probed table denies the insert.
#[tokio::test]
async fn local_insert_with_exists_policy_propagates_enforcing_mode_to_nested_exists_rel() {
    let projects_policies = permissions(|p| {
        p.allow_insert()
            .where_(pe::exists(pe::table("admins").where_(pe::all_of([
                pe::eq("user_id", pe::session("user_id")),
                pe::exists(pe::table("team_memberships").where_(pe::rel::all_of([
                    pe::rel::eq_outer("team_id", "team_id"),
                    pe::rel::eq_session("user_id", "user_id"),
                ]))),
            ]))));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .column("team_id", ColumnType::Text),
        )
        .table(
            TableSchema::builder("team_memberships")
                .column("team_id", ColumnType::Text)
                .column("user_id", ColumnType::Text),
        )
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .policies(projects_policies),
        )
        .build();
    let client = JazzClient::test_client(schema).await;

    client
        .insert(
            "admins",
            crate::row_input!("user_id" => "alice", "team_id" => "team-a"),
        )
        .expect("seed admin row");
    client
        .insert(
            "team_memberships",
            crate::row_input!("team_id" => "team-a", "user_id" => "alice"),
        )
        .expect("seed membership row");

    let err = client
        .for_session(Session::new("alice"))
        .insert("projects", crate::row_input!("name" => "alice project"))
        .expect_err(
            "enforcing mode should deny nested EXISTS_REL checks when the probed table lacks an explicit SELECT policy",
        );
    assert_client_policy_denied(err, "projects", Operation::Insert);
}

/// Verifies local INSERT enforcement for an EXISTS_REL admin policy: sessions
/// without a matching admin row are denied and admins are allowed.
#[tokio::test]
async fn local_insert_with_exists_rel_policy_denies_non_admin() {
    let projects_policies = permissions(|p| {
        p.allow_insert().where_(pe::exists(
            pe::table("admins").where_(pe::rel::eq_session("user_id", "user_id")),
        ));
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
                .policies(projects_policies),
        )
        .build();
    let client = JazzClient::test_client(schema).await;

    client
        .insert("admins", crate::row_input!("user_id" => "alice"))
        .expect("seed admin row");

    let bob_err = client
        .for_session(Session::new("bob"))
        .insert("projects", crate::row_input!("name" => "bob project"))
        .expect_err("non-admin insert should be denied");
    assert_client_policy_denied(bob_err, "projects", Operation::Insert);

    client
        .for_session(Session::new("alice"))
        .insert("projects", crate::row_input!("name" => "alice project"))
        .expect("admin insert should be allowed");
}

/// Verifies that EXISTS_REL scans require an explicit SELECT policy on the
/// scanned table under enforcing mode.
#[tokio::test]
async fn local_insert_with_exists_rel_policy_requires_explicit_select_on_scanned_table() {
    let projects_policies = permissions(|p| {
        p.allow_insert().where_(pe::exists(
            pe::table("admins").where_(pe::rel::eq_session("user_id", "user_id")),
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
    let client = JazzClient::test_client(schema).await;

    client
        .insert("admins", crate::row_input!("user_id" => "alice"))
        .expect("seed admin row");

    let err = client
        .for_session(Session::new("alice"))
        .insert("projects", crate::row_input!("name" => "alice project"))
        .expect_err(
            "enforcing mode should deny EXISTS_REL scans when the scanned table lacks an explicit SELECT policy",
        );
    assert_client_policy_denied(err, "projects", Operation::Insert);
}

/// Verifies that relation predicates compare NULL literals correctly inside
/// EXISTS_REL, allowing active rows and denying revoked rows.
#[tokio::test]
async fn local_insert_with_exists_rel_null_literal_predicate_matches_null_rows() {
    let projects_policies = permissions(|p| {
        p.allow_insert()
            .where_(pe::exists(pe::table("admins").where_(pe::rel::all_of([
                pe::rel::eq_session("user_id", "user_id"),
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
                .policies(projects_policies),
        )
        .build();
    let client = JazzClient::test_client(schema).await;

    client
        .insert(
            "admins",
            crate::row_input!("user_id" => "alice", "revoked_at" => Value::Null),
        )
        .expect("seed active admin row");
    client
        .insert(
            "admins",
            crate::row_input!("user_id" => "carol", "revoked_at" => "2026-03-30T12:00:00Z"),
        )
        .expect("seed revoked admin row");

    client
        .for_session(Session::new("alice"))
        .insert("projects", crate::row_input!("name" => "alice project"))
        .expect("active admin row should satisfy revoked_at = NULL predicate");

    let carol_err = client
        .for_session(Session::new("carol"))
        .insert("projects", crate::row_input!("name" => "carol project"))
        .expect_err("revoked admin row should fail revoked_at = NULL predicate");
    assert_client_policy_denied(carol_err, "projects", Operation::Insert);
}

/// Verifies local DELETE enforcement for an EXISTS_REL admin policy, including
/// that an already-deleted row cannot be deleted a second time.
#[tokio::test]
async fn local_delete_with_exists_rel_policy_allows_admin_and_denies_non_admin() {
    let protected_policies = permissions(|p| {
        p.allow_delete().where_(pe::exists(
            pe::table("admins").where_(pe::rel::eq_session("user_id", "user_id")),
        ));
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
                .policies(protected_policies),
        )
        .build();
    let client = JazzClient::test_client(schema).await;

    client
        .insert("admins", crate::row_input!("user_id" => "alice"))
        .expect("seed admin row");
    let protected = client
        .insert("protected", crate::row_input!("data" => "initial"))
        .expect("seed protected row")
        .0;

    let bob_err = client
        .for_session(Session::new("bob"))
        .delete(protected)
        .expect_err("non-admin delete should be denied");
    assert_client_policy_denied(bob_err, "protected", Operation::Delete);

    client
        .for_session(Session::new("alice"))
        .delete(protected)
        .expect("admin delete should be allowed");
    let second_delete = client
        .for_session(Session::new("alice"))
        .delete(protected)
        .expect_err("deleted row should not be deleted again");
    assert!(format!("{second_delete:?}").contains("row already deleted"));
}
