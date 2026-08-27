use std::collections::HashSet;
use std::time::Duration;

pub use jazz::query::{Query, col, eq, lit};
pub use jazz::row_input;
pub use jazz::tools::metadata::SYSTEM_PRINCIPAL_ID;
pub use jazz::tools::{
    AppContext, ColumnType, JazzClient, JazzError, ObjectId, OrderedRowDelta, Schema,
    SchemaBuilder, SubscriptionStream, TableSchema, Value,
};
use jazz::tools::{Operation, Session, WriteContext};
use jazz::tools::{permissions, policy_expr as pe};

// The server requires UUID principals
const ALICE_ID: &str = "9750dcc2-516e-5ea0-8a26-54fa6ff6986b";
const BOB_ID: &str = "756886b3-2033-583f-bd5a-a22f02fb5a6b";
const CAROL_ID: &str = "263ae6d4-cf47-5333-9fcd-c81d5d12a27c";
const CHARLIE_ID: &str = "275b74ef-e22a-59d6-8b2c-4face1410f59";
const DAVE_ID: &str = "e1010a53-b8c5-50a2-a61d-645964c37e67";
const MALLORY_ID: &str = "5363f5ca-d268-52d3-af19-c4c0c5e93f63";
const OBSERVER_ID: &str = "211663a4-14bd-52c4-92b4-f369967c20b3";

#[allow(unused_imports)]
mod support {
    pub use jazz_testkit::*;
}

fn explicit_allow_all_policies(
    mut policies: jazz::tools::TablePolicies,
) -> jazz::tools::TablePolicies {
    if policies.select.using.is_none() {
        policies.select.using = Some(pe::always());
    }
    if policies.insert.with_check.is_none() {
        policies.insert.with_check = Some(pe::always());
    }
    if policies.update.using.is_none() && policies.update.with_check.is_none() {
        policies.update.using = Some(pe::always());
        policies.update.with_check = Some(pe::always());
    }
    if policies.delete.using.is_none() {
        policies.delete.using = Some(pe::always());
    }

    policies
}

/// Schema for ReBAC tests: documents with owner_id policy + folders for INHERITS
fn rebac_test_schema() -> Schema {
    let folders_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::eq("owner_id", pe::session(vec!["claims", "sub"])));
        p.allow_insert()
            .where_(pe::eq("owner_id", pe::session(vec!["claims", "sub"])));
    });

    let docs_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::eq("owner_id", pe::session(vec!["claims", "sub"])));
        p.allow_insert()
            .where_(pe::eq("owner_id", pe::session(vec!["claims", "sub"])));
    });

    SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(folders_policies),
        )
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .nullable_fk_column("folder_id", "folders")
                .policies(docs_policies),
        )
        .build()
}

fn provenance_notes_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("notes").column("title", ColumnType::Text))
        .build()
}

fn authorship_permissions_schema() -> Schema {
    let created_by_is_session = pe::eq("$createdBy", pe::session("user"));
    let notes_policies = permissions(|p| {
        p.allow_read().where_(created_by_is_session.clone());
        p.allow_insert().where_(created_by_is_session.clone());
        p.allow_update().where_(created_by_is_session.clone());
        p.allow_delete().where_(created_by_is_session);
    });

    SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .policies(notes_policies),
        )
        .build()
}

fn assert_client_policy_denied(err: crate::JazzError, table: &str, operation: Operation) {
    let crate::JazzError::Write(message) = err else {
        panic!("expected policy denial write error, got {err:?}");
    };
    let expected = format!("policy denied {operation} on table {table}");
    assert!(
        message.ends_with(&expected),
        "expected denial ending in {expected:?}, got {message:?}",
    );
}

fn recursive_folders_schema(max_depth: Option<usize>) -> Schema {
    let select_inherited = match max_depth {
        Some(max_depth) => pe::allowed_to_read_with_depth("parent_id", max_depth),
        None => pe::allowed_to_read("parent_id"),
    };
    let update_inherited = match max_depth {
        Some(max_depth) => pe::allowed_to_update_with_depth("parent_id", max_depth),
        None => pe::allowed_to_update("parent_id"),
    };

    let folders_policies = permissions(|p| {
        p.allow_read().where_(pe::any_of([
            pe::eq("owner_id", pe::session(vec!["claims", "sub"])),
            select_inherited,
        ]));
        p.allow_update()
            .where_old(pe::any_of([
                pe::eq("owner_id", pe::session(vec!["claims", "sub"])),
                update_inherited,
            ]))
            .where_new(pe::always());
    });

    SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .nullable_fk_column("parent_id", "folders")
                .policies(folders_policies),
        )
        .build()
}

fn declared_file_inheritance_schema(array_edge: bool) -> Schema {
    let source_fk_column = if array_edge { "images" } else { "image" };
    let files_policies = permissions(|p| {
        p.allow_read().where_(pe::any_of([
            pe::eq("owner_id", pe::session(vec!["claims", "sub"])),
            pe::allowed_to_read_referencing("todos", source_fk_column),
        ]));
        p.allow_update()
            .where_old(pe::any_of([
                pe::eq("owner_id", pe::session(vec!["claims", "sub"])),
                pe::allowed_to_update_referencing("todos", source_fk_column),
            ]))
            .where_new(pe::always());
    });

    let todos_table = if array_edge {
        TableSchema::builder("todos")
            .column("owner_id", ColumnType::Text)
            .column("title", ColumnType::Text)
            .array_fk_column("images", "files")
    } else {
        TableSchema::builder("todos")
            .column("owner_id", ColumnType::Text)
            .column("title", ColumnType::Text)
            .nullable_fk_column("image", "files")
    };
    let todos_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::eq("owner_id", pe::session(vec!["claims", "sub"])));
        p.allow_update()
            .where_old(pe::eq("owner_id", pe::session(vec!["claims", "sub"])))
            .where_new(pe::always());
    });

    SchemaBuilder::new()
        .table(
            TableSchema::builder("files")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(files_policies),
        )
        .table(todos_table.policies(todos_policies))
        .build()
}

mod authorship_policies;
mod claims_policies;
mod complex_policies;
mod declared_fk_inheritance;
mod exists_policies;
mod exists_rel_policies;
mod inheritance_validation;
mod inherited_policies;
mod insert_policies;
mod magic_provenance;
mod mutations;
mod recursive_inheritance;
mod recursive_policies;
mod select_policies;
mod session_cases;
mod simple_policies;
