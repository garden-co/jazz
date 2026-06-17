#![cfg(feature = "test")]

#[macro_use]
extern crate jazz_tools;

use std::collections::HashSet;
use std::time::Duration;

pub use jazz_tools::metadata::SYSTEM_PRINCIPAL_ID;
use jazz_tools::query_manager::policy::Operation;
use jazz_tools::query_manager::session::{Session, WriteContext};
use jazz_tools::query_manager::types::{permissions, policy_expr as pe};
pub use jazz_tools::row_input;
pub use jazz_tools::{
    AppContext, ColumnType, JazzClient, JazzError, ObjectId, OrderedRowDelta, QueryBuilder, Schema,
    SchemaBuilder, SubscriptionStream, TableSchema, Value,
};
pub use jazz_tools::{query_manager, server, sync_manager, test_support};

#[path = "../support/mod.rs"]
mod support;

fn explicit_allow_all_policies(
    mut policies: jazz_tools::query_manager::types::TablePolicies,
) -> jazz_tools::query_manager::types::TablePolicies {
    use jazz_tools::query_manager::policy::PolicyExpr;

    if policies.select.using.is_none() {
        policies.select.using = Some(PolicyExpr::True);
    }
    if policies.insert.with_check.is_none() {
        policies.insert.with_check = Some(PolicyExpr::True);
    }
    if policies.update.using.is_none() && policies.update.with_check.is_none() {
        policies.update.using = Some(PolicyExpr::True);
        policies.update.with_check = Some(PolicyExpr::True);
    }
    if policies.delete.using.is_none() {
        policies.delete.using = Some(PolicyExpr::True);
    }

    policies
}

/// Schema for ReBAC tests: documents with owner_id policy + folders for INHERITS
fn rebac_test_schema() -> Schema {
    let folders_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::eq("owner_id", pe::session("user_id")));
        p.allow_insert()
            .where_(pe::eq("owner_id", pe::session("user_id")));
    });

    let docs_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::eq("owner_id", pe::session("user_id")));
        p.allow_insert()
            .where_(pe::eq("owner_id", pe::session("user_id")));
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

fn magic_introspection_schema() -> Schema {
    let is_admin = pe::eq("user_id", pe::session("user_id"));
    let protected_policies = permissions(|p| {
        p.allow_read().always();
        p.allow_update()
            .where_old(pe::exists(pe::table("admins").where_(is_admin.clone())))
            .where_new(pe::always());
        p.allow_delete().where_(pe::exists(
            pe::table("admins").where_(pe::rel::eq_session("user_id", "user_id")),
        ));
    });

    SchemaBuilder::new()
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
        .build()
}

fn provenance_notes_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("notes").column("title", ColumnType::Text))
        .build()
}

fn authorship_permissions_schema() -> Schema {
    let created_by_is_session = pe::eq("$createdBy", pe::session("user_id"));
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
            pe::eq("owner_id", pe::session("user_id")),
            select_inherited,
        ]));
        p.allow_update()
            .where_old(pe::any_of([
                pe::eq("owner_id", pe::session("user_id")),
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
            pe::eq("owner_id", pe::session("user_id")),
            pe::allowed_to_read_referencing("todos", source_fk_column),
        ]));
        p.allow_update()
            .where_old(pe::any_of([
                pe::eq("owner_id", pe::session("user_id")),
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
            .where_(pe::eq("owner_id", pe::session("user_id")));
        p.allow_update()
            .where_old(pe::eq("owner_id", pe::session("user_id")))
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
