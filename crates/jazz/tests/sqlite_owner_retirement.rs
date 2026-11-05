//! An external storage lease must outlive every writer, including retained handles.
mod common;
use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;

fn config() -> DbConfig {
    DbConfig::new(
        common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("text", ColumnType::Text)
                        .policies(common::allow_all_policies()),
                )
                .build(),
        ),
        DbIdentity {
            node: NodeUuid::from_bytes([21; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}
fn cells(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("text".into(), Value::String(text.into()))])
}

/// Alice's owner retires while Bob's old view and staged transaction remain
/// alive. Cached statement reuse and later commits cannot touch the file;
/// reopening the owner sees only the previously committed row.
#[test]
fn retired_owner_revokes_cached_statements_and_retained_views() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("notes.sqlite");
    let db = block_on(Db::open_at(config(), &path, FileOptions::default())).unwrap();
    let view = block_on(db.register_schema_view(config().schema)).unwrap();
    let row = RowUuid::from_bytes([1; 16]);
    block_on(view.upsert("notes", row, cells("before"), Default::default())).unwrap();
    // Warm the same update and query statements that the stale view will reuse.
    block_on(view.update("notes", row, cells("committed"), Default::default())).unwrap();
    let query = view.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        block_on(view.all(&query, ReadOpts::default()))
            .unwrap()
            .len(),
        1
    );
    let transaction = OpenTransactionId::new();
    block_on(view.begin_serializable(transaction)).unwrap();
    block_on(view.tx_ref(transaction).upsert(
        "notes",
        RowUuid::from_bytes([2; 16]),
        cells("staged"),
        Default::default(),
    ))
    .unwrap();
    db.retire_storage_owner().unwrap();
    assert!(block_on(view.update("notes", row, cells("late"), Default::default())).is_err());
    assert!(block_on(view.all(&query, ReadOpts::default())).is_err());
    assert!(block_on(view.commit_serializable_handle(transaction)).is_err());
    let reopened = block_on(Db::open_at(config(), &path, FileOptions::default())).unwrap();
    let query = reopened.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(reopened.all(&query, ReadOpts::default())).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .cells()
            .find(|(name, _)| *name == "text")
            .unwrap()
            .1
            .unwrap(),
        &Value::String("committed".into())
    );
}
