//! Direct Db tests deliberately live beside the implementation: precise catalogue
//! publication, an open author batch, and a persistent reopen must be controlled
//! independently. They use public Db/schema/query APIs and real Memory/RocksDB
//! storage; server-facing end-to-end migration is covered by the TS server suite.

fn allow_all_policies() -> crate::tools::TablePolicies {
    use crate::tools::{PolicyExpr, TablePolicies};
    TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

use crate::block_on;
use crate::db::{Db, DbConfig, DbIdentity, InsertOptions, MergeableTxOps};
use crate::groove::records::Value;
use crate::groove::storage::{MemoryStorage, OrderedKvStorage, ReopenableStorage};
use crate::ids::{AuthorSubject, NodeUuid, RowUuid};
use crate::protocol::{CurrentWriteSchema, MigrationLens, SchemaVersion, TableLens};
use crate::query::{Query, col, eq, lit};
use crate::schema::JazzSchema;
use crate::tools::{
    ColumnType, ObjectId, OpenTransactionId, SchemaBuilder, TableSchemaBuilder,
    Value as PublicValue,
};
use jazz_storage_rocksdb::RocksDbStorage;
use std::collections::BTreeMap;

fn schema(references: bool) -> JazzSchema {
    let sources = TableSchemaBuilder::new("sources");
    let sources = if references {
        sources
            .fk_column("target", "targets")
            .nullable_fk_column("optional", "targets")
            .array_fk_column("many", "targets")
    } else {
        sources
            .column("target", ColumnType::Uuid)
            .nullable_column("optional", ColumnType::Uuid)
            .column(
                "many",
                ColumnType::Array {
                    element: Box::new(ColumnType::Uuid),
                },
            )
    };
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                sources
                    .index_only(Vec::<String>::new())
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("targets")
                    .column("label", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
    .unwrap()
}

fn identity() -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([0x71; 16]),
        author: AuthorSubject::SYSTEM,
    }
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

// Db's catalogue APIs operate on Groove cells; build inputs with the public
// macro and convert only the value types this fixture uses.
fn cells(input: std::collections::HashMap<String, PublicValue>) -> BTreeMap<String, Value> {
    fn convert(value: PublicValue) -> Value {
        match value {
            PublicValue::Uuid(v) => Value::Uuid(*v.uuid()),
            PublicValue::Text(v) => Value::String(v),
            PublicValue::Null => Value::Nullable(None),
            PublicValue::Array(v) => Value::Array(v.into_iter().map(convert).collect()),
            _ => panic!("unexpected fixture value"),
        }
    }
    input.into_iter().map(|(k, v)| (k, convert(v))).collect()
}
fn source_cells(nullable: bool) -> BTreeMap<String, Value> {
    let target = PublicValue::Uuid(ObjectId::from_uuid(id(1).0));
    let mut result = cells(
        crate::row_input!("target" => target.clone(), "optional" => if nullable { PublicValue::Null } else { target.clone() }, "many" => PublicValue::Array(vec![target])),
    );
    if !nullable {
        result.insert(
            "optional".into(),
            Value::Nullable(Some(Box::new(Value::Uuid(id(1).0)))),
        );
    }
    result
}
async fn insert_source<S: OrderedKvStorage + ReopenableStorage + 'static>(
    db: &Db<S>,
    n: u8,
    nullable: bool,
) -> crate::db::WriteHandle<S> {
    db.insert(
        "sources",
        source_cells(nullable),
        InsertOptions {
            row_id: Some(id(n)),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}
fn assert_rows<S: OrderedKvStorage + ReopenableStorage + 'static>(db: &Db<S>, expected: &[u8]) {
    let query = db.prepare_query(&Query::from("sources")).unwrap();
    let rows = db.read(&query).unwrap();
    let mut actual = rows.iter().map(|r| r.row_uuid()).collect::<Vec<_>>();
    actual.sort();
    assert_eq!(actual, expected.iter().map(|n| id(*n)).collect::<Vec<_>>());
    for row in rows {
        assert_eq!(
            row.cell_at(0),
            Some(Value::Uuid(id(1).0)),
            "UUID bytes survive migration"
        );
        assert_eq!(
            row.cell_at(2),
            Some(Value::Array(vec![Value::Uuid(id(1).0)]))
        );
        assert_eq!(
            row.cell_at(1),
            Some(Value::Nullable(if row.row_uuid() == id(3) {
                None
            } else {
                Some(Box::new(Value::Uuid(id(1).0)))
            }))
        );
    }
    for column in ["target", "optional", "many"] {
        // Reverse traversal becomes available through the new reference metadata
        // and must find rows authored before publication.
        let query = db
            .prepare_query(&Query::from("targets").join_via("sources", column, []))
            .unwrap();
        assert_eq!(
            db.read(&query)
                .unwrap()
                .iter()
                .map(|r| r.row_uuid())
                .collect::<Vec<_>>(),
            vec![
                id(1);
                expected.len() - usize::from(column == "optional" && expected.contains(&3))
            ]
        );
    }
    // Constrained reads require admission of the reference-derived physical index.
    let query = db
        .prepare_query(&Query::from("sources").filter(eq(col("target"), lit(Value::Uuid(id(1).0)))))
        .unwrap();
    assert_eq!(db.read(&query).unwrap().len(), expected.len());
}
async fn migrate<S: OrderedKvStorage + ReopenableStorage + 'static>(db: &Db<S>) {
    db.insert(
        "targets",
        cells(crate::row_input!("label" => "existing target")),
        InsertOptions {
            row_id: Some(id(1)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    insert_source(db, 2, false).await;
    insert_source(db, 3, true).await;
    let pending = OpenTransactionId::new();
    db.begin_mergeable(pending).await.unwrap();
    db.mergeable_tx_ref(pending)
        .insert(
            "sources",
            source_cells(false),
            InsertOptions {
                row_id: Some(id(6)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    publish_reference_schema(db).await;
    assert_rows(db, &[2, 3]);
    db.commit_mergeable_handle(pending).await.unwrap();
    assert_rows(db, &[2, 3, 6]);
    insert_source(db, 4, false).await;
    assert_rows(db, &[2, 3, 4, 6]);
}

async fn publish_reference_schema<S: OrderedKvStorage + ReopenableStorage + 'static>(db: &Db<S>) {
    let old = schema(false);
    let new = SchemaVersion::new(schema(true));
    assert_ne!(old.version_id(), new.id);
    assert!(
        db.register_schema_view(new.schema.clone()).await.is_err(),
        "reference changes still require explicit lineage"
    );
    let lens = MigrationLens::new(
        old.version_id(),
        new.id,
        ["sources", "targets"]
            .into_iter()
            .map(|table| TableLens {
                source_table: table.into(),
                target_table: table.into(),
                ops: vec![],
            })
            .collect(),
    )
    .unwrap();
    let lens_id = lens.id();
    let publication = db
        .author_schema_lineage_publication(
            new.clone(),
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    db.publish_schema_with_lens(1, publication).await.unwrap();
    db.activate_catalogue_schema_for_test(CurrentWriteSchema {
        revision: 1,
        schema: new.id,
    })
    .await
    .unwrap();
    assert!(db.catalogue_lens(lens_id).is_some());
    assert_eq!(db.catalogue_schema(old.version_id()), Some(old));
}

/// Alice keeps existing UUID/null/array cells and an open author batch while
/// explicitly adding reference metadata: old writes -> publish lens -> new joins.
#[test]
fn explicit_identity_lens_adds_references_to_existing_uuid_columns() {
    block_on(async {
        let schema = schema(false);
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let db = Db::open(DbConfig::new(
            schema,
            MemoryStorage::new(&refs).unwrap(),
            identity(),
        ))
        .await
        .unwrap();
        migrate(&db).await;
    });
}

/// Alice closes and reopens RocksDB after migration; old/new schema views and
/// writes remain usable: migrate -> close -> reopen -> query both views -> write.
#[test]
fn reference_metadata_and_old_rows_survive_rocksdb_reopen() {
    block_on(async {
        let old = schema(false);
        let families = old.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let directory = tempfile::tempdir().unwrap();
        let db = Db::open(DbConfig::new(
            old.clone(),
            RocksDbStorage::open(directory.path(), &refs).unwrap(),
            identity(),
        ))
        .await
        .unwrap();
        migrate(&db).await;
        db.close().await.unwrap();
        drop(db);
        let db = Db::open(DbConfig::new(
            old.clone(),
            RocksDbStorage::open(directory.path(), &refs).unwrap(),
            identity(),
        ))
        .await
        .unwrap();
        assert_eq!(
            db.current_write_schema().unwrap().schema,
            schema(true).version_id()
        );
        assert_eq!(db.catalogue_schema(old.version_id()), Some(old));
        assert_eq!(
            db.catalogue_schema(schema(true).version_id()),
            Some(schema(true).without_permissions())
        );
        assert_rows(&db, &[2, 3, 4, 6]);
        let old_view = db.register_schema_view(schema(false)).await.unwrap();
        let old_query = old_view.prepare_query(&Query::from("sources")).unwrap();
        let old_rows = old_view.read(&old_query).unwrap();
        assert_eq!(old_rows.len(), 4);
        assert!(
            old_rows
                .iter()
                .all(|row| row.cell_at(0) == Some(Value::Uuid(id(1).0)))
        );
        drop(old_view);
        insert_source(&db, 5, false).await;
        assert_rows(&db, &[2, 3, 4, 5, 6]);
        db.close().await.unwrap();
    });
}

/// Alice uploads old rows to Bob's Core authority before Bob adds references.
/// alice -> globally accepted rows -> bob publishes lens -> indexed old-row read.
#[test]
fn reference_migration_backfills_authority_settled_rows() {
    block_on(async {
        let old = schema(false);
        let families = old.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let core = Db::open_history_complete(DbConfig::new(
            old.clone(),
            MemoryStorage::new(&refs).unwrap(),
            identity(),
        ))
        .await
        .unwrap();
        let writer = Db::open(DbConfig::new(
            old,
            MemoryStorage::new(&refs).unwrap(),
            DbIdentity {
                node: NodeUuid::from_bytes([0x72; 16]),
                author: AuthorSubject::for_test_bytes([0x73; 16]),
            },
        ))
        .await
        .unwrap();
        let (client_transport, server_transport) = super::support::duplex();
        core.accept_subscriber(server_transport, AuthorSubject::for_test_bytes([0x73; 16]));
        writer.connect_upstream(client_transport).await;
        let target = writer
            .insert(
                "targets",
                cells(crate::row_input!("label" => "existing target")),
                InsertOptions {
                    row_id: Some(id(1)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let first = insert_source(&writer, 2, false).await;
        let nullable = insert_source(&writer, 3, true).await;
        for _ in 0..8 {
            writer.tick().await.unwrap();
            core.tick().await.unwrap();
        }
        for write in [target, first, nullable] {
            write.wait(crate::tx::DurabilityTier::Global).await.unwrap();
        }
        publish_reference_schema(&core).await;
        assert_rows(&core, &[2, 3]);
        let query = core
            .prepare_query(
                &Query::from("sources").filter(eq(col("target"), lit(Value::Uuid(id(1).0)))),
            )
            .unwrap();
        let rows = core
            .all_for_identity(
                &query,
                crate::db::ReadOpts {
                    tier: crate::tx::DurabilityTier::Global,
                    local_updates: crate::db::LocalUpdates::Deferred,
                    propagation: crate::db::Propagation::LocalOnly,
                    ..Default::default()
                },
                AuthorSubject::for_test_bytes([0x73; 16]),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "new reference index must backfill existing authority-settled rows"
        );
    });
}
