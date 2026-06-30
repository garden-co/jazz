use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::schema::{JazzSchema, Policy, TableSchema};

fn author(byte: u8) -> AuthorId {
    AuthorId::from_bytes([byte; 16])
}

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)])
            .with_write_policy(Policy::shape(Query::from("todos"))),
    ])
}

fn open_db(identity: AuthorId) -> Db<MemoryStorage> {
    let schema = schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = MemoryStorage::new(&refs);
    jazz::db::block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: identity,
        },
        id_source: None,
        large_value_checkpoint_op_interval: 1024,
    }))
    .expect("open db")
}

#[test]
fn row_provenance_preserves_created_fields_and_advances_updated_at() {
    let alice = author(0xa1);
    let row = RowUuid::from_bytes([0x33; 16]);
    let db = open_db(alice);

    db.insert_with_id_at_ms(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("first".to_owned()))]),
        1_000,
    )
    .expect("insert row");

    db.update_at_ms(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
        2_000,
    )
    .expect("update row");

    let prepared = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = db.read(&prepared).expect("read row");
    assert_eq!(rows.len(), 1);

    let provenance = db
        .row_provenance(&rows[0])
        .expect("resolve provenance")
        .expect("row has provenance");

    assert_eq!(provenance.created_by, alice);
    assert_eq!(provenance.created_at.0, 1_000);
    assert_eq!(provenance.updated_by, alice);
    assert_eq!(provenance.updated_at.0, 2_000);
    assert!(provenance.created_at < provenance.updated_at);

    let (descriptor, raw) = rows[0].encoded_record();
    let encoded = jazz::groove::records::BorrowedRecord::new(raw, descriptor);
    let created_at_idx = descriptor
        .field_index("$createdAt")
        .expect("createdAt field");
    let updated_at_idx = descriptor
        .field_index("$updatedAt")
        .expect("updatedAt field");
    assert_eq!(
        encoded.get_u64(created_at_idx).expect("createdAt value"),
        1_000
    );
    assert_eq!(
        encoded.get_u64(updated_at_idx).expect("updatedAt value"),
        2_000
    );
}
