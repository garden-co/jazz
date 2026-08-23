use std::collections::BTreeMap;
use std::io::{Cursor, Read};

mod common;

use common::{allow_all_policies, compile_schema};
use jazz::db::{Db, DbConfig, DbIdentity};
use jazz::groove::large_values::{INLINE_VALUE_MAX_BYTES, LargeValueKind};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnDescriptor, ColumnType, RowDescriptor, Schema, TableName, TableSchema};
use jazz::tx::DurabilityTier;

fn schema() -> JazzSchema {
    let columns = RowDescriptor::new(vec![
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("done", ColumnType::Boolean),
    ]);
    compile_schema(&Schema::from([(
        TableName::new("todos"),
        TableSchema::with_policies(columns, allow_all_policies()),
    )]))
}

fn open_db() -> Db<TestStorage> {
    let schema = schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    jazz::block_on(Db::open(DbConfig {
        schema,
        storage: TestStorage::new(&refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x91; 16]),
            author: AuthorId::from_bytes([0xa1; 16]),
        },
        id_source: None,
    }))
    .expect("open db")
}

struct NarrowReader {
    cursor: Cursor<Vec<u8>>,
    max_read: usize,
}

impl Read for NarrowReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let end = buffer.len().min(self.max_read);
        self.cursor.read(&mut buffer[..end])
    }
}

#[test]
fn streaming_create_publishes_one_ordinary_logical_row() {
    let db = open_db();
    let text = format!("{}streamed-tail-🙂", "x".repeat(INLINE_VALUE_MAX_BYTES * 3));
    let reader = NarrowReader {
        cursor: Cursor::new(text.clone().into_bytes()),
        max_read: 137,
    };
    let write = jazz::block_on(db.insert_streaming_value(
        "todos",
        BTreeMap::from([("done".to_owned(), Value::Bool(false))]),
        "title",
        LargeValueKind::String,
        reader,
    ))
    .expect("streaming insert");
    jazz::block_on(write.wait(DurabilityTier::Local)).expect("local durability");

    let table = schema().tables()[0].clone();
    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = db.read(&query).expect("read row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell(&table, "title"), Some(Value::String(text)));
    assert_eq!(rows[0].cell(&table, "done"), Some(Value::Bool(false)));
}

#[test]
fn streaming_create_validation_failure_publishes_no_row() {
    let db = open_db();
    let invalid_utf8 = Cursor::new(
        vec![b'x'; INLINE_VALUE_MAX_BYTES]
            .into_iter()
            .chain([0xff])
            .collect::<Vec<_>>(),
    );
    let result = jazz::block_on(db.insert_streaming_value(
        "todos",
        BTreeMap::from([("done".to_owned(), Value::Bool(false))]),
        "title",
        LargeValueKind::String,
        invalid_utf8,
    ));
    assert!(result.is_err());

    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    assert!(db.read(&query).expect("read rows").is_empty());
}
