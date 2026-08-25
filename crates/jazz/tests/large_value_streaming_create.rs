use std::collections::BTreeMap;
use std::io::{Cursor, Read};

mod common;

use common::{allow_all_policies, compile_schema};
use jazz::db::{Db, DbConfig, DbIdentity, StreamingMutationKind};
use jazz::groove::large_values::{INLINE_VALUE_MAX_BYTES, LEAF_MAX_BYTES};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::LargeValueStagingPolicy;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnDescriptor, ColumnType, RowDescriptor, Schema, TableName, TableSchema};
use jazz::tx::DurabilityTier;

fn schema() -> JazzSchema {
    let columns = RowDescriptor::new(vec![
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("payload", ColumnType::Json { schema: None }),
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
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
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

struct FailingReader {
    cursor: Cursor<Vec<u8>>,
}

impl Read for FailingReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.cursor.read(buffer)?;
        if read == 0 {
            Err(std::io::Error::other("injected reader failure"))
        } else {
            Ok(read)
        }
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
fn streaming_create_derives_json_kind_from_the_column_schema() {
    let db = open_db();
    let json = format!(r#"{{"body":"{}"}}"#, "json-".repeat(INLINE_VALUE_MAX_BYTES));
    let write = jazz::block_on(db.insert_streaming_value(
        "todos",
        BTreeMap::from([("done".to_owned(), Value::Bool(false))]),
        "payload",
        Cursor::new(json.clone()),
    ))
    .expect("the JSON column, not a caller ABI argument, determines staging kind");
    jazz::block_on(write.wait(DurabilityTier::Local)).expect("local durability");

    let table = schema().tables()[0].clone();
    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = db.read(&query).expect("read row");
    assert_eq!(rows[0].cell(&table, "payload"), Some(Value::String(json)));
}

#[test]
fn streaming_create_validation_failure_publishes_no_row() {
    let db = open_db();
    let invalid_utf8 = Cursor::new(
        vec![b'x'; LEAF_MAX_BYTES + 1]
            .into_iter()
            .chain([0xff])
            .collect::<Vec<_>>(),
    );
    let result = jazz::block_on(db.insert_streaming_value(
        "todos",
        BTreeMap::from([("done".to_owned(), Value::Bool(false))]),
        "title",
        invalid_utf8,
    ));
    assert!(result.is_err());

    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    assert!(db.read(&query).expect("read rows").is_empty());
    db.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 60_000,
        max_age_ms: 0,
    });
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert_eq!(
        jazz::block_on(db.evict_expired_staged_large_values()).expect("expiry pass"),
        0,
        "terminal validation failure immediately removes its pending claim"
    );
}

#[test]
fn streaming_update_and_upsert_publish_ordinary_logical_rows() {
    let db = open_db();
    let inserted = jazz::block_on(db.insert_streaming_value(
        "todos",
        BTreeMap::from([("done".to_owned(), Value::Bool(false))]),
        "title",
        Cursor::new("initial"),
    ))
    .expect("streaming insert");
    let row = inserted.row_uuid();

    jazz::block_on(db.write_streaming_value_with_id(
        StreamingMutationKind::Update,
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        "title",
        Cursor::new("updated"),
        None,
        Some(42),
        None,
        None,
    ))
    .expect("streaming update");
    jazz::block_on(db.write_streaming_value_with_id(
        StreamingMutationKind::Upsert,
        "todos",
        row,
        BTreeMap::new(),
        "title",
        Cursor::new("upserted"),
        None,
        Some(43),
        None,
        None,
    ))
    .expect("streaming upsert");

    let table = schema().tables()[0].clone();
    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    let rows = db.read(&query).expect("read row");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(&table, "title"),
        Some(Value::String("upserted".to_owned()))
    );
    assert_eq!(rows[0].cell(&table, "done"), Some(Value::Bool(true)));
}

#[test]
fn failed_streaming_publication_evicts_the_finalized_staged_root() {
    let db = open_db();
    let row = RowUuid::from_bytes([0x68; 16]);
    jazz::block_on(db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("existing".to_owned())),
            ("done".to_owned(), Value::Bool(false)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .expect("seed row");

    let cells = BTreeMap::from([("done".to_owned(), Value::Bool(true))]);
    let mut upload = db
        .begin_streaming_value_upload("todos", &cells, "title")
        .expect("begin upload");
    jazz::block_on(db.push_streaming_value_upload(&mut upload, b"replacement"))
        .expect("stage root");
    let result = jazz::block_on(db.finish_streaming_value_upload(
        upload,
        StreamingMutationKind::Insert,
        "todos",
        row,
        cells,
        "title",
        None,
        None,
        None,
        None,
        None,
    ));
    let error = match result {
        Ok(_) => panic!("duplicate insert unexpectedly published"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("already exists"));

    db.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 60_000,
        max_age_ms: 0,
    });
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert_eq!(
        jazz::block_on(db.evict_expired_staged_large_values()).expect("expiry pass"),
        0,
        "failed publication removes the finalized staged root immediately"
    );
}

#[test]
fn push_streaming_stops_at_the_ingress_limit_and_closes_the_upload() {
    let db = open_db();
    db.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: 1,
        window_ms: 60_000,
        max_age_ms: 10 * 60 * 1_000,
    });
    let cells = BTreeMap::from([("done".to_owned(), Value::Bool(false))]);
    let mut upload = db
        .begin_streaming_value_upload("todos", &cells, "title")
        .expect("begin upload");

    let error = jazz::block_on(
        db.push_streaming_value_upload(&mut upload, &vec![b'x'; LEAF_MAX_BYTES + 1]),
    )
    .expect_err("the first finalized leaf exceeds the one-byte ingress budget");
    assert!(error.to_string().contains("rate limit"));

    let closed = jazz::block_on(db.push_streaming_value_upload(&mut upload, b"retry"))
        .expect_err("a rejected upload cannot resume after its emitted batch was discarded");
    assert!(closed.to_string().contains("closed"));

    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    assert!(db.read(&query).expect("read rows").is_empty());
}

/// Two local streams establish pending journals, maintenance expires them,
/// and neither stale handle may recreate its journal through push or finish.
///
/// ```text
/// begin/push ──► pending ──expiry maintenance──► absent
/// stale push/finish ──► LargeValueStageExpired; remains absent
/// ```
#[test]
fn maintenance_evicted_local_stream_handles_cannot_recreate_pending_uploads() {
    let db = open_db();
    let cells = BTreeMap::from([("done".to_owned(), Value::Bool(false))]);
    let mut push_upload = db
        .begin_streaming_value_upload("todos", &cells, "title")
        .expect("begin push upload");
    let mut finish_upload = db
        .begin_streaming_value_upload("todos", &cells, "title")
        .expect("begin finish upload");
    jazz::block_on(db.push_streaming_value_upload(&mut push_upload, b"first"))
        .expect("initialize push upload journal");
    jazz::block_on(db.push_streaming_value_upload(&mut finish_upload, b"second"))
        .expect("initialize finish upload journal");

    db.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 60_000,
        max_age_ms: 0,
    });
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert_eq!(
        jazz::block_on(db.evict_expired_staged_large_values()).expect("expiry pass"),
        2
    );

    let push_error = jazz::block_on(db.push_streaming_value_upload(&mut push_upload, b"stale"))
        .expect_err("an evicted stream cannot push");
    assert!(push_error.to_string().contains("expired"));

    let finish_result = jazz::block_on(db.finish_streaming_value_upload(
        finish_upload,
        StreamingMutationKind::Insert,
        "todos",
        RowUuid::from_bytes([0x77; 16]),
        cells,
        "title",
        None,
        None,
        None,
        None,
        None,
    ));
    let finish_error = match finish_result {
        Ok(_) => panic!("an evicted stream cannot finish"),
        Err(error) => error,
    };
    assert!(finish_error.to_string().contains("expired"));
    assert_eq!(
        jazz::block_on(db.evict_expired_staged_large_values()).expect("second expiry pass"),
        0,
        "stale operations must not recreate pending journals"
    );
    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    assert!(db.read(&query).expect("read rows").is_empty());
}

#[test]
fn native_reader_streaming_uses_the_managed_ingress_and_cleanup_path() {
    let db = open_db();
    db.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: 1,
        window_ms: 60_000,
        // Keep this test isolated to ingress admission; expiry behavior is
        // exercised independently below.
        max_age_ms: 10 * 60 * 1_000,
    });

    let result = jazz::block_on(db.insert_streaming_value(
        "todos",
        BTreeMap::from([("done".to_owned(), Value::Bool(false))]),
        "title",
        Cursor::new(vec![b'x'; LEAF_MAX_BYTES + 1]),
    ));
    let error = match result {
        Ok(_) => panic!("native readers must pass through incremental upload admission"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("rate limit"));

    assert_eq!(
        jazz::block_on(db.evict_expired_staged_large_values()).expect("expiry pass"),
        0,
        "a rejected native upload leaves no second-path pending claim"
    );
    let query = db.prepare_query(&db.table("todos")).expect("prepare query");
    assert!(db.read(&query).expect("read rows").is_empty());
}

#[test]
fn native_reader_failure_releases_its_pending_upload() {
    let db = open_db();
    let result = jazz::block_on(db.insert_streaming_value(
        "todos",
        BTreeMap::from([("done".to_owned(), Value::Bool(false))]),
        "title",
        FailingReader {
            cursor: Cursor::new(vec![b'x'; LEAF_MAX_BYTES + 1]),
        },
    ));
    assert!(result.is_err());

    db.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 60_000,
        max_age_ms: 0,
    });
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert_eq!(
        jazz::block_on(db.evict_expired_staged_large_values()).expect("expiry pass"),
        0,
        "reader failure immediately removes its pending claim"
    );
}

#[test]
fn explicit_streaming_abort_releases_the_pending_upload_immediately() {
    let db = open_db();
    let cells = BTreeMap::from([("done".to_owned(), Value::Bool(false))]);
    let mut upload = db
        .begin_streaming_value_upload("todos", &cells, "title")
        .expect("begin upload");
    jazz::block_on(db.push_streaming_value_upload(&mut upload, &vec![b'x'; LEAF_MAX_BYTES + 1]))
        .expect("persist a pending upload");

    jazz::block_on(db.abort_streaming_value_upload(upload)).expect("abort upload");
    db.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 60_000,
        max_age_ms: 0,
    });
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert_eq!(
        jazz::block_on(db.evict_expired_staged_large_values()).expect("expiry pass"),
        0,
        "abort already removed the persisted pending claim"
    );
}
