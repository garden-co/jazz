//! Streaming handles retain their original database, schema and target.

use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ErrorCode, ReadOpts, StreamingMutationKind,
    StreamingValueUpload, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::large_values::LEAF_MAX_BYTES;
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;

fn schema(extra: bool) -> JazzSchema {
    let files = TableSchemaBuilder::new("files")
        .nullable_column("body", ColumnType::Text)
        .nullable_column("alternate", ColumnType::Text)
        .policies(allow_all_policies());
    let files = if extra {
        files.nullable_column("note", ColumnType::Text)
    } else {
        files
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(files)
            .table(
                TableSchemaBuilder::new("copies")
                    .nullable_column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open(actor: u8) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(false),
        DbIdentity {
            node: NodeUuid::from_bytes([actor; 16]),
            author: AuthorSubject::for_test_bytes([actor; 16]),
        },
    )))
    .unwrap()
}

fn begin(db: &Db) -> StreamingValueUpload {
    db.begin_streaming_value_upload("files", &BTreeMap::new(), "body")
        .unwrap()
}

fn publish(
    db: &Db,
    upload: StreamingValueUpload,
    id: u8,
    table: &str,
    column: &str,
    cells: BTreeMap<String, Value>,
) -> Result<(), Error> {
    block_on(db.finish_streaming_value_upload(
        upload,
        StreamingMutationKind::Insert,
        table,
        RowUuid::from_bytes([id; 16]),
        cells,
        column,
        WriteIdentity::Database,
        None,
        None,
        None,
    ))
    .map(|_| ())
}

fn bodies(db: &Db, table: &str) -> Vec<String> {
    let query = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(&query, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            let value = row
                .cells()
                .find(|(name, _)| *name == "body")
                .unwrap()
                .1
                .cloned()
                .unwrap();
            match value {
                Value::Nullable(Some(value)) => match *value {
                    Value::String(value) => value,
                    other => panic!("expected text, got {other:?}"),
                },
                Value::String(value) => value,
                other => panic!("expected text, got {other:?}"),
            }
        })
        .collect()
}

fn stage(db: &Db, byte: u8) -> StreamingValueUpload {
    let mut upload = begin(db);
    block_on(db.push_streaming_value_upload(&mut upload, &vec![byte; LEAF_MAX_BYTES + 1])).unwrap();
    upload
}

/// Bob cannot feed Alice's handle into his store; Alice can still finish it.
#[test]
fn a_foreign_push_does_not_poison_the_original_upload() {
    let alice = open(1);
    let bob = open(2);
    let mut upload = begin(&alice);
    let error = block_on(bob.push_streaming_value_upload(&mut upload, b"bob")).unwrap_err();
    assert_eq!(error.code, ErrorCode::Schema);
    block_on(alice.push_streaming_value_upload(&mut upload, b"alice")).unwrap();
    publish(&alice, upload, 1, "files", "body", BTreeMap::new()).unwrap();
    assert_eq!(bodies(&alice, "files"), ["alice"]);
    assert!(bodies(&bob, "files").is_empty());
}

/// Bob's store and Alice's different schema view cannot finish or abort her
/// upload. Consuming a rejected handle queues cleanup only at its real owner.
#[test]
fn foreign_terminal_calls_release_only_the_original_owners_upload() {
    for other_schema in [false, true] {
        for finish in [false, true] {
            let alice = open(1);
            let other = if other_schema {
                block_on(alice.register_schema_view(schema(true))).unwrap()
            } else {
                open(2)
            };
            let upload = stage(&alice, b'a');
            let error = if finish {
                publish(&other, upload, 1, "files", "body", BTreeMap::new()).unwrap_err()
            } else {
                block_on(other.abort_streaming_value_upload(upload)).unwrap_err()
            };
            assert_eq!(error.code, ErrorCode::Schema);
            block_on(alice.tick()).unwrap();
            assert!(bodies(&alice, "files").is_empty());
            assert!(bodies(&other, "files").is_empty());
            // Staged chunks are deliberately invisible to row queries, so use
            // the existing inventory diagnostic to detect a leaked lease.
            assert_eq!(alice.large_value_nodes_for_test().unwrap(), (0, 0));
            assert_eq!(other.large_value_nodes_for_test().unwrap(), (0, 0));
        }
    }
}

/// Alice's schema change cannot reinterpret an upload; another handle for the
/// exact original schema can continue and publish it on her shared owner.
#[test]
fn an_upload_is_bound_to_its_schema_but_not_one_handle_instance() {
    let alice = open(1);
    let same = block_on(alice.register_schema_view(schema(false))).unwrap();
    let changed = block_on(alice.register_schema_view(schema(true))).unwrap();
    let mut upload = begin(&alice);
    let error = block_on(changed.push_streaming_value_upload(&mut upload, b"changed")).unwrap_err();
    assert_eq!(error.code, ErrorCode::Schema);
    block_on(same.push_streaming_value_upload(&mut upload, b"same schema")).unwrap();
    publish(&same, upload, 1, "files", "body", BTreeMap::new()).unwrap();
    assert_eq!(bodies(&alice, "files"), ["same schema"]);
}

/// Alice cannot retarget a completed upload to another same-kind table or
/// column. Rejection publishes nothing and does not leave staged nodes behind.
#[test]
fn finishing_cannot_change_the_original_table_or_column() {
    for (table, column) in [("copies", "body"), ("files", "alternate")] {
        let alice = open(1);
        let upload = stage(&alice, b'a');
        let error = publish(&alice, upload, 1, table, column, BTreeMap::new()).unwrap_err();
        assert_eq!(error.code, ErrorCode::Schema);
        block_on(alice.tick()).unwrap();
        assert!(bodies(&alice, "files").is_empty());
        assert!(bodies(&alice, "copies").is_empty());
        // Queries cannot expose abandoned upload bytes: inspect their inventory.
        assert_eq!(alice.large_value_nodes_for_test().unwrap(), (0, 0));
    }
}

/// Alice cannot supply both streamed bytes and a second value for that column.
#[test]
fn finishing_rejects_a_duplicate_streamed_column_in_the_other_cells() {
    let alice = open(1);
    let upload = stage(&alice, b'a');
    let error = publish(
        &alice,
        upload,
        1,
        "files",
        "body",
        BTreeMap::from([("body".to_owned(), Value::String("conflicting".to_owned()))]),
    )
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::WriteRejected);
    block_on(alice.tick()).unwrap();
    assert!(bodies(&alice, "files").is_empty());
    // An unpublished upload has no row whose absence could prove reclamation.
    assert_eq!(alice.large_value_nodes_for_test().unwrap(), (0, 0));
}

/// Alice abandons one stream while another remains live and a third is already
/// committed. The next ordinary tick reclaims only the abandoned stream.
///
/// ```text
/// alice: commit C + stage L,A -> drop A -> tick -> C,L survive
/// ```
#[test]
fn dropping_an_upload_reclaims_it_without_expiring_live_or_committed_values() {
    let alice = open(1);
    let committed = stage(&alice, b'c');
    publish(&alice, committed, 1, "files", "body", BTreeMap::new()).unwrap();
    let live = stage(&alice, b'l');
    let abandoned = stage(&alice, b'a');
    // Row reads cannot reveal leaked staging nodes; the existing diagnostic
    // measures reclamation, while logical reads verify the retained values.
    let before = alice.large_value_nodes_for_test().unwrap().0;
    drop(abandoned);
    block_on(alice.tick()).unwrap();
    assert!(alice.large_value_nodes_for_test().unwrap().0 < before);
    assert_eq!(bodies(&alice, "files"), ["c".repeat(LEAF_MAX_BYTES + 1)]);
    publish(&alice, live, 2, "files", "body", BTreeMap::new()).unwrap();
    block_on(alice.tick()).unwrap();
    let mut values = bodies(&alice, "files");
    values.sort();
    assert_eq!(
        values,
        [
            "c".repeat(LEAF_MAX_BYTES + 1),
            "l".repeat(LEAF_MAX_BYTES + 1)
        ]
    );
    let settled = alice.large_value_nodes_for_test().unwrap();
    block_on(alice.tick()).unwrap();
    assert_eq!(alice.large_value_nodes_for_test().unwrap(), settled);
}

/// Bob holds a real SQLite writer lock while Alice's dropped upload awaits
/// cleanup. Her failed tick must retain the work for retry without losing rows.
///
/// ```text
/// alice: drop upload -> tick fails -> bob unlocks -> tick reclaims upload
/// ```
#[test]
fn failed_cleanup_is_retried_after_the_storage_lock_is_released() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.sqlite");
    let alice = block_on(Db::open_at(
        DbConfig::new(
            schema(false),
            DbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author: AuthorSubject::for_test_bytes([1; 16]),
            },
        ),
        &path,
        jazz::db::FileOptions::default(),
    ))
    .unwrap();
    let committed = stage(&alice, b'c');
    publish(&alice, committed, 1, "files", "body", BTreeMap::new()).unwrap();
    block_on(alice.tick()).unwrap();
    let durable = alice.large_value_nodes_for_test().unwrap();
    let abandoned = stage(&alice, b'a');
    drop(abandoned);
    // A second SQLite connection is only fault injection: the public database
    // API intentionally exposes neither its writer lock nor cleanup queue.
    let bob = rusqlite::Connection::open(&path).unwrap();
    bob.execute_batch("BEGIN IMMEDIATE").unwrap();
    let error = block_on(alice.tick()).unwrap_err();
    assert_eq!(error.code, ErrorCode::Storage);
    assert!(error.message.contains("locked"));
    assert_eq!(bodies(&alice, "files"), ["c".repeat(LEAF_MAX_BYTES + 1)]);
    bob.execute_batch("ROLLBACK").unwrap();
    block_on(alice.tick()).unwrap();
    assert_eq!(alice.large_value_nodes_for_test().unwrap(), durable);
    assert_eq!(bodies(&alice, "files"), ["c".repeat(LEAF_MAX_BYTES + 1)]);
}
