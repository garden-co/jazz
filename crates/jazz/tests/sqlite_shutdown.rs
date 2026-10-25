//! Owner shutdown closes application mutation admission before draining the
//! thread-affine runtime. Cleanup and real authority receipts remain usable.
mod common;

use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ErrorCode, ReadOpts, ReadTier, StreamingMutationKind,
    WriteIdentity, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate};
use std::collections::BTreeMap;

fn schema() -> JazzSchema {
    common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .nullable_column("payload", ColumnType::Bytea)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    )
}
fn identity(node: u8, author: AuthorSubject) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author,
    }
}
fn cells(title: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".into(), Value::String(title.into()))])
}
fn closed<T>(result: Result<T, Error>) {
    let Err(error) = result else {
        panic!("closing owner admitted a mutation")
    };
    assert_eq!(error.code, ErrorCode::Storage);
    assert_eq!(error.message, "database is closing or closed");
}
fn count(db: &Db) -> usize {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    block_on(db.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
        .unwrap()
        .len()
}

/// Alice has one pending write, retained schema/transaction handles and an
/// upload. Shutdown refuses new writes through all of them, but lets the real
/// authority settle the existing write and lets Alice release upload storage.
#[test]
fn shutdown_fences_retained_handles_while_receipts_and_cleanup_drain() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = block_on(Db::open(DbConfig::new(schema(), identity(1, alice)))).unwrap();
    let authority = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        identity(2, AuthorSubject::SYSTEM),
    )))
    .unwrap();
    let (up, down) = duplex();
    block_on(db.connect_upstream(up));
    authority.accept_subscriber(down, alice);
    let write =
        block_on(db.insert("notes", cells("already committed"), Default::default())).unwrap();
    let row = write.row_uuid();
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    block_on(
        db.serializable_tx_ref(tx)
            .insert("notes", cells("uncommitted"), Default::default()),
    )
    .unwrap();
    let view = block_on(db.register_schema_view(schema())).unwrap();
    let mut upload = db
        .begin_streaming_value_upload("notes", &cells("stream"), "payload")
        .unwrap();
    block_on(db.push_streaming_value_upload(&mut upload, &vec![7; 100_000])).unwrap();
    let mut staged_upload = db
        .begin_streaming_value_upload("notes", &cells("staged"), "payload")
        .unwrap();
    block_on(db.push_streaming_value_upload(&mut staged_upload, &vec![8; 100_000])).unwrap();
    let mut staged = block_on(db.stage_streaming_value_upload(
        staged_upload,
        StreamingMutationKind::Insert,
        "notes",
        RowUuid::from_bytes([8; 16]),
        cells("staged"),
        "payload",
        WriteIdentity::Database,
        None,
        None,
        None,
    ))
    .unwrap();
    assert_eq!(
        db.write_state(write.mergeable_tx_id()).unwrap().fate,
        Fate::Pending
    );

    db.stop_mutation_admission();
    db.stop_mutation_admission(); // idempotent, including a close retry
    closed(block_on(db.insert(
        "notes",
        cells("late"),
        Default::default(),
    )));
    closed(block_on(view.insert(
        "notes",
        cells("late view"),
        Default::default(),
    )));
    closed(block_on(db.update(
        "notes",
        row,
        cells("late patch"),
        Default::default(),
    )));
    closed(block_on(db.upsert(
        "notes",
        row,
        cells("late upsert"),
        Default::default(),
    )));
    closed(block_on(db.delete("notes", row, Default::default())));
    closed(block_on(db.restore(
        "notes",
        row,
        Some(cells("late restore")),
        Default::default(),
    )));
    closed(block_on(db.begin_mergeable(OpenTransactionId::new())));
    closed(block_on(db.begin_serializable(OpenTransactionId::new())));
    closed(block_on(db.serializable_tx_ref(tx).insert(
        "notes",
        cells("late staged"),
        Default::default(),
    )));
    closed(db.begin_streaming_value_upload("notes", &cells("late stream"), "payload"));
    closed(block_on(
        db.push_streaming_value_upload(&mut upload, b"late"),
    ));
    closed(block_on(db.attach_staged_streaming_value(tx, &mut staged)));
    assert!(
        !block_on(db.abort_staged_streaming_value(&mut staged)).unwrap(),
        "failed attachment consumed the capability"
    );
    block_on(db.abort_streaming_value_upload(upload)).unwrap();
    closed(block_on(db.commit_serializable_handle(tx)));
    assert_eq!(count(&db), 1, "unpublished writes remain absent");

    for _ in 0..4 {
        block_on(db.tick()).unwrap();
        block_on(authority.tick()).unwrap();
    }
    block_on(db.tick()).unwrap();
    block_on(db.wait_for_transaction(write.mergeable_tx_id(), DurabilityTier::Global)).unwrap();
    assert_eq!(
        db.write_state(write.mergeable_tx_id()).unwrap().fate,
        Fate::Accepted
    );
    assert_eq!(count(&authority), 1);
    db.retire_storage_owner().unwrap();
    closed(block_on(view.insert(
        "notes",
        cells("retired"),
        Default::default(),
    )));
    assert!(
        block_on(view.all(
            &view.prepare_query(&Query::from("notes")).unwrap(),
            ReadOpts::default()
        ))
        .is_err()
    );
}

/// Alice closes the owner while her callback transaction is suspended. The
/// callback's earlier writes roll back even if it subsequently returns success.
#[test]
fn shutdown_during_a_callback_prevents_publication() {
    let db = block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        identity(3, AuthorSubject::SYSTEM),
    )))
    .unwrap();
    let result = block_on(db.transaction(async |tx| {
        tx.insert("notes", cells("before closing"), Default::default())
            .await?;
        db.stop_mutation_admission();
        Ok(())
    }));
    closed(result);
    assert_eq!(count(&db), 0);
    block_on(db.close()).unwrap();
}

/// Alice's retained initialization seal cannot publish after close admission;
/// cancelling another seal remains valid and creates no rows.
#[test]
fn shutdown_refuses_initialization_publication_but_allows_cancellation() {
    let db = block_on(Db::open(DbConfig::new(
        schema(),
        identity(4, AuthorSubject::for_test_bytes([0xa1; 16])),
    )))
    .unwrap();
    let mut seals = Vec::new();
    for title in ["publish", "cancel"] {
        let tx = OpenTransactionId::new();
        block_on(db.begin_serializable(tx)).unwrap();
        block_on(
            db.serializable_tx_ref(tx)
                .insert("notes", cells(title), Default::default()),
        )
        .unwrap();
        seals.push(db.seal_initialization_transaction(tx).unwrap());
    }
    let unsealed = OpenTransactionId::new();
    block_on(db.begin_serializable(unsealed)).unwrap();
    db.stop_mutation_admission();
    closed(db.seal_initialization_transaction(unsealed));
    db.cancel_initialization_transaction(seals.pop().unwrap())
        .unwrap();
    closed(db.publish_initialization_transaction(seals.pop().unwrap()));
    assert_eq!(count(&db), 0);
}
