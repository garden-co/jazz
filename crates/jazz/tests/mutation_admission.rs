//! Host shutdown fences application mutations while normal sync can drain.

use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ErrorCode, FileOptions, LargeValueUpdate,
    LargeValueUpdatePage, LargeValueUpdateSplice, ReadOpts, ReadTier, StreamingMutationKind,
    WriteIdentity, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::large_values::LEAF_MAX_BYTES;
use jazz::protocol::BranchSelector;
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

fn schema(extra: bool) -> JazzSchema {
    let docs = TableSchemaBuilder::new("docs")
        .column("body", ColumnType::Text)
        .policies(allow_all_policies());
    let docs = if extra {
        docs.nullable_column("note", ColumnType::Text)
    } else {
        docs
    };
    compile_schema(&SchemaBuilder::new().table(docs).build())
}

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    DbConfig::new(
        schema(false),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

fn open(node: u8) -> Db {
    block_on(Db::open(config(node, AuthorSubject::SYSTEM))).unwrap()
}

fn body(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".to_owned(), Value::String(text.to_owned()))])
}

fn insert(db: &Db, text: &str) -> RowUuid {
    block_on(db.insert("docs", body(text), Default::default()))
        .unwrap()
        .row_uuid()
}

fn values(db: &Db) -> Vec<Value> {
    let query = db.prepare_query(&Query::from("docs")).unwrap();
    block_on(db.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
        .unwrap()
        .iter()
        .map(|row| row.cell("body").unwrap())
        .collect()
}

fn closing<T>(result: Result<T, Error>) {
    let Err(error) = result else {
        panic!("mutation was admitted after the shutdown fence");
    };
    assert_eq!(error.code, ErrorCode::Storage);
    assert!(error.message.contains("closing or closed"));
}

fn drive<T>(alice: &Db, bob: &Db, future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..100 {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        block_on(alice.tick()).unwrap();
        block_on(bob.tick()).unwrap();
    }
    panic!("sync operation did not finish within 100 turns");
}

fn splice() -> Vec<LargeValueUpdate> {
    vec![LargeValueUpdate::Splice {
        column: "body".to_owned(),
        within: LargeValueUpdatePage::TextUtf8 { from: 0, to: 1 },
        splices: vec![LargeValueUpdateSplice {
            at: 0,
            delete: 1,
            insert: b"x".to_vec(),
        }],
    }]
}

/// Alice closes through one schema view. All retained views reject row writes,
/// including restoration, while reads and Bob's independent owner still work.
#[test]
fn every_schema_view_shares_the_row_mutation_fence() {
    let alice = open(1);
    let same = block_on(alice.register_schema_view(schema(false))).unwrap();
    let changed = block_on(alice.register_schema_view(schema(true))).unwrap();
    let live = insert(&alice, "kept");
    let deleted = insert(&alice, "deleted");
    block_on(alice.delete("docs", deleted, Default::default())).unwrap();
    changed.stop_mutation_admission();
    alice.stop_mutation_admission(); // Repeated host close requests are harmless.
    for view in [&alice, &same, &changed] {
        closing(block_on(view.insert(
            "docs",
            body("new"),
            Default::default(),
        )));
        closing(block_on(view.update(
            "docs",
            live,
            body("changed"),
            Default::default(),
        )));
        closing(block_on(view.upsert(
            "docs",
            live,
            body("upserted"),
            Default::default(),
        )));
        closing(block_on(view.delete("docs", live, Default::default())));
        closing(block_on(view.restore(
            "docs",
            deleted,
            None,
            Default::default(),
        )));
        assert_eq!(values(view), [Value::String("kept".to_owned())]);
    }
    let bob = open(2);
    insert(&bob, "independent");
    assert_eq!(values(&bob), [Value::String("independent".to_owned())]);
}

/// Alice's open mergeable and serializable batches can still read and abandon
/// their staged prefix after close starts, but cannot stage, commit or reopen.
///
/// ```text
/// alice: begin -> stage -> close fence -> read staged -> abandon
///                                  \-> stage/commit/new begin rejected
/// ```
#[test]
fn open_batches_remain_readable_and_abandonable() {
    for serializable in [false, true] {
        let alice = open(1);
        let id = OpenTransactionId::new();
        if serializable {
            block_on(alice.begin_serializable(id)).unwrap();
        } else {
            block_on(alice.begin_mergeable(id)).unwrap();
        }
        let tx = alice.tx_ref(id);
        let row = block_on(tx.insert("docs", body("staged"), Default::default()))
            .unwrap()
            .row_uuid();
        alice.stop_mutation_admission();
        closing(block_on(tx.update(
            "docs",
            row,
            body("late"),
            Default::default(),
        )));
        closing(block_on(tx.update_with_large_value_mutations(
            "docs",
            row,
            BTreeMap::new(),
            splice(),
            Default::default(),
        )));
        let query = alice.prepare_query(&Query::from("docs")).unwrap();
        let rows = block_on(tx.all_prepared(&query)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].cell("body"),
            Some(Value::String("staged".to_owned()))
        );
        let commit = if serializable {
            block_on(alice.commit_serializable_handle(id))
        } else {
            block_on(alice.commit_mergeable_handle(id))
        };
        closing(commit);
        alice.abandon_transaction_handle(id).unwrap();
        closing(block_on(alice.begin_mergeable(OpenTransactionId::new())));
        closing(block_on(alice.begin_serializable(OpenTransactionId::new())));
        assert!(values(&alice).is_empty());
        block_on(alice.tick()).unwrap();
    }
}

/// Alice's separate staged-handle API rejects all further operations, including
/// branch moves, and cannot publish the operations queued before shutdown.
#[test]
fn retained_mergeable_handles_cannot_stage_or_commit() {
    let alice = open(1);
    let tx = block_on(alice.mergeable_tx()).unwrap();
    let row = block_on(tx.insert("docs", body("staged"), Default::default()))
        .unwrap()
        .row_uuid();
    alice.stop_mutation_admission();
    closing(block_on(tx.insert(
        "docs",
        body("late"),
        Default::default(),
    )));
    closing(block_on(tx.update(
        "docs",
        row,
        body("late"),
        Default::default(),
    )));
    closing(block_on(tx.upsert(
        "docs",
        row,
        body("late"),
        Default::default(),
    )));
    closing(block_on(tx.delete("docs", row, Default::default())));
    closing(block_on(tx.restore("docs", row, None, Default::default())));
    let branch = |name: &str| BranchSelector::new([("branch", Value::String(name.to_owned()))]);
    closing(block_on(tx.move_between_branches(
        "docs",
        branch("a"),
        branch("b"),
        row,
    )));
    closing(block_on(tx.commit()));
    closing(block_on(alice.mergeable_tx()));
    closing(block_on(
        alice.mergeable_tx_for_identity(AuthorSubject::SYSTEM),
    ));
    assert!(values(&alice).is_empty());
}

/// Alice's callback transaction must roll back when another retained view starts
/// closing before the callback returns, even if the callback itself succeeds.
///
/// ```text
/// callback: insert -> sibling sets fence -> return Ok -> rollback
/// ```
#[test]
fn a_close_during_a_callback_rolls_back_its_uncommitted_writes() {
    let alice = open(1);
    let sibling = block_on(alice.register_schema_view(schema(false))).unwrap();
    closing(block_on(alice.transaction(async |tx| {
        tx.insert("docs", body("uncommitted"), Default::default())
            .await?;
        sibling.stop_mutation_admission();
        Ok(())
    })));
    assert!(values(&alice).is_empty());
    let called = std::cell::Cell::new(false);
    closing(block_on(alice.transaction(async |_tx| {
        called.set(true);
        Ok(())
    })));
    assert!(!called.get());
    block_on(alice.tick()).unwrap();
}

/// Alice cannot edit large cells, feed or publish existing streams after close,
/// but explicit abort and dropped-handle maintenance still release their bytes.
#[test]
fn closing_blocks_large_value_mutations_but_allows_stream_cleanup() {
    let alice = open(1);
    let row = insert(&alice, "original");
    let stage = |byte: u8| {
        let mut upload = alice
            .begin_streaming_value_upload("docs", &BTreeMap::new(), "body")
            .unwrap();
        block_on(alice.push_streaming_value_upload(&mut upload, &vec![byte; LEAF_MAX_BYTES + 1]))
            .unwrap();
        upload
    };
    let mut aborted = stage(b'a');
    let finished = stage(b'f');
    let dropped = stage(b'd');
    alice.stop_mutation_admission();
    closing(block_on(alice.append_value(
        "docs",
        row,
        "body",
        b"late".to_vec(),
    )));
    closing(block_on(alice.update_with_large_value_mutations(
        "docs",
        row,
        BTreeMap::new(),
        splice(),
        Default::default(),
    )));
    closing(alice.begin_streaming_value_upload("docs", &BTreeMap::new(), "body"));
    closing(block_on(
        alice.push_streaming_value_upload(&mut aborted, b"late"),
    ));
    closing(block_on(alice.finish_streaming_value_upload(
        finished,
        StreamingMutationKind::Insert,
        "docs",
        RowUuid::from_bytes([3; 16]),
        BTreeMap::new(),
        "body",
        WriteIdentity::Database,
        None,
        None,
        None,
    )));
    block_on(alice.abort_streaming_value_upload(aborted)).unwrap();
    drop(dropped);
    block_on(alice.tick()).unwrap();
    block_on(alice.evict_expired_staged_large_values()).unwrap();
    assert_eq!(values(&alice), [Value::String("original".to_owned())]);
    // Staged chunks have no row representation, so the existing inventory
    // diagnostic is needed to prove abort/drop cleanup did not leak them.
    assert_eq!(alice.large_value_nodes_for_test().unwrap(), (0, 0));
}

/// Alice fences new writes after committing locally. Bob's authority still
/// accepts that upload and returns a real receipt; remote reads also keep working
/// when both owners have stopped accepting application mutations.
///
/// ```text
/// alice: local commit -> fence -> upload -> bob (fenced) -> receipt -> Global
/// alice: remote read -----------------------------------> bob -> rows
/// ```
#[test]
fn admitted_uploads_and_remote_reads_drain_after_the_fence() {
    let author = AuthorSubject::for_test_bytes([1; 16]);
    let alice = block_on(Db::open(config(1, author))).unwrap();
    let bob = open(2);
    let (client, server) = duplex();
    let _upstream = block_on(alice.connect_upstream(client));
    bob.accept_subscriber(server, author).unwrap();
    let write = block_on(alice.insert("docs", body("committed"), Default::default())).unwrap();
    let mut wait =
        pin!(alice.wait_for_transaction(write.mergeable_tx_id(), DurabilityTier::Global));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(wait.as_mut().poll(&mut cx).is_pending());
    alice.stop_mutation_admission();
    bob.stop_mutation_admission();
    closing(block_on(alice.insert(
        "docs",
        body("late"),
        Default::default(),
    )));
    drive(&alice, &bob, wait.as_mut()).unwrap();
    assert_eq!(values(&bob), [Value::String("committed".to_owned())]);
    let query = alice.prepare_query(&Query::from("docs")).unwrap();
    let rows = drive(
        &alice,
        &bob,
        alice.all(&query, ReadOpts::for_read_tier(ReadTier::Remote)),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell("body"),
        Some(Value::String("committed".to_owned()))
    );
}

/// Alice's one-way runtime fence is not a durable policy: after that owner drops,
/// Bob can reopen the same file, retain its rows and accept new writes.
#[test]
fn a_new_storage_owner_starts_with_mutation_admission_open() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("docs.sqlite");
    let alice = block_on(Db::open_at(
        config(1, AuthorSubject::SYSTEM),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    insert(&alice, "kept");
    alice.stop_mutation_admission();
    closing(block_on(alice.insert(
        "docs",
        body("late"),
        Default::default(),
    )));
    block_on(alice.close()).unwrap();
    drop(alice);
    let bob = block_on(Db::open_at(
        config(2, AuthorSubject::SYSTEM),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    assert_eq!(values(&bob), [Value::String("kept".to_owned())]);
    insert(&bob, "new owner");
    assert_eq!(values(&bob).len(), 2);
}
