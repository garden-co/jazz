//! Rust callback and handle facades use the same staged transaction machinery.
use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{Db, DbConfig, DbIdentity, Error, ErrorCode, InsertOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::{
    collections::BTreeMap,
    future::Future,
    task::{Context, Poll, Waker},
};

fn open() -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}
fn body(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".into(), Value::String(text.into()))])
}

/// Alice writes through both the callback handle and surrounding Db; both
/// reads see the staged rows and both write handles name the final commit.
#[test]
fn callback_reads_and_receipts_share_one_batch() {
    let alice = open();
    let query = alice.prepare_query(&alice.table("docs")).unwrap();
    let ((a, b), receipt) = block_on(alice.transaction(async |tx| {
        let a = tx.insert("docs", body("a"), Default::default()).await?;
        let b = alice.insert("docs", body("b"), Default::default()).await?;
        assert_eq!(tx.all_prepared(&query).await?.len(), 2);
        assert_eq!(alice.read(&query)?.len(), 2);
        Ok((a, b))
    }))
    .unwrap();
    assert_eq!(a.mergeable_tx_id(), receipt.0);
    assert_eq!(b.mergeable_tx_id(), receipt.0);
    assert_eq!(alice.read(&query).unwrap().len(), 2);
}

/// Alice cancels a suspended callback after staging a write. Its batch is
/// abandoned and another callback can commit on the same database.
#[test]
fn cancelling_a_callback_abandons_its_batch() {
    let alice = open();
    let query = alice.prepare_query(&alice.table("docs")).unwrap();
    let mut pending = Box::pin(alice.transaction(async |tx| {
        tx.insert("docs", body("cancelled"), Default::default())
            .await?;
        std::future::pending::<()>().await;
        Ok(())
    }));
    assert!(matches!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    drop(pending);
    assert!(alice.read(&query).unwrap().is_empty());
    block_on(
        alice.transaction(async |tx| tx.insert("docs", body("kept"), Default::default()).await),
    )
    .unwrap();
    assert_eq!(
        alice.read(&query).unwrap()[0].cell("body"),
        Some(Value::String("kept".into()))
    );
}

/// Alice catches a rejected nested callback and a duplicate insert, then
/// commits her valid staged row; neither error abandons the parent batch.
#[test]
fn caught_errors_leave_the_parent_batch_usable() {
    let alice = open();
    let query = alice.prepare_query(&alice.table("docs")).unwrap();
    block_on(alice.transaction(async |tx| {
        let row = tx
            .insert("docs", body("kept"), Default::default())
            .await?
            .row_uuid();
        assert!(alice.transaction(async |_| Ok(())).await.is_err());
        assert!(
            tx.insert(
                "docs",
                body("duplicate"),
                InsertOptions {
                    row_id: Some(row),
                    ..Default::default()
                }
            )
            .await
            .is_err()
        );
        assert_eq!(tx.all_prepared(&query).await?.len(), 1);
        Ok(())
    }))
    .unwrap();
    assert_eq!(
        alice.read(&query).unwrap()[0].cell("body"),
        Some(Value::String("kept".into()))
    );
}

/// Alice edits a large value twice through the surrounding Db and reads the
/// second edit before rejecting the callback. No edit survives its rollback.
#[test]
fn large_edits_use_the_callback_overlay() {
    let alice = open();
    let text = "a".repeat(jazz::large_values::LEAF_MAX_BYTES + 1);
    let row = block_on(alice.insert("docs", body(&text), Default::default()))
        .unwrap()
        .row_uuid();
    let result = block_on(alice.transaction(async |_| {
        alice
            .append_value("docs", row, "body", b"b".to_vec())
            .await?;
        alice
            .append_value("docs", row, "body", b"c".to_vec())
            .await?;
        assert_eq!(
            alice
                .read_value_range(
                    "docs",
                    row,
                    "body",
                    text.len() as u64..text.len() as u64 + 2
                )
                .await?,
            b"bc"
        );
        Err::<(), _>(Error::new(ErrorCode::Query, "rollback"))
    }));
    assert!(result.is_err());
    let query = alice.prepare_query(&alice.table("docs")).unwrap();
    assert_eq!(
        alice.read(&query).unwrap()[0].cell("body"),
        Some(Value::String(text))
    );
}

/// Alice suspends between two staged writes and resumes after Bob signals her.
/// Both writes remain readable in the callback and commit under one receipt.
#[test]
fn a_callback_resumes_into_the_same_batch() {
    let alice = open();
    let query = alice.prepare_query(&alice.table("docs")).unwrap();
    let (bob, resume) = futures::channel::oneshot::channel();
    let mut pending = Box::pin(alice.transaction(async |tx| {
        let a = tx.insert("docs", body("a"), Default::default()).await?;
        resume.await.unwrap();
        let b = tx.insert("docs", body("b"), Default::default()).await?;
        assert_eq!(tx.all_prepared(&query).await?.len(), 2);
        Ok((a, b))
    }));
    assert!(matches!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    bob.send(()).unwrap();
    let ((a, b), receipt) = block_on(pending).unwrap();
    assert_eq!(a.mergeable_tx_id(), receipt.0);
    assert_eq!(b.mergeable_tx_id(), receipt.0);
    assert_eq!(alice.read(&query).unwrap().len(), 2);
}

/// Alice streams a value through the surrounding Db inside her callback.
/// The batch retains its bytes until commit and returns the commit's receipt.
#[test]
fn a_streamed_callback_write_keeps_its_bytes_until_commit() {
    let alice = open();
    let text = "s".repeat(jazz::large_values::LEAF_MAX_BYTES + 1);
    let query = alice.prepare_query(&alice.table("docs")).unwrap();
    let (write, receipt) = block_on(alice.transaction(async |tx| {
        let write = alice
            .insert_streaming_value("docs", BTreeMap::new(), "body", text.as_bytes())
            .await?;
        assert_eq!(
            tx.all_prepared(&query).await?[0].cell("body"),
            Some(Value::String(text.clone()))
        );
        Ok(write)
    }))
    .unwrap();
    assert_eq!(write.mergeable_tx_id(), receipt.0);
    assert_eq!(
        alice.read(&query).unwrap()[0].cell("body"),
        Some(Value::String(text))
    );
}
