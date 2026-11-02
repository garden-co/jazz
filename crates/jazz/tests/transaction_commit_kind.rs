//! A handle's commit API must preserve the isolation chosen when it opened.

use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, UpdateOptions, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;

fn open() -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::for_test_bytes([1; 16]),
        },
    )))
    .unwrap()
}

fn cells(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".into(), Value::String(text.into()))])
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn bodies(db: &Db) -> Vec<String> {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    db.read(&prepared)
        .unwrap()
        .into_iter()
        .map(|row| match row.cell("body") {
            Some(Value::String(body)) => body,
            other => panic!("expected text, got {other:?}"),
        })
        .collect()
}

/// Alice reads a row, stages a dependent write, then Bob changes her read.
/// Selecting mergeable commit must not let Alice escape serializable validation.
/// alice: begin -> read A -> stage B ---------> wrong commit -> refused
/// bob:                             update A
#[test]
fn a_wrong_commit_api_cannot_bypass_a_serializable_read_conflict() {
    let db = open();
    block_on(db.insert(
        "notes",
        cells("before"),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    let id = OpenTransactionId::new();
    block_on(db.begin_serializable(id)).unwrap();
    let query = db
        .prepare_query(&Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(row(1).0)))))
        .unwrap();
    let observed = block_on(db.tx_ref(id).all_prepared(&query)).unwrap();
    assert_eq!(
        observed[0].cell("body"),
        Some(Value::String("before".into()))
    );
    block_on(
        db.tx_ref(id)
            .insert("notes", cells("based on before"), Default::default()),
    )
    .unwrap();
    block_on(db.update(
        "notes",
        row(1),
        cells("bob changed it"),
        UpdateOptions {
            identity: WriteIdentity::Session(AuthorSubject::for_test_bytes([2; 16])),
            ..Default::default()
        },
    ))
    .unwrap();

    let error = block_on(db.commit_mergeable_handle(id)).unwrap_err();
    assert_eq!(error.code, ErrorCode::WriteRejected);
    assert!(error.to_string().contains("not mergeable"));
    assert_eq!(bodies(&db), ["bob changed it"]);
    assert!(block_on(db.commit_serializable_handle(id)).is_err());
}

/// Alice's wrong-kind commit consumes its handle without publishing, in either
/// direction. A fresh handle with the same id can still commit with its own kind.
#[test]
fn wrong_kind_refusal_discards_staging_and_allows_a_fresh_transaction() {
    for serializable in [true, false] {
        let db = open();
        let id = OpenTransactionId::new();
        if serializable {
            block_on(db.begin_serializable(id)).unwrap();
        } else {
            block_on(db.begin_mergeable(id)).unwrap();
        }
        block_on(
            db.tx_ref(id)
                .insert("notes", cells("discarded"), Default::default()),
        )
        .unwrap();
        let refused = if serializable {
            block_on(db.commit_mergeable_handle(id))
        } else {
            block_on(db.commit_serializable_handle(id))
        };
        assert_eq!(refused.unwrap_err().code, ErrorCode::WriteRejected);
        assert!(bodies(&db).is_empty());
        assert!(db.abandon_transaction_handle(id).is_err());

        if serializable {
            block_on(db.begin_serializable(id)).unwrap();
        } else {
            block_on(db.begin_mergeable(id)).unwrap();
        }
        block_on(
            db.tx_ref(id)
                .insert("notes", cells("committed"), Default::default()),
        )
        .unwrap();
        if serializable {
            block_on(db.commit_serializable_handle(id)).unwrap();
        } else {
            block_on(db.commit_mergeable_handle(id)).unwrap();
        }
        assert_eq!(bodies(&db), ["committed"]);
    }
}
