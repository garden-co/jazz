//! A root filter cannot hide changes to another root row used by its read policy.
use crate::common::{compile_schema, exists, read_and_allow_all_writes, session_eq};
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriptionEvent,
    SubscriptionStream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x71; 16])
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn cells(body: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".into(), Value::String(body.into()))])
}

fn open(requires_support: bool, has_support: bool) -> Db {
    let policy = if requires_support {
        exists("notes", vec![session_eq("body", &["claims", "support"])])
    } else {
        PolicyExpr::True
    };
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(read_and_allow_all_writes(policy)),
            )
            .build(),
    );
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x72; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    db.set_identity_claims(
        alice(),
        BTreeMap::from([("\0claims:support".into(), Value::String("support".into()))]),
    );
    db.seed_settled_mergeable_for_bootstrap(
        "notes",
        row(4),
        AuthorSubject::SYSTEM,
        cells("secret"),
    )
    .unwrap();
    if has_support {
        db.seed_settled_mergeable_for_bootstrap(
            "notes",
            row(5),
            AuthorSubject::SYSTEM,
            cells("support"),
        )
        .unwrap();
    }
    db
}

fn query() -> Query {
    Query::from("notes").filter(eq(col("body"), lit(Value::String("secret".into()))))
}

fn content(current: &CurrentRow) -> (RowUuid, String) {
    let value = current
        .cells()
        .find(|(name, _)| *name == "body")
        .expect("body column")
        .1
        .expect("present body");
    let Value::String(body) = value else {
        panic!("text body expected: {value:?}");
    };
    (current.row_uuid(), body.clone())
}

fn drain(stream: &mut SubscriptionStream, rows: &mut BTreeMap<RowUuid, String>) {
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } => {
                if reset {
                    rows.clear();
                }
                for removed in removed {
                    assert!(rows.remove(&removed.row_uuid).is_some(), "visible removal");
                }
                for current in added.iter().chain(updated.iter()) {
                    let (id, body) = content(current);
                    rows.insert(id, body);
                }
            }
            SubscriptionEvent::Rejected { reason } => panic!("subscription rejected: {reason}"),
            _ => {}
        }
    }
}

fn fresh(db: &Db) -> BTreeMap<RowUuid, String> {
    // One eligible root makes limit(2) equivalent while keeping its SQL distinct.
    // One-shot execution uses the same handle, Alice and unchanged registered claims.
    let prepared = db.prepare_query(&query().limit(2)).unwrap();
    block_on(db.all_for_identity(&prepared, ReadOpts::default(), alice()))
        .unwrap()
        .iter()
        .map(content)
        .collect()
}

fn subscribe(db: &Db) -> (SubscriptionStream, BTreeMap<RowUuid, String>) {
    let prepared = db.prepare_query(&query()).unwrap();
    let mut stream =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), alice())).unwrap();
    let mut rows = BTreeMap::new();
    drain(&mut stream, &mut rows);
    assert_eq!(rows, fresh(db));
    (stream, rows)
}

/// Alice watches a filtered note under an already-active self-table policy.
/// authority deletes separate support -> fresh result empties -> live result must empty.
#[test]
fn active_self_table_support_deletion_refreshes_filtered_result() {
    let db = open(true, true);
    let (mut stream, mut rows) = subscribe(&db);
    assert_eq!(rows, BTreeMap::from([(row(4), "secret".into())]));

    block_on(db.delete("notes", row(5), Default::default())).unwrap();
    block_on(db.tick()).unwrap();
    let expected = fresh(&db);
    assert!(
        expected.is_empty(),
        "deleting support revokes the fresh result"
    );
    drain(&mut stream, &mut rows);
    assert_eq!(rows, expected, "support deletion must revoke the live root");
}

/// Alice's filtered result starts empty under an already-active self-table policy.
/// authority inserts separate support -> fresh result admits note -> live result must admit it.
#[test]
fn active_self_table_support_insertion_refreshes_empty_result() {
    let db = open(true, false);
    let (mut stream, mut rows) = subscribe(&db);
    assert!(rows.is_empty());

    block_on(db.insert(
        "notes",
        cells("support"),
        InsertOptions {
            row_id: Some(row(5)),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(db.tick()).unwrap();
    let expected = fresh(&db);
    assert_eq!(expected, BTreeMap::from([(row(4), "secret".into())]));
    drain(&mut stream, &mut rows);
    assert_eq!(rows, expected, "support insertion must admit the live root");
}

/// Alice's ordinary public root filter still skips an unrelated row's update.
/// Updating the matching row afterwards proves the retained keyed watch remains live.
#[test]
fn ordinary_root_key_skips_unrelated_rows() {
    let db = open(false, true);
    let (mut stream, mut rows) = subscribe(&db);
    assert_eq!(rows, BTreeMap::from([(row(4), "secret".into())]));
    let before = db.fanout_stats();
    block_on(db.update(
        "notes",
        row(5),
        cells("still unrelated"),
        Default::default(),
    ))
    .unwrap();
    block_on(db.tick()).unwrap();
    drain(&mut stream, &mut rows);
    assert_eq!(rows, fresh(&db));
    assert_eq!(
        db.fanout_stats().woken,
        before.woken,
        "unrelated key stays asleep"
    );

    block_on(db.update("notes", row(4), cells("moved"), Default::default())).unwrap();
    block_on(db.tick()).unwrap();
    let expected = fresh(&db);
    assert!(expected.is_empty());
    drain(&mut stream, &mut rows);
    assert_eq!(rows, expected);
    assert!(
        db.fanout_stats().woken > before.woken,
        "matching key still wakes"
    );
}
