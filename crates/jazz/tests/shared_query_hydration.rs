//! Independent expected-state oracle: a shared evaluator cannot validate itself
//! merely by comparing its one-shot and subscription entry points.
use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, LocalUpdates, PreparedQuery, Propagation, ReadOpts,
    SeededRowIdSource, SubscriptionEvent, SubscriptionStream, block_on,
};
use jazz::groove::{records::Value, storage::TestStorage};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, param};
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{Operation, PolicyExpr};
use jazz::tools::{ColumnType, SchemaBuilder, TablePolicies, TableSchemaBuilder};

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn user(n: u8) -> AuthorSubject {
    let id = uuid::Uuid::from_bytes([n; 16]);
    AuthorSubject::for_test_uuid(id).with_account(jazz::account_registry::AccountId(id))
}
// row_input! is the public fixture builder; the low-level Db facade accepts
// native cells rather than the tools client's RowInput.
fn cells(input: std::collections::HashMap<String, jazz::tools::Value>) -> BTreeMap<String, Value> {
    input
        .into_iter()
        .map(|(name, value)| {
            (
                name,
                match value {
                    jazz::tools::Value::Uuid(id) => Value::Uuid(*id.uuid()),
                    jazz::tools::Value::Text(text) => Value::String(text),
                    other => panic!("unexpected fixture cell {other:?}"),
                },
            )
        })
        .collect()
}
fn opts() -> ReadOpts {
    ReadOpts {
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        ..ReadOpts::default()
    }
}
fn open() -> Db<TestStorage> {
    let account = |field| PolicyExpr::eq_session(field, vec!["user".into(), "account".into()]);
    let source = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("groups")
                .column("member", ColumnType::Uuid)
                .policies(TablePolicies::new().with_select(account("member"))),
        )
        .table(
            TableSchemaBuilder::new("documents")
                .column("owner", ColumnType::Uuid)
                .column("bucket", ColumnType::Text)
                .fk_column("group_id", "groups")
                .index_only(["bucket", "owner"])
                .policies(TablePolicies::new().with_select(PolicyExpr::or(vec![
                    account("owner"),
                    PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "group_id".into(),
                        max_depth: None,
                    },
                ]))),
        )
        .build();
    let schema = JazzSchema::new(&source).unwrap();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            TestStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x71; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(71)),
    ))
    .unwrap()
}
fn insert(db: &Db<TestStorage>, n: u8, owner: u8, bucket: &str) {
    block_on(db.insert(
        "documents",
        cells(jazz::row_input!(
            "owner" => jazz::tools::ObjectId::from_uuid(user(owner).test_uuid()),
            "bucket" => bucket,
            "group_id" => jazz::tools::ObjectId::from_uuid(row(1).0)
        )),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}
fn page(db: &Db<TestStorage>, bucket: &str) -> PreparedQuery {
    db.prepare_query_bound(
        &Query::from("documents").filter(eq(col("bucket"), param("bucket"))),
        BTreeMap::from([("bucket".into(), Value::String(bucket.into()))]),
    )
    .unwrap()
}
fn assert_state(
    db: &Db<TestStorage>,
    query: &PreparedQuery,
    identity: u8,
    stream: &mut SubscriptionStream,
    state: &mut BTreeSet<RowUuid>,
    expected: &[u8],
) {
    block_on(db.tick()).unwrap();
    while let Some(event) = stream.try_next_event() {
        let SubscriptionEvent::Delta {
            reset,
            added,
            removed,
            updated,
            ..
        } = event
        else {
            panic!("unexpected stream event: {event:?}");
        };
        if reset {
            state.clear();
        }
        for removed in removed {
            assert!(state.remove(&removed.row_uuid));
        }
        for added in added {
            assert!(state.insert(added.row_uuid()));
        }
        for updated in updated {
            assert!(state.contains(&updated.row_uuid()));
        }
    }
    let expected = expected.iter().copied().map(row).collect::<BTreeSet<_>>();
    assert_eq!(*state, expected, "retained consumer");
    let actual = block_on(db.all_for_identity(query, opts(), user(identity)))
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual, expected,
        "fresh hydration and cleanup of only its own handle"
    );
}

#[test]
fn live_candidates_follow_inserts_moves_deletion_and_inherited_permission_changes() {
    let db = open();
    block_on(db.insert(
        "groups",
        cells(jazz::row_input!("member" => jazz::tools::ObjectId::from_uuid(user(3).test_uuid()))),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    let a = page(&db, "a");
    let b = page(&db, "b");
    let mut alice = block_on(db.subscribe_for_identity(&a, opts(), user(2))).unwrap();
    let mut bob = block_on(db.subscribe_for_identity(&b, opts(), user(3))).unwrap();
    let mut alice_rows = BTreeSet::new();
    let mut bob_rows = BTreeSet::new();
    assert_state(&db, &a, 2, &mut alice, &mut alice_rows, &[]);
    assert_state(&db, &b, 3, &mut bob, &mut bob_rows, &[]);
    insert(&db, 10, 2, "a");
    insert(&db, 11, 4, "a"); // hidden until membership changes
    insert(&db, 12, 4, "b"); // Bob sees through inheritance
    assert_state(&db, &a, 2, &mut alice, &mut alice_rows, &[10]);
    assert_state(&db, &b, 3, &mut bob, &mut bob_rows, &[12]);
    block_on(db.update(
        "documents",
        row(10),
        cells(jazz::row_input!("bucket" => "b")),
        Default::default(),
    ))
    .unwrap();
    assert_state(&db, &a, 2, &mut alice, &mut alice_rows, &[]);
    assert_state(&db, &b, 3, &mut bob, &mut bob_rows, &[10, 12]);
    block_on(db.update(
        "groups",
        row(1),
        cells(jazz::row_input!("member" => jazz::tools::ObjectId::from_uuid(user(2).test_uuid()))),
        Default::default(),
    ))
    .unwrap();
    assert_state(&db, &a, 2, &mut alice, &mut alice_rows, &[11]);
    assert_state(&db, &b, 3, &mut bob, &mut bob_rows, &[]);
    block_on(db.delete("documents", row(11), Default::default())).unwrap();
    assert_state(&db, &a, 2, &mut alice, &mut alice_rows, &[]);
    block_on(db.restore("documents", row(11), None, Default::default())).unwrap();
    assert_state(&db, &a, 2, &mut alice, &mut alice_rows, &[11]);
    block_on(alice.close()).unwrap();
    insert(&db, 13, 3, "b");
    assert_state(&db, &b, 3, &mut bob, &mut bob_rows, &[13]);
    assert_eq!(db.active_groove_subscriptions_for_test(), 1);
    block_on(bob.close()).unwrap();
    assert_eq!(db.active_groove_subscriptions_for_test(), 0);
    block_on(db.close()).unwrap();
}
