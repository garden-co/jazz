//! Independent expected-state oracle: a shared evaluator cannot validate itself
//! merely by comparing its one-shot and subscription entry points.
use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, LocalUpdates, MergeableTxOps, PreparedQuery,
    Propagation, ReadOpts, SeededRowIdSource, SubscriptionEvent, SubscriptionStream, block_on,
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
fn open(history_complete: bool) -> Db<TestStorage> {
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
    let config = DbConfig::new(
        schema,
        TestStorage::new(&refs),
        DbIdentity {
            node: NodeUuid::from_bytes([0x71; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
    .with_id_source(SeededRowIdSource::new(71));
    if history_complete {
        block_on(Db::open_history_complete(config)).unwrap()
    } else {
        block_on(Db::open(config)).unwrap()
    }
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
    let db = open(false);
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

#[test]
fn settled_index_candidates_remain_live_after_promotion_and_newer_ahead_exit() {
    let db = open(true);
    let group = block_on(db.insert(
        "groups",
        cells(jazz::row_input!("member" => jazz::tools::ObjectId::from_uuid(user(2).test_uuid()))),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    db.finalize_local_mergeable_commit_for_test(group.mergeable_tx_id())
        .unwrap();
    let query = page(&db, "a");
    let mut stream = block_on(db.subscribe_for_identity(&query, opts(), user(2))).unwrap();
    let mut state = BTreeSet::new();
    assert_state(&db, &query, 2, &mut stream, &mut state, &[]);
    let write = block_on(db.insert(
        "documents",
        cells(jazz::row_input!(
            "owner" => jazz::tools::ObjectId::from_uuid(user(4).test_uuid()),
            "bucket" => "a",
            "group_id" => jazz::tools::ObjectId::from_uuid(row(1).0)
        )),
        InsertOptions {
            row_id: Some(row(20)),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_state(&db, &query, 2, &mut stream, &mut state, &[20]);
    db.finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
        .unwrap();
    assert_state(&db, &query, 2, &mut stream, &mut state, &[20]);
    let global = ReadOpts {
        tier: jazz::tx::DurabilityTier::Global,
        ..opts()
    };
    let ids = block_on(db.all_for_identity(&query, global.clone(), user(2)))
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![row(20)]);

    let moved = block_on(db.update(
        "documents",
        row(20),
        cells(jazz::row_input!("bucket" => "b")),
        Default::default(),
    ))
    .unwrap();
    // The settled prefix still contains row 20. Its newer Ahead winner must
    // suppress it, not allow the old matching content to reappear locally.
    assert_state(&db, &query, 2, &mut stream, &mut state, &[]);
    db.finalize_local_mergeable_commit_for_test(moved.mergeable_tx_id())
        .unwrap();
    assert_state(&db, &query, 2, &mut stream, &mut state, &[]);
    assert!(
        block_on(db.all_for_identity(&query, global, user(2)))
            .unwrap()
            .is_empty()
    );
    block_on(stream.close()).unwrap();
    block_on(db.close()).unwrap();
}

#[test]
fn first_result_preserves_explicit_public_provenance_and_projected_cells() {
    let db = open(false);
    let write = block_on(db.insert(
        "documents",
        cells(jazz::row_input!(
            "owner" => jazz::tools::ObjectId::from_uuid(user(2).test_uuid()),
            "bucket" => "selected",
            "group_id" => jazz::tools::ObjectId::from_uuid(row(1).0)
        )),
        InsertOptions {
            updated_at_ms: Some(1234),
            ..Default::default()
        },
    ))
    .unwrap();
    let query = db
        .prepare_query(&Query::from("documents").select([
            "bucket",
            "$createdAt",
            "$updatedAt",
            "$createdBy",
            "$updatedBy",
        ]))
        .unwrap();
    let rows = block_on(db.all_for_identity(&query, opts(), user(2))).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), write.row_uuid());
    let (descriptor, raw) = rows[0].encoded_record();
    let record = jazz::groove::records::BorrowedRecord::new(raw, descriptor);
    assert_eq!(
        record
            .get_nullable_string(descriptor.field_index("bucket").expect("selected cell"))
            .unwrap(),
        Some("selected")
    );
    for field in ["$createdAt", "$updatedAt"] {
        assert_eq!(
            record
                .get_u64(descriptor.field_index(field).expect("selected timestamp"))
                .unwrap(),
            1234
        );
    }
    for field in ["$createdBy", "$updatedBy"] {
        assert!(
            rows[0].binding_field_names().contains(&Some(field)),
            "selected author must retain its public publication role"
        );
        assert!(descriptor.field_index(field).is_some());
    }
    let provenance = db.row_provenance(&rows[0]).unwrap().unwrap();
    let writer = AuthorSubject::system_at(NodeUuid::from_bytes([0x71; 16]));
    assert_eq!(provenance.created_by, writer);
    assert_eq!(provenance.updated_by, writer);
    block_on(db.close()).unwrap();
}

#[test]
fn first_result_policy_id_read_keeps_bounded_storage_work() {
    let db = open(true);
    let tx = block_on(db.mergeable_tx()).unwrap();
    for n in 10..90 {
        block_on(tx.insert(
            "documents",
            cells(jazz::row_input!(
                "owner" => jazz::tools::ObjectId::from_uuid(user(2).test_uuid()),
                "bucket" => "a",
                "group_id" => jazz::tools::ObjectId::from_uuid(row(1).0)
            )),
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let committed = block_on(tx.commit()).unwrap();
    db.finalize_local_mergeable_commit_for_test(committed)
        .unwrap();
    let query = db
        .prepare_query(
            &Query::from("documents")
                .filter(eq(col("id"), jazz::query::lit(Value::Uuid(row(40).0)))),
        )
        .unwrap();
    db.reset_storage_read_metrics_for_test();
    let rows = block_on(db.all_for_identity(
        &query,
        ReadOpts {
            tier: jazz::tx::DurabilityTier::Global,
            ..opts()
        },
        user(2),
    ))
    .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![row(40)]
    );
    let metrics = db.take_storage_read_metrics_for_test();
    assert!(
        (1..=4).contains(&metrics.global_current_rows.reads),
        "a one-row policy lookup must not hydrate all 80 rows: {metrics:?}"
    );
    block_on(db.close()).unwrap();
}

/// The public results and retained deltas are the behavioral oracle. The
/// test-only storage counter is needed to distinguish a key intersection from
/// fetching every row in both equality buckets before the graph joins them.
#[test]
fn first_result_intersects_index_keys_before_loading_rows() {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("documents")
                    .column("owner", ColumnType::Uuid)
                    .column("bucket", ColumnType::Text)
                    .index_only(["owner", "bucket"])
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    )
    .unwrap();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open_history_complete(
        DbConfig::new(
            schema,
            TestStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x72; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(72)),
    ))
    .unwrap();
    let tx = block_on(db.mergeable_tx()).unwrap();
    for n in 10..90 {
        block_on(tx.insert(
            "documents",
            cells(jazz::row_input!(
                "owner" => jazz::tools::ObjectId::from_uuid(user(2).test_uuid()),
                "bucket" => "other"
            )),
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    for n in 90..170 {
        block_on(tx.insert(
            "documents",
            cells(jazz::row_input!(
                "owner" => jazz::tools::ObjectId::from_uuid(user(3).test_uuid()),
                "bucket" => "wanted"
            )),
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    block_on(tx.insert(
        "documents",
        cells(jazz::row_input!(
            "owner" => jazz::tools::ObjectId::from_uuid(user(2).test_uuid()),
            "bucket" => "wanted"
        )),
        InsertOptions {
            row_id: Some(row(200)),
            ..Default::default()
        },
    ))
    .unwrap();
    let committed = block_on(tx.commit()).unwrap();
    db.finalize_local_mergeable_commit_for_test(committed)
        .unwrap();
    let query = db
        .prepare_query(
            &Query::from("documents")
                .filter(eq(
                    col("owner"),
                    jazz::query::lit(Value::Uuid(user(2).test_uuid())),
                ))
                .filter(eq(col("bucket"), jazz::query::lit("wanted"))),
        )
        .unwrap();
    db.reset_storage_read_metrics_for_test();
    let rows = block_on(db.all_for_identity(
        &query,
        ReadOpts {
            tier: jazz::tx::DurabilityTier::Global,
            ..opts()
        },
        user(2),
    ))
    .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![row(200)]
    );
    let metrics = db.take_storage_read_metrics_for_test();
    assert!(metrics.global_current_indexes.reads >= 160, "{metrics:?}");
    assert!(metrics.global_current_rows.reads <= 3, "{metrics:?}");

    let mut stream = block_on(db.subscribe_for_identity(&query, opts(), user(2))).unwrap();
    let mut state = BTreeSet::new();
    assert_state(&db, &query, 2, &mut stream, &mut state, &[200]);
    block_on(db.update(
        "documents",
        row(10),
        cells(jazz::row_input!("bucket" => "wanted")),
        Default::default(),
    ))
    .unwrap();
    assert_state(&db, &query, 2, &mut stream, &mut state, &[10, 200]);
    block_on(db.update(
        "documents",
        row(200),
        cells(jazz::row_input!(
            "owner" => jazz::tools::ObjectId::from_uuid(user(3).test_uuid())
        )),
        Default::default(),
    ))
    .unwrap();
    assert_state(&db, &query, 2, &mut stream, &mut state, &[10]);
    block_on(stream.close()).unwrap();
    block_on(db.close()).unwrap();
}

fn point(db: &Db<TestStorage>, n: u8) -> PreparedQuery {
    db.prepare_query(
        &Query::from("documents").filter(eq(col("id"), jazz::query::lit(Value::Uuid(row(n).0)))),
    )
    .unwrap()
}

/// A retained `id == X` subscription on a policy table follows every way the
/// row can enter or leave the reader's view: an inherited grant moving between
/// readers, the row's own deletion and restore, a direct ownership grant, and
/// the deletion of the parent row the inherited grant depends on.
///
/// alice/bob ──subscribe id==10──► documents ──inherits──► groups
///           group member moves / doc delete+restore / owner change / group delete
#[test]
fn policy_id_subscription_follows_grants_deletion_restore_and_parent_deletion() {
    let db = open(false);
    block_on(db.insert(
        "groups",
        cells(jazz::row_input!("member" => jazz::tools::ObjectId::from_uuid(user(3).test_uuid()))),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    insert(&db, 10, 4, "a");
    insert(&db, 11, 4, "a"); // same table, never matched by the point query
    let query = point(&db, 10);
    let mut alice = block_on(db.subscribe_for_identity(&query, opts(), user(2))).unwrap();
    let mut bob = block_on(db.subscribe_for_identity(&query, opts(), user(3))).unwrap();
    let mut alice_rows = BTreeSet::new();
    let mut bob_rows = BTreeSet::new();
    assert_state(&db, &query, 2, &mut alice, &mut alice_rows, &[]);
    assert_state(&db, &query, 3, &mut bob, &mut bob_rows, &[10]);

    block_on(db.update(
        "groups",
        row(1),
        cells(jazz::row_input!("member" => jazz::tools::ObjectId::from_uuid(user(2).test_uuid()))),
        Default::default(),
    ))
    .unwrap();
    assert_state(&db, &query, 2, &mut alice, &mut alice_rows, &[10]);
    assert_state(&db, &query, 3, &mut bob, &mut bob_rows, &[]);

    block_on(db.delete("documents", row(10), Default::default())).unwrap();
    assert_state(&db, &query, 2, &mut alice, &mut alice_rows, &[]);
    block_on(db.restore("documents", row(10), None, Default::default())).unwrap();
    assert_state(&db, &query, 2, &mut alice, &mut alice_rows, &[10]);

    block_on(db.update(
        "documents",
        row(10),
        cells(jazz::row_input!("owner" => jazz::tools::ObjectId::from_uuid(user(3).test_uuid()))),
        Default::default(),
    ))
    .unwrap();
    assert_state(&db, &query, 2, &mut alice, &mut alice_rows, &[10]);
    assert_state(&db, &query, 3, &mut bob, &mut bob_rows, &[10]);

    // Deleting the parent removes only the inherited grant.
    block_on(db.delete("groups", row(1), Default::default())).unwrap();
    assert_state(&db, &query, 2, &mut alice, &mut alice_rows, &[]);
    assert_state(&db, &query, 3, &mut bob, &mut bob_rows, &[10]);
    block_on(db.restore("groups", row(1), None, Default::default())).unwrap();
    assert_state(&db, &query, 2, &mut alice, &mut alice_rows, &[10]);

    block_on(alice.close()).unwrap();
    block_on(bob.close()).unwrap();
    block_on(db.close()).unwrap();
}

/// Retained hydration of `id == X` on a policy table must read the one row,
/// not the whole table. The storage counter is test-only because the public
/// result is identical either way; only the work differs (#3511).
#[test]
fn retained_policy_id_subscription_keeps_bounded_storage_work() {
    let db = open(true);
    let tx = block_on(db.mergeable_tx()).unwrap();
    for n in 10..90 {
        block_on(tx.insert(
            "documents",
            cells(jazz::row_input!(
                "owner" => jazz::tools::ObjectId::from_uuid(user(2).test_uuid()),
                "bucket" => "a",
                "group_id" => jazz::tools::ObjectId::from_uuid(row(1).0)
            )),
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let committed = block_on(tx.commit()).unwrap();
    db.finalize_local_mergeable_commit_for_test(committed)
        .unwrap();
    let query = point(&db, 40);
    db.reset_storage_read_metrics_for_test();
    let mut stream = block_on(db.subscribe_for_identity(&query, opts(), user(2))).unwrap();
    let mut rows = BTreeSet::new();
    block_on(db.tick()).unwrap();
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta { added, .. } = event {
            rows.extend(added.into_iter().map(|row| row.row_uuid()));
        }
    }
    assert_eq!(rows, BTreeSet::from([row(40)]));
    let metrics = db.take_storage_read_metrics_for_test();
    assert!(
        (1..=4).contains(&metrics.global_current_rows.reads),
        "a retained one-row policy subscription must not hydrate all 80 rows: {metrics:?}"
    );
    block_on(stream.close()).unwrap();
    block_on(db.close()).unwrap();
}
