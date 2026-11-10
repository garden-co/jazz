//! Ordinary authenticated write-only acknowledgements are independent of
//! SELECT subscriptions. No initialization journal or E2EE setup is involved.
use super::common;
use jazz::db::{Db, DbConfig, ReadOpts, block_on, duplex};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};
fn author(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", name).unwrap()
}
fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn cells(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("text".into(), Value::String(text.into()))])
}
fn rows(db: &Db) -> Vec<(RowUuid, String)> {
    let q = db.prepare_query(&Query::from("notes")).unwrap();
    let mut rows = block_on(db.all(&q, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            (
                row.row_uuid(),
                match row.cell_at(0).unwrap() {
                    Value::String(s) => s,
                    _ => panic!("text"),
                },
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.0);
    rows
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not settle");
}
fn connect(alice: &Db, bob: &Db, who: &str) {
    let (up, down) = duplex();
    block_on(alice.connect_upstream(up));
    bob.accept_subscriber(down, author(who)).unwrap();
}

// Control uses the ordinary public replica path, without a reservation,
// initialization journal, prepared slot or sealed transaction.
fn ordinary_write_only_case(serializable: bool) {
    use jazz::tools::{PolicyExpr, TablePolicies};
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::False)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::False),
                    ),
            )
            .build(),
    );
    let alice = block_on(Db::open(DbConfig::new(
        schema.clone(),
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([11; 16]),
            author: author("alice"),
        },
    )))
    .unwrap();
    alice.become_replica();
    let bob = block_on(Db::open(DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([13; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    bob.set_authority();
    let first = if serializable {
        let id = OpenTransactionId::new();
        block_on(alice.begin_serializable(id)).unwrap();
        block_on(alice.tx_ref(id).insert(
            "notes",
            cells("founder"),
            jazz::db::InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap();
        block_on(alice.commit_serializable_handle(id)).unwrap().0
    } else {
        block_on(alice.insert(
            "notes",
            cells("founder"),
            jazz::db::InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap()
        .mergeable_tx_id()
    };
    let edit = block_on(alice.update("notes", row(1), cells("rejected edit"), Default::default()))
        .unwrap();
    connect(&alice, &bob, "alice");
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(first, DurabilityTier::Global),
    )
    .unwrap();
    assert!(
        drive(
            &[&alice, &bob],
            alice.wait_for_transaction(edit.mergeable_tx_id(), DurabilityTier::Global)
        )
        .is_err()
    );
    assert_eq!(rows(&alice), vec![(row(1), "founder".into())]);
    assert_eq!(rows(&bob), vec![(row(1), "founder".into())]);
}
/// Alice's ordinary mergeable insert settles; her refused edit must retain
/// the accepted founder on Alice and on Bob's authority.
#[test]
fn ordinary_mergeable_write_only_retains_accepted_founder_after_refused_edit() {
    ordinary_write_only_case(false);
}
/// Alice's ordinary serializable insert follows the same contract without
/// initialization reservation or sealed publication.
#[test]
fn ordinary_serializable_write_only_retains_accepted_founder_after_refused_edit() {
    ordinary_write_only_case(true);
}

fn policy_pair(policies: jazz::tools::TablePolicies) -> (Db, Db) {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    );
    let alice = block_on(Db::open(DbConfig::new(
        schema.clone(),
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([21; 16]),
            author: author("alice"),
        },
    )))
    .unwrap();
    alice.become_replica();
    let bob = block_on(Db::open(DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([23; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    bob.set_authority();
    (alice, bob)
}
fn pump(alice: &Db, bob: &Db) {
    for _ in 0..4 {
        block_on(alice.tick()).unwrap();
        block_on(bob.tick()).unwrap();
    }
    block_on(alice.tick()).unwrap();
}
fn refused_readable_case(initially_cached: bool) {
    use jazz::tools::{PolicyExpr, TablePolicies};
    let (alice, bob) = policy_pair(
        TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::False),
    );
    block_on(bob.insert(
        "notes",
        cells("authority founder"),
        jazz::db::InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    let refused = if initially_cached {
        connect(&alice, &bob, "alice");
        let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
        let initial = drive(
            &[&alice, &bob],
            alice.all_serialized_query(
                &query,
                ReadOpts::for_read_tier(jazz::db::ReadTier::Remote),
                None,
                None,
                None,
            ),
        )
        .unwrap();
        let jazz::db::SerializedReadResult::Rows(initial) = initial else {
            panic!("flat notes read");
        };
        assert_eq!(initial.len(), 1);
        drop(initial);
        // Close the one-shot read; there is no live subscription keeping
        // this row current when the authority later changes it.
        pump(&alice, &bob);
        block_on(bob.update(
            "notes",
            row(1),
            cells("current authority"),
            Default::default(),
        ))
        .unwrap();
        block_on(alice.update("notes", row(1), cells("refused edit"), Default::default())).unwrap()
    } else {
        let duplicate = block_on(alice.insert(
            "notes",
            cells("refused duplicate"),
            jazz::db::InsertOptions {
                row_id: Some(row(1)),
                ..Default::default()
            },
        ))
        .unwrap();
        connect(&alice, &bob, "alice");
        duplicate
    };
    assert!(
        drive(
            &[&alice, &bob],
            alice.wait_for_transaction(refused.mergeable_tx_id(), DurabilityTier::Global)
        )
        .is_err()
    );
    let expected = if initially_cached {
        "current authority"
    } else {
        "authority founder"
    };
    assert_eq!(rows(&alice), vec![(row(1), expected.into())]);
    assert_eq!(rows(&bob), vec![(row(1), expected.into())]);
}
/// Alice holds a cold readable row; Bob changes it before refusing her edit.
/// The refusal refreshes the readable current image rather than stale overlay.
#[test]
fn refusal_refreshes_a_readable_cold_baseline_without_a_live_subscription() {
    refused_readable_case(true);
}
/// Alice's duplicate INSERT is refused; its readable authority row still
/// reaches Alice even though she had no cached copy or live subscription.
#[test]
fn refusal_supplies_a_readable_preexisting_row_after_a_duplicate_insert() {
    refused_readable_case(false);
}
/// Alice queues INSERT -> DELETE -> RESTORE before Bob's single fan-out.
/// All three real Fates accept; her final own image and founder metadata agree
/// with Bob despite SELECT=false and without granting a read subscription.
#[test]
fn write_only_accepted_insert_delete_restore_keeps_the_founder_image() {
    use jazz::tools::{PolicyExpr, TablePolicies};
    let (alice, bob) = policy_pair(
        TablePolicies::new()
            .with_select(PolicyExpr::False)
            .with_insert(PolicyExpr::True)
            .with_delete(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True),
    );
    let insert = block_on(alice.insert(
        "notes",
        cells("founder"),
        jazz::db::InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    let delete = block_on(alice.delete("notes", row(1), Default::default())).unwrap();
    let restore = block_on(alice.restore("notes", row(1), None, Default::default())).unwrap();
    connect(&alice, &bob, "alice");
    for receipt in [insert, delete, restore] {
        drive(
            &[&alice, &bob],
            alice.wait_for_transaction(receipt.mergeable_tx_id(), DurabilityTier::Global),
        )
        .unwrap();
    }
    assert_eq!(rows(&alice), vec![(row(1), "founder".into())]);
    assert_eq!(rows(&bob), vec![(row(1), "founder".into())]);
    let query = Query::from("notes").select(["$createdAt", "$createdBy.identity.subject"]);
    let metadata = |db: &Db| {
        let prepared = db.prepare_query(&query).unwrap();
        let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
        assert_eq!(rows.len(), 1);
        vec![
            rows[0].cell("$createdAt"),
            rows[0].cell("$createdBy.identity.subject"),
        ]
    };
    let client = metadata(&alice);
    let authority = metadata(&bob);
    assert_eq!(client, authority);
    match &client[1] {
        Some(Value::Nullable(Some(subject))) => {
            assert_eq!(**subject, Value::String("alice".into()))
        }
        other => panic!("missing actual founder identity: {other:?}"),
    }
}
