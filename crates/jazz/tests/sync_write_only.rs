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
    block_on(bob.accept_subscriber(down, author(who))).unwrap();
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
    block_on(alice.become_replica());
    let bob = block_on(Db::open(DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([13; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    block_on(bob.set_authority());
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
    block_on(alice.become_replica());
    let bob = block_on(Db::open(DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([23; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    block_on(bob.set_authority());
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

fn local_rows(db: &Db) -> Vec<(RowUuid, String)> {
    let q = db.prepare_query(&Query::from("notes")).unwrap();
    let opts = ReadOpts::for_read_tier(jazz::db::ReadTier::LocalOnly);
    let mut rows = block_on(db.all(&q, opts))
        .unwrap()
        .iter()
        .map(|row| {
            let Value::String(text) = row.cell_at(0).unwrap() else {
                panic!("text");
            };
            (row.row_uuid(), text)
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.0);
    rows
}
fn remote_read(db: &Db, dbs: &[&Db]) -> usize {
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let answer = drive(
        dbs,
        db.all_serialized_query(
            &query,
            ReadOpts::for_read_tier(jazz::db::ReadTier::Remote),
            None,
            None,
            None,
        ),
    )
    .unwrap();
    let jazz::db::SerializedReadResult::Rows(answer) = answer else {
        panic!("flat notes read");
    };
    answer.len()
}
fn insert(db: &Db, n: u8, text: &str) -> u64 {
    block_on(db.insert(
        "notes",
        cells(text),
        jazz::db::InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap()
    .mergeable_tx_id()
}
fn open_as(policies: jazz::tools::TablePolicies, node: u8, who: AuthorSubject) -> Db {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    );
    block_on(Db::open(DbConfig::new(
        schema,
        jazz::db::DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: who,
        },
    )))
    .unwrap()
}
fn write_only() -> jazz::tools::TablePolicies {
    use jazz::tools::{PolicyExpr, TablePolicies};
    TablePolicies::new()
        .with_select(PolicyExpr::False)
        .with_insert(PolicyExpr::True)
        .with_delete(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
}

/// A write-only insert stays with its writer once accepted, with no read
/// subscription, as a cold copy: the next covering remote read, whose answer
/// the writer may not see it in, reconciles it away (INV-SYNC-14).
#[test]
fn an_accepted_write_only_insert_stays_cold_until_a_covering_read() {
    let (alice, bob) = policy_pair(write_only());
    connect(&alice, &bob, "alice");
    let insert = insert(&alice, 1, "founder");
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(insert, DurabilityTier::Global),
    )
    .unwrap();
    pump(&alice, &bob);
    assert_eq!(local_rows(&alice), vec![(row(1), "founder".into())]);
    assert_eq!(remote_read(&alice, &[&alice, &bob]), 0);
    pump(&alice, &bob);
    assert_eq!(local_rows(&alice), vec![]);
    assert_eq!(local_rows(&bob), vec![(row(1), "founder".into())]);
}

/// A live read of the table covers Alice's write-only insert: her result
/// must keep matching the authority's for her (INV-SYNC-14), which never
/// lists the row, so the row is evicted rather than kept as written.
#[test]
fn a_write_only_insert_a_live_read_covers_is_evicted() {
    let (alice, bob) = policy_pair(write_only());
    connect(&alice, &bob, "alice");
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    let mut live = block_on(alice.subscribe(&query, ReadOpts::default())).unwrap();
    pump(&alice, &bob);
    let insert = insert(&alice, 1, "founder");
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(insert, DurabilityTier::Global),
    )
    .unwrap();
    pump(&alice, &bob);
    assert_eq!(local_rows(&alice), vec![]);
    let mut result = Vec::new();
    while let Some(event) = live.try_next_event() {
        if let jazz::db::SubscriptionEvent::Delta {
            reset,
            added,
            removed,
            ..
        } = event
        {
            if reset {
                result.clear();
            }
            result.retain(|row| !removed.iter().any(|gone| gone.row_uuid == *row));
            result.extend(added.iter().map(|row| row.row_uuid()));
        }
    }
    assert_eq!(result, Vec::<RowUuid>::new());
    assert_eq!(local_rows(&bob), vec![(row(1), "founder".into())]);
}

/// Alice's own UPDATE (allowed by its check) moves her row out of her read
/// scope. She holds no live read of it, yet her write took her access away:
/// the row is evicted from her replica, not confirmed as written.
#[test]
fn an_own_update_out_of_read_scope_evicts_the_row_without_a_subscription() {
    use jazz::tools::{CmpOp, PolicyExpr, PolicyValue, TablePolicies};
    let (alice, bob) = policy_pair(
        TablePolicies::new()
            .with_select(PolicyExpr::Cmp {
                column: "text".into(),
                op: CmpOp::Ne,
                value: PolicyValue::Literal(jazz::tools::Value::Text("hidden".into())),
            })
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True),
    );
    connect(&alice, &bob, "alice");
    let insert = insert(&alice, 1, "visible");
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(insert, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(local_rows(&alice), vec![(row(1), "visible".into())]);
    let hide = block_on(alice.update("notes", row(1), cells("hidden"), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    drive(
        &[&alice, &bob],
        alice.wait_for_transaction(hide, DurabilityTier::Global),
    )
    .unwrap();
    pump(&alice, &bob);
    assert_eq!(local_rows(&alice), vec![]);
    assert_eq!(local_rows(&bob), vec![(row(1), "hidden".into())]);
}

/// Alice's write-only insert and Carol's upsert of the same row reach the
/// authority in one tick: Alice's is accepted, then superseded by Carol's,
/// which Alice may not read. Alice's version is no longer the row's, so she
/// is not handed it as current: the row is evicted from her replica.
#[test]
fn a_write_only_insert_superseded_by_another_writer_is_evicted() {
    let alice = open_as(write_only(), 31, author("alice"));
    block_on(alice.become_replica());
    let carol = open_as(write_only(), 32, AuthorSubject::SYSTEM);
    block_on(carol.become_replica());
    let bob = open_as(write_only(), 33, AuthorSubject::SYSTEM);
    block_on(bob.set_authority());
    connect(&alice, &bob, "alice");
    let (up, down) = duplex();
    block_on(carol.connect_upstream(up));
    block_on(bob.accept_subscriber(down, AuthorSubject::SYSTEM)).unwrap();
    let all = [&alice, &carol, &bob];
    for _ in 0..3 {
        for db in all {
            block_on(db.tick()).unwrap();
        }
    }
    let mine = insert(&alice, 1, "alice");
    let theirs = block_on(carol.upsert("notes", row(1), cells("carol"), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    // Both commits reach Bob before he ticks: he accepts Alice's, then
    // Carol's over it.
    for db in all {
        block_on(db.tick()).unwrap();
    }
    drive(
        &all,
        alice.wait_for_transaction(mine, DurabilityTier::Global),
    )
    .unwrap();
    drive(
        &all,
        carol.wait_for_transaction(theirs, DurabilityTier::Global),
    )
    .unwrap();
    for _ in 0..4 {
        for db in all {
            block_on(db.tick()).unwrap();
        }
    }
    assert_eq!(local_rows(&bob), vec![(row(1), "carol".into())]);
    assert_eq!(local_rows(&alice), vec![]);
}

fn relayed(relay: AuthorSubject, server_link: bool) -> (Db, Db, Db) {
    let server = open_as(write_only(), 41, AuthorSubject::SYSTEM);
    block_on(server.set_authority());
    let edge = open_as(write_only(), 42, relay);
    if server_link {
        let (up, down) = duplex();
        block_on(edge.connect_upstream(up));
        block_on(server.accept_subscriber(down, relay)).unwrap();
        block_on(edge.set_remote_link_hint(jazz::db::RemoteLinkHint::Live));
    } else {
        block_on(edge.set_remote_link_hint(jazz::db::RemoteLinkHint::NoServer));
    }
    let client = open_as(write_only(), 43, author("alice"));
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    block_on(edge.accept_subscriber(down, author("alice"))).unwrap();
    block_on(client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live));
    let insert = insert(&client, 1, "founder");
    let tier = if server_link {
        DurabilityTier::Global
    } else {
        DurabilityTier::Local
    };
    drive(
        &[&client, &edge, &server],
        client.wait_for_transaction(insert, tier),
    )
    .unwrap();
    for _ in 0..4 {
        for db in [&client, &edge, &server, &edge] {
            block_on(db.tick()).unwrap();
        }
    }
    block_on(client.tick()).unwrap();
    (client, edge, server)
}

/// Through a relay serving another identity, the relay only passes on the
/// authority's verdict: it cannot confirm the write-only row as written, so
/// the session's row is reconciled as any written row it may not read, and
/// evicted (as before write-only acknowledgements).
#[test]
fn a_write_only_insert_through_another_identitys_relay_is_evicted() {
    let (client, _edge, server) = relayed(AuthorSubject::SYSTEM, true);
    assert_eq!(local_rows(&server), vec![(row(1), "founder".into())]);
    assert_eq!(local_rows(&client), vec![]);
}

/// A relay with no server is the last word on its sessions' commits, but
/// not the global authority: its acceptance confirms no write-only row as
/// written, and it reconciles the row as any written row its session may
/// not read, as before write-only acknowledgements. The writer's replica
/// keeps the write it holds as Local, as it always did on this path.
#[test]
fn a_write_only_insert_a_relay_with_no_server_accepts_stays_local() {
    let (client, edge, _server) = relayed(AuthorSubject::SYSTEM, false);
    assert_eq!(local_rows(&edge), vec![(row(1), "founder".into())]);
    assert_eq!(local_rows(&client), vec![(row(1), "founder".into())]);
}

/// The writer's own store relaying for it (a worker for its tab) is the
/// authority's direct session: it keeps the write-only row as written, and
/// serves its own identity's sessions what it holds.
#[test]
fn a_write_only_insert_through_the_writers_own_relay_stays() {
    let (client, edge, _server) = relayed(author("alice"), true);
    assert_eq!(local_rows(&edge), vec![(row(1), "founder".into())]);
    assert_eq!(local_rows(&client), vec![(row(1), "founder".into())]);
}

/// Alice caches a row she may read; Bob's change then takes her access
/// away while no live read covers it. Her later edit is refused: the
/// refusal sends no eviction (a refusal takes no access away), so her cold
/// copy stays until her next covering remote read reconciles it.
#[test]
fn a_refused_edit_leaves_a_cold_copy_to_the_next_covering_read() {
    use jazz::tools::{CmpOp, PolicyExpr, PolicyValue, TablePolicies};
    let (alice, bob) = policy_pair(
        TablePolicies::new()
            .with_select(PolicyExpr::Cmp {
                column: "text".into(),
                op: CmpOp::Ne,
                value: PolicyValue::Literal(jazz::tools::Value::Text("hidden".into())),
            })
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True),
    );
    block_on(bob.insert(
        "notes",
        cells("visible"),
        jazz::db::InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    ))
    .unwrap();
    connect(&alice, &bob, "alice");
    assert_eq!(remote_read(&alice, &[&alice, &bob]), 1);
    pump(&alice, &bob);
    block_on(bob.update("notes", row(1), cells("hidden"), Default::default())).unwrap();
    pump(&alice, &bob);
    let edit = block_on(alice.update("notes", row(1), cells("edit"), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    assert!(
        drive(
            &[&alice, &bob],
            alice.wait_for_transaction(edit, DurabilityTier::Global)
        )
        .is_err()
    );
    pump(&alice, &bob);
    assert_eq!(local_rows(&alice), vec![(row(1), "visible".into())]);
    assert_eq!(remote_read(&alice, &[&alice, &bob]), 0);
    pump(&alice, &bob);
    assert_eq!(local_rows(&alice), vec![]);
}
