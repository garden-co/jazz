//! The rows a write's policy reads at the authority (SPEC 7, WITH CHECK):
//! committed state overlaid with the unit's other inserts and updates. The
//! protected row reads as committed, so a same-table EXISTS never sees the
//! row it is deciding; the unit's deletes are not overlaid, so a row the
//! unit both inserts and deletes is never evidence.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, OpenTransactionId, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies,
    TableSchemaBuilder,
};
use jazz::tx::DurabilityTier;

use common::compile_schema;

fn me() -> PolicyValue {
    PolicyValue::SessionRef(vec![
        "user".to_owned(),
        "identity".to_owned(),
        "subject".to_owned(),
    ])
}

fn outer(column: &str) -> PolicyValue {
    PolicyValue::SessionRef(vec!["__jazz_outer_row".to_owned(), column.to_owned()])
}

fn cmp(column: &str, value: PolicyValue) -> PolicyExpr {
    PolicyExpr::Cmp {
        column: column.to_owned(),
        op: CmpOp::Eq,
        value,
    }
}

/// `members`: anyone sees the roster; an owner of a band adds members. A
/// new member row names the band and the account, so without the right
/// evidence it would vouch for itself. `bands`: anyone creates one; its
/// creator becomes its first owner through `founders`. `tasks` need a band
/// the writer founded.
fn schema() -> JazzSchema {
    let is_owner = PolicyExpr::Exists {
        table: "members".to_owned(),
        condition: Box::new(PolicyExpr::And(vec![
            cmp("band", outer("band")),
            cmp("account", me()),
            cmp(
                "role",
                PolicyValue::Literal(jazz::tools::Value::Text("owner".into())),
            ),
        ])),
    };
    let founded = PolicyExpr::Exists {
        table: "bands".to_owned(),
        condition: Box::new(PolicyExpr::And(vec![
            cmp("id", outer("band")),
            cmp("founder", me()),
        ])),
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("bands")
                    .column("founder", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(cmp("founder", me()))
                            .with_delete(cmp("founder", me())),
                    ),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .fk_column("band", "bands")
                    .column("account", ColumnType::Text)
                    .column("role", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(is_owner),
                    ),
            )
            .table(
                TableSchemaBuilder::new("tasks")
                    .fk_column("band", "bands")
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(founded),
                    ),
            )
            .build(),
    )
}

fn open(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

fn authority() -> Db {
    block_on(Db::open_history_complete(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn subject(author: AuthorSubject) -> Value {
    Value::String(author.principal_parts().1)
}

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..200 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

fn connect(client: &Db, server: &Db, author: AuthorSubject) {
    let (client_side, server_side) = duplex();
    block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, author);
}

fn count(db: &Db, table: &str) -> usize {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .len()
}

fn options(row: RowUuid) -> InsertOptions {
    InsertOptions {
        row_id: Some(row),
        ..Default::default()
    }
}

/// Commit `writes` as one mergeable unit from `client` and wait for the
/// authority's verdict.
fn commit(
    client: &Db,
    server: &Db,
    writes: impl FnOnce(OpenTransactionId),
) -> Result<(), jazz::db::Error> {
    let batch = OpenTransactionId::new();
    block_on(client.begin_mergeable(batch)).unwrap();
    writes(batch);
    let receipt = block_on(client.commit_mergeable_handle(batch)).unwrap();
    drive(
        &[client, server, client],
        client.wait_for_transaction(receipt.0, DurabilityTier::Global),
    )
    .map(|_| ())
}

#[test]
fn a_new_row_is_not_evidence_for_itself() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let server = authority();
    let band = RowUuid::from_bytes([0x10; 16]);
    block_on(server.insert(
        "bands",
        BTreeMap::from([("founder".to_owned(), subject(alice))]),
        options(band),
    ))
    .unwrap();
    block_on(server.insert(
        "members",
        BTreeMap::from([
            ("band".to_owned(), Value::Uuid(band.0)),
            ("account".to_owned(), subject(alice)),
            ("role".to_owned(), Value::String("owner".into())),
        ]),
        Default::default(),
    ))
    .unwrap();
    let alice_db = open(0xa1, alice);
    let bob_db = open(0xb0, bob);
    connect(&alice_db, &server, alice);
    connect(&bob_db, &server, bob);
    let member = |who: AuthorSubject| {
        BTreeMap::from([
            ("band".to_owned(), Value::Uuid(band.0)),
            ("account".to_owned(), subject(who)),
            ("role".to_owned(), Value::String("owner".into())),
        ])
    };

    // Bob's owner row for alice's band would satisfy its own EXISTS.
    let graft = commit(&bob_db, &server, |batch| {
        block_on(
            bob_db
                .tx_ref(batch)
                .insert("members", member(bob), Default::default()),
        )
        .unwrap();
    });
    assert!(graft.is_err(), "a row never vouches for itself");
    // Nor do two new rows vouch for each other: neither is grounded.
    let pair = commit(&bob_db, &server, |batch| {
        let tx = bob_db.tx_ref(batch);
        block_on(tx.insert("members", member(bob), Default::default())).unwrap();
        block_on(tx.insert("members", member(bob), Default::default())).unwrap();
    });
    assert!(pair.is_err(), "two new rows never vouch for each other");
    // An owner adds him.
    commit(&alice_db, &server, |batch| {
        block_on(
            alice_db
                .tx_ref(batch)
                .insert("members", member(bob), Default::default()),
        )
        .unwrap();
    })
    .expect("an owner adds a member");
    assert_eq!(count(&server, "members"), 2);
}

#[test]
fn a_unit_s_earlier_inserts_are_evidence_and_its_deletes_are_not() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = authority();
    let alice_db = open(0xa1, alice);
    connect(&alice_db, &server, alice);
    let band = |founder: AuthorSubject| BTreeMap::from([("founder".to_owned(), subject(founder))]);
    let task = |band: RowUuid| BTreeMap::from([("band".to_owned(), Value::Uuid(band.0))]);

    // A band and its first task in one unit.
    let first = RowUuid::from_bytes([0x21; 16]);
    commit(&alice_db, &server, |batch| {
        let tx = alice_db.tx_ref(batch);
        block_on(tx.insert("bands", band(alice), options(first))).unwrap();
        block_on(tx.insert("tasks", task(first), Default::default())).unwrap();
    })
    .expect("the unit's band grounds its task");

    // Grounding does not depend on the order the unit names its writes.
    let later = RowUuid::from_bytes([0x23; 16]);
    commit(&alice_db, &server, |batch| {
        let tx = alice_db.tx_ref(batch);
        block_on(tx.insert("tasks", task(later), Default::default())).unwrap();
        block_on(tx.insert("bands", band(alice), options(later))).unwrap();
    })
    .expect("a task named before its band is grounded by it");

    // A band the unit inserts and deletes never grounds anything, whether
    // the delete comes before or after the task.
    for (n, delete_first) in [(0x22, true), (0x24, false)] {
        let gone = RowUuid::from_bytes([n; 16]);
        let fleeting = commit(&alice_db, &server, |batch| {
            let tx = alice_db.tx_ref(batch);
            block_on(tx.insert("bands", band(alice), options(gone))).unwrap();
            if delete_first {
                block_on(tx.delete("bands", gone, Default::default())).unwrap();
            }
            block_on(tx.insert("tasks", task(gone), Default::default())).unwrap();
            if !delete_first {
                block_on(tx.delete("bands", gone, Default::default())).unwrap();
            }
        });
        assert!(fleeting.is_err(), "a row the unit deletes is not evidence");
    }

    // A committed band the unit deletes still reads as committed.
    commit(&alice_db, &server, |batch| {
        let tx = alice_db.tx_ref(batch);
        block_on(tx.delete("bands", first, Default::default())).unwrap();
        block_on(tx.insert("tasks", task(first), Default::default())).unwrap();
    })
    .expect("a deleted committed band is evidence as committed");
    assert_eq!(count(&server, "tasks"), 3);
}

/// A write check reads its policy's rows as of the unit's cut: committed
/// rows, overlaid with the unit's grounded ones, plus the newest history
/// image of rows changed since. Every arm of that read must let SQLite
/// search by the policy's correlated keys; a scan reads every row in the
/// table, other tenants' included, on every check (#4298).
#[test]
fn write_checks_search_the_policy_table_through_the_cut() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = authority();
    let band = RowUuid::from_bytes([0x10; 16]);
    block_on(server.insert(
        "bands",
        BTreeMap::from([("founder".to_owned(), subject(alice))]),
        options(band),
    ))
    .unwrap();
    let owner = RowUuid::from_bytes([0x20; 16]);
    block_on(server.insert(
        "members",
        BTreeMap::from([
            ("band".to_owned(), Value::Uuid(band.0)),
            ("account".to_owned(), subject(alice)),
            ("role".to_owned(), Value::String("owner".into())),
        ]),
        options(owner),
    ))
    .unwrap();
    let joining = RowUuid::from_bytes([0x21; 16]);

    // The first round overlays nothing; later rounds overlay grounded rows.
    for grounded in [vec![], vec![owner]] {
        let plan = server
            .explain_write_check_plan("members", joining, alice, &grounded)
            .unwrap();
        assert!(
            !plan.contains("SCAN") && !plan.contains("CO-ROUTINE"),
            "grounded {grounded:?}: {plan}"
        );
    }
}
