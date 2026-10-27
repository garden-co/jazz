//! The rows a write's policy reads at the authority (SPEC 7, WITH CHECK):
//! committed state overlaid with the unit's other inserts, restores and
//! updates. The protected row reads as committed, so a same-table EXISTS
//! never sees the row it is deciding; the unit's deletes are not overlaid,
//! so a row the unit both inserts and deletes is never evidence. Every
//! write must also hold on the state the whole unit leaves behind
//! (INV-RLS-9).

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

/// `shows` belong to their chief, who may open or close them and hand them
/// on. A `gig` needs an open show its writer is chief of.
fn venue_schema() -> JazzSchema {
    let my_open_show = PolicyExpr::Exists {
        table: "shows".to_owned(),
        condition: Box::new(PolicyExpr::And(vec![
            cmp("id", outer("show")),
            cmp("chief", me()),
            cmp(
                "open",
                PolicyValue::Literal(jazz::tools::Value::Boolean(true)),
            ),
        ])),
    };
    let mine = cmp("chief", me());
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("shows")
                    .column("chief", ColumnType::Text)
                    .column("open", ColumnType::Boolean)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(mine.clone())
                            .with_update(Some(mine.clone()), PolicyExpr::True)
                            .with_delete(mine),
                    ),
            )
            .table(
                TableSchemaBuilder::new("gigs")
                    .fk_column("show", "shows")
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(my_open_show),
                    ),
            )
            .build(),
    )
}

/// An authority and alice's client on the venue schema.
fn venue(alice: AuthorSubject) -> (Db, Db) {
    let server = block_on(Db::open_history_complete(DbConfig::new(
        venue_schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let client = block_on(Db::open(DbConfig::new(
        venue_schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xa1; 16]),
            author: alice,
        },
    )))
    .unwrap();
    connect(&client, &server, alice);
    (server, client)
}

fn show_cells(chief: AuthorSubject, open: bool) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("chief".to_owned(), subject(chief)),
        ("open".to_owned(), Value::Bool(open)),
    ])
}

fn under(show: RowUuid) -> BTreeMap<String, Value> {
    BTreeMap::from([("show".to_owned(), Value::Uuid(show.0))])
}

fn opened(open: bool) -> BTreeMap<String, Value> {
    BTreeMap::from([("open".to_owned(), Value::Bool(open))])
}

/// A unit's updates and restores are evidence for its other writes, like
/// its inserts, whatever order the unit names them in.
///
/// ```text
/// alice ──tx{ gig(closed show) }──────────────────► core ──► Rejected
/// alice ──tx{ gig(show), open show }──────────────► core ──► Accepted
/// alice ──tx{ gig(deleted show) }─────────────────► core ──► Rejected
/// alice ──tx{ gig(show), restore show }───────────► core ──► Accepted
/// ```
#[test]
fn a_unit_s_updates_and_restores_are_evidence() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let (server, alice_db) = venue(alice);
    let (closed, deleted) = (
        RowUuid::from_bytes([0x31; 16]),
        RowUuid::from_bytes([0x32; 16]),
    );
    commit(&alice_db, &server, |batch| {
        let tx = alice_db.tx_ref(batch);
        block_on(tx.insert("shows", show_cells(alice, false), options(closed))).unwrap();
        block_on(tx.insert("shows", show_cells(alice, true), options(deleted))).unwrap();
    })
    .unwrap();
    commit(&alice_db, &server, |batch| {
        block_on(
            alice_db
                .tx_ref(batch)
                .delete("shows", deleted, Default::default()),
        )
        .unwrap();
    })
    .unwrap();

    // Planted negatives: neither show grounds a gig on its own.
    for show in [closed, deleted] {
        let alone = commit(&alice_db, &server, |batch| {
            block_on(
                alice_db
                    .tx_ref(batch)
                    .insert("gigs", under(show), Default::default()),
            )
            .unwrap();
        });
        assert!(alone.is_err(), "a closed or deleted show grounds nothing");
    }

    commit(&alice_db, &server, |batch| {
        let tx = alice_db.tx_ref(batch);
        block_on(tx.insert("gigs", under(closed), Default::default())).unwrap();
        block_on(tx.update("shows", closed, opened(true), Default::default())).unwrap();
    })
    .expect("the unit's update opens the show for its gig");
    commit(&alice_db, &server, |batch| {
        let tx = alice_db.tx_ref(batch);
        block_on(tx.insert("gigs", under(deleted), Default::default())).unwrap();
        block_on(tx.restore("shows", deleted, None, Default::default())).unwrap();
    })
    .expect("the unit's restore brings the show back for its gig");
    assert_eq!(count(&server, "gigs"), 2);
}

/// Every write must also hold on the state the whole unit leaves behind:
/// a write whose evidence the same unit takes away is refused, although
/// it passes against committed state before the rest of the unit lands.
///
/// ```text
/// alice ──tx{ gig(show), close show }─────────────► core ──► Rejected
/// alice ──tx{ gig(show), hand show to bob }───────► core ──► Rejected
/// alice ──tx{ gig(show), rewrite show unchanged }─► core ──► Accepted
/// ```
#[test]
fn a_write_must_hold_on_the_unit_s_post_state() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let (server, alice_db) = venue(alice);
    let show = RowUuid::from_bytes([0x41; 16]);
    commit(&alice_db, &server, |batch| {
        block_on(
            alice_db
                .tx_ref(batch)
                .insert("shows", show_cells(alice, true), options(show)),
        )
        .unwrap();
    })
    .unwrap();

    // An update of the evidence after the gig, under a monotone policy.
    for (taken, cells) in [
        ("closing the show", opened(false)),
        (
            "handing the show on",
            BTreeMap::from([("chief".to_owned(), subject(bob))]),
        ),
    ] {
        let refused = commit(&alice_db, &server, |batch| {
            let tx = alice_db.tx_ref(batch);
            block_on(tx.insert("gigs", under(show), Default::default())).unwrap();
            block_on(tx.update("shows", show, cells, Default::default())).unwrap();
        });
        assert!(refused.is_err(), "{taken} takes the gig's evidence away");
    }
    // Planted positive: rewriting the show without taking anything away.
    commit(&alice_db, &server, |batch| {
        let tx = alice_db.tx_ref(batch);
        block_on(tx.insert("gigs", under(show), Default::default())).unwrap();
        block_on(tx.update("shows", show, opened(true), Default::default())).unwrap();
    })
    .expect("an update that keeps the evidence passes the post-state check");
    assert_eq!(count(&server, "gigs"), 1);
}

/// `folders` nest in a folder their writer owns.
fn folder_schema() -> JazzSchema {
    let in_my_folder = PolicyExpr::Exists {
        table: "folders".to_owned(),
        condition: Box::new(PolicyExpr::And(vec![
            cmp("id", outer("parent")),
            cmp("owner", me()),
        ])),
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("folders")
                    .column("owner", ColumnType::Text)
                    .fk_column("parent", "folders")
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(in_my_folder),
                    ),
            )
            .build(),
    )
}

/// An authority holding alice's root folder, and alice's client.
fn folders(alice: AuthorSubject, root: RowUuid) -> (Db, Db) {
    let server = block_on(Db::open_history_complete(DbConfig::new(
        folder_schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    block_on(server.insert("folders", folder(alice, root), options(root))).unwrap();
    let client = block_on(Db::open(DbConfig::new(
        folder_schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xa1; 16]),
            author: alice,
        },
    )))
    .unwrap();
    connect(&client, &server, alice);
    (server, client)
}

fn folder(owner: AuthorSubject, parent: RowUuid) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("owner".to_owned(), subject(owner)),
        ("parent".to_owned(), Value::Uuid(parent.0)),
    ])
}

fn chain_folder(index: u128) -> RowUuid {
    RowUuid(uuid::Uuid::from_u128(0x0f00_0000 + index))
}

/// Insert `len` folders as one chain under `root`, children first: folder
/// `i` nests in folder `i + 1`, the last in `root`. Each round of the
/// authority's checks grounds one more level.
fn insert_chain(db: &Db, batch: OpenTransactionId, alice: AuthorSubject, len: u128, root: RowUuid) {
    let tx = db.tx_ref(batch);
    for index in 0..len {
        let parent = if index + 1 == len {
            root
        } else {
            chain_folder(index + 1)
        };
        block_on(tx.insert(
            "folders",
            folder(alice, parent),
            options(chain_folder(index)),
        ))
        .unwrap();
    }
}

/// A dependent chain named children first is still accepted, one level
/// per round; an import-sized one is rejected as not supported yet once
/// its checks would read more than the evidence budget of its own rows,
/// instead of being re-checked round after round.
///
/// ```text
/// alice ──tx{ f0(parent: f1), …, f23(parent: root) }─────► core ──► Accepted
/// alice ──tx{ 2000-folder chain, children first }────────► core ──► Rejected (not supported yet)
/// ```
#[test]
fn a_dependent_chain_is_grounded_round_by_round_within_a_budget() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let root = RowUuid::from_bytes([0x50; 16]);
    let (server, alice_db) = folders(alice, root);
    commit(&alice_db, &server, |batch| {
        insert_chain(&alice_db, batch, alice, 24, root)
    })
    .expect("a chain named children first is grounded");
    assert_eq!(count(&server, "folders"), 25);

    let root = RowUuid::from_bytes([0x51; 16]);
    let (server, alice_db) = folders(alice, root);
    let refused = commit(&alice_db, &server, |batch| {
        insert_chain(&alice_db, batch, alice, 2_000, root)
    })
    .expect_err("an import-sized self-dependent chain is over the budget");
    assert!(
        refused.to_string().contains("is not supported yet"),
        "{refused}"
    );
    assert_eq!(count(&server, "folders"), 1);
}

/// The common import shape: thousands of rows whose policy reads their own
/// table, all under a committed parent. Every check passes on committed
/// state in the first round, the policy is monotone and the unit updates
/// nothing, so no post-state pass and no budget applies.
///
/// ```text
/// alice ──tx{ 2000 folders(parent: committed root) }─────► core ──► Accepted
/// ```
#[test]
fn an_import_under_committed_parents_is_accepted() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let root = RowUuid::from_bytes([0x52; 16]);
    let (server, alice_db) = folders(alice, root);
    commit(&alice_db, &server, |batch| {
        let tx = alice_db.tx_ref(batch);
        for index in 0..2_000 {
            block_on(tx.insert("folders", folder(alice, root), options(chain_folder(index))))
                .unwrap();
        }
    })
    .expect("an import under a committed parent is accepted");
    assert_eq!(count(&server, "folders"), 2_001);
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
