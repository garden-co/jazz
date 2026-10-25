//! SQLite same-unit creation permission contracts, through public transactions.
mod common;
use jazz::db::{Db, DbConfig, DbIdentity, Error, ReadOpts, RemoteLinkHint, block_on, duplex};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
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
fn row(id: u128) -> RowUuid {
    RowUuid(uuid::Uuid::from_u128(id))
}
fn outer(column: &str, source: &str) -> PolicyExpr {
    PolicyExpr::eq_session(column, vec!["__jazz_outer_row".into(), source.into()])
}
fn parent(marked: bool) -> PolicyExpr {
    let condition = PolicyExpr::and(vec![outer("id", "parent"), outer("scope", "scope")]);
    if marked {
        PolicyExpr::exists_including_created("nodes", condition)
    } else {
        PolicyExpr::Exists {
            table: "nodes".into(),
            condition: Box::new(condition),
        }
    }
}
fn insert(dependent: PolicyExpr) -> PolicyExpr {
    PolicyExpr::and(vec![
        PolicyExpr::eq_literal("allowed", jazz::tools::Value::Boolean(true)),
        PolicyExpr::or(vec![
            PolicyExpr::eq_literal("seed", jazz::tools::Value::Boolean(true)),
            dependent,
        ]),
    ])
}
fn schema(policy: PolicyExpr) -> jazz::schema::JazzSchema {
    common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("nodes")
                    .fk_column("parent", "nodes")
                    .column("scope", ColumnType::Text)
                    .column("seed", ColumnType::Boolean)
                    .column("allowed", ColumnType::Boolean)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(policy)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    )
}
fn cells(parent: u128, seed: bool, allowed: bool, scope: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("parent".into(), Value::Uuid(row(parent).0)),
        ("scope".into(), Value::String(scope.into())),
        ("seed".into(), Value::Bool(seed)),
        ("allowed".into(), Value::Bool(allowed)),
    ])
}
fn open(schema: jazz::schema::JazzSchema, node: u8, authority: bool) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: author(if node == 1 { "alice" } else { "bob" }),
        },
    )))
    .unwrap();
    if authority {
        db.set_authority();
        db.initialize_authority_catalogue().unwrap();
    }
    db
}
fn rows(db: &Db) -> Vec<(RowUuid, String)> {
    let q = db.prepare_query(&Query::from("nodes")).unwrap();
    let mut rows = block_on(db.all(&q, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.row_uuid(),
                match r
                    .cells()
                    .find(|(name, _)| *name == "scope")
                    .unwrap()
                    .1
                    .unwrap()
                {
                    Value::String(s) => s.clone(),
                    _ => panic!("text"),
                },
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
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
    panic!("did not settle")
}
struct Fixture {
    alice: Db,
    bob: Db,
}
impl Fixture {
    fn new(policy: PolicyExpr) -> Self {
        let schema = schema(policy);
        let alice = open(schema.clone(), 1, false);
        alice.set_remote_link_hint(RemoteLinkHint::NoServer);
        Self {
            alice,
            bob: open(schema, 2, true),
        }
    }
    fn commit(&self, values: &[(u128, BTreeMap<String, Value>)], serializable: bool) -> u64 {
        let id = OpenTransactionId::new();
        if serializable {
            block_on(self.alice.begin_serializable(id)).unwrap();
        } else {
            block_on(self.alice.begin_mergeable(id)).unwrap();
        }
        for (row_id, value) in values {
            block_on(self.alice.mergeable_tx_ref(id).upsert(
                "nodes",
                row(*row_id),
                value.clone(),
                Default::default(),
            ))
            .unwrap();
        }
        if serializable {
            block_on(self.alice.commit_serializable_handle(id))
                .unwrap()
                .0
        } else {
            block_on(self.alice.commit_mergeable_handle(id)).unwrap().0
        }
    }
    fn connect(&self) {
        let (up, down) = duplex();
        block_on(self.alice.connect_upstream(up));
        self.bob.accept_subscriber(down, author("alice"));
    }
    fn settle(&self, tx: u64) -> Result<(), Error> {
        drive(
            &[&self.alice, &self.bob],
            self.alice.wait_for_transaction(tx, DurabilityTier::Global),
        )
        .map(drop)
    }
}
/// Alice's reverse-order chain needs repeated permission proofs. Bob admits the
/// whole serializable unit only after its independent seed authorizes the chain.
#[test]
fn reverse_chain_and_seeded_cycle_are_authorized() {
    for values in [
        vec![
            (1, cells(2, false, true, "room")),
            (2, cells(3, false, true, "room")),
            (3, cells(3, true, true, "room")),
        ],
        vec![
            (1, cells(2, false, true, "room")),
            (2, cells(1, true, true, "room")),
        ],
    ] {
        let f = Fixture::new(insert(parent(true)));
        let tx = f.commit(&values, true);
        f.connect();
        f.settle(tx).unwrap();
        assert_eq!(rows(&f.bob).len(), values.len());
    }
}
/// Self/mutual cycles, an unauthorized parent and mismatched extra correlations
/// cannot manufacture an independent grant. Rejection removes the entire unit.
#[test]
fn ungrounded_or_wrong_scope_units_are_rejected_atomically() {
    for values in [
        vec![(1, cells(1, false, true, "room"))],
        vec![
            (1, cells(2, false, true, "room")),
            (2, cells(1, false, true, "room")),
        ],
        vec![
            (1, cells(2, false, true, "room")),
            (2, cells(2, true, false, "room")),
            (3, cells(3, true, true, "room")),
        ],
        vec![
            (1, cells(2, false, true, "first")),
            (2, cells(2, true, true, "second")),
        ],
    ] {
        let f = Fixture::new(insert(parent(true)));
        let tx = f.commit(&values, true);
        f.connect();
        assert!(f.settle(tx).is_err());
        assert!(rows(&f.bob).is_empty());
        assert!(rows(&f.alice).is_empty());
    }
}
/// Alice's ordinary EXISTS occurrence cannot borrow a marked occurrence's new
/// witnesses, even when they refer to the same physical table and parent id.
#[test]
fn unmarked_alias_stays_accepted_only() {
    let f = Fixture::new(insert(PolicyExpr::and(vec![parent(true), parent(false)])));
    let tx = f.commit(
        &[
            (1, cells(2, false, true, "room")),
            (2, cells(2, true, true, "room")),
        ],
        true,
    );
    f.connect();
    assert!(f.settle(tx).is_err());
    assert!(rows(&f.bob).is_empty());
}
/// A mergeable write can use accepted evidence or its independent seed arm,
/// but a mergeable batch never publishes new witnesses for dependent inserts.
#[test]
fn mergeable_uses_accepted_evidence_but_not_same_unit_creates() {
    let f = Fixture::new(insert(parent(true)));
    let tx = f.commit(
        &[
            (1, cells(2, false, true, "room")),
            (2, cells(2, true, true, "room")),
        ],
        false,
    );
    f.connect();
    assert!(f.settle(tx).is_err());
    let seed = f.commit(&[(10, cells(10, true, true, "room"))], false);
    f.settle(seed).unwrap();
    let child = f.commit(&[(11, cells(10, false, true, "room"))], false);
    f.settle(child).unwrap();
    assert_eq!(rows(&f.bob).len(), 2);
}
/// Bob's local session-bound transaction follows the same proof as an upload;
/// Alice may stage a dependent row before its seed without an early verdict.
#[test]
fn local_serializable_session_uses_the_same_unit_proof() {
    let bob = open(schema(insert(parent(true))), 2, true);
    let id = OpenTransactionId::new();
    block_on(bob.begin_serializable_for_identity(id, author("alice"))).unwrap();
    for (n, value) in [
        (1, cells(2, false, true, "room")),
        (2, cells(2, true, true, "room")),
    ] {
        block_on(
            bob.serializable_tx_ref(id)
                .upsert("nodes", row(n), value, Default::default()),
        )
        .unwrap();
    }
    block_on(bob.commit_serializable_handle(id)).unwrap();
    assert_eq!(rows(&bob).len(), 2);
}
/// An update of an accepted parent is not a newly created witness. Bob must
/// reject a dependent insert needing the parent's new scope and roll it back.
#[test]
fn updated_parent_cannot_be_promoted_to_created_evidence() {
    let f = Fixture::new(insert(parent(true)));
    let seed = f.commit(&[(10, cells(10, true, true, "old"))], false);
    f.connect();
    f.settle(seed).unwrap();
    let tx = f.commit(
        &[
            (1, cells(10, false, true, "new")),
            (10, cells(10, true, true, "new")),
        ],
        true,
    );
    assert!(f.settle(tx).is_err());
    assert_eq!(rows(&f.bob), vec![(row(10), "old".into())]);
}
/// Bob rechecks the final state: a marked child cannot keep a matching accepted
/// parent grant that Alice changes away in the same serializable transaction.
#[test]
fn final_state_revocation_invalidates_the_complete_unit() {
    let f = Fixture::new(insert(parent(true)));
    let seed = f.commit(&[(10, cells(10, true, true, "old"))], false);
    f.connect();
    f.settle(seed).unwrap();
    let tx = f.commit(
        &[
            (1, cells(10, false, true, "old")),
            (10, cells(10, true, true, "new")),
        ],
        true,
    );
    assert!(f.settle(tx).is_err());
    assert_eq!(rows(&f.bob), vec![(row(10), "old".into())]);
}

/// Marking an outer occurrence does not mark a nested one. Each nested source
/// keeps its own accepted-only/authorized-create mode and immediate correlation.
#[test]
fn nested_occurrences_keep_their_own_evidence_modes() {
    for marked in [false, true] {
        let dependent = PolicyExpr::exists_including_created(
            "nodes",
            PolicyExpr::and(vec![outer("id", "parent"), parent(marked)]),
        );
        let f = Fixture::new(insert(dependent));
        let tx = f.commit(
            &[
                (1, cells(2, false, true, "room")),
                (2, cells(3, false, true, "room")),
                (3, cells(3, true, true, "room")),
            ],
            true,
        );
        f.connect();
        assert_eq!(f.settle(tx).is_ok(), marked);
        assert_eq!(rows(&f.bob).len(), if marked { 3 } else { 0 });
    }
}
/// A reverse declared-reference witness must point at this candidate's id.
/// A seed for another row does not authorize Alice's candidate.
#[test]
fn reverse_reference_witnesses_only_authorize_matching_roots() {
    for matching in [false, true] {
        let dependent = PolicyExpr::exists_including_created("nodes", outer("parent", "id"));
        let f = Fixture::new(insert(dependent));
        let tx = f.commit(
            &[
                (1, cells(1, false, true, "room")),
                (2, cells(if matching { 1 } else { 9 }, true, true, "room")),
            ],
            true,
        );
        f.connect();
        assert_eq!(f.settle(tx).is_ok(), matching);
        assert_eq!(rows(&f.bob).len(), if matching { 2 } else { 0 });
    }
}
/// Alice's candidate and accepted witnesses are a union. Adding a new seed does
/// not hide the accepted parent required by another row in the same unit.
#[test]
fn accepted_witnesses_remain_visible_alongside_created_witnesses() {
    let f = Fixture::new(insert(parent(true)));
    let seed = f.commit(&[(10, cells(10, true, true, "room"))], false);
    f.connect();
    f.settle(seed).unwrap();
    let tx = f.commit(
        &[
            (1, cells(10, false, true, "room")),
            (2, cells(2, true, true, "room")),
            (3, cells(2, false, true, "room")),
        ],
        true,
    );
    f.settle(tx).unwrap();
    assert_eq!(rows(&f.bob).len(), 4);
}
/// The authority's authenticated session supplies `$createdBy` and its policy
/// identity. A new witness does not erase that row's independent author check.
#[test]
fn provenance_and_session_checks_survive_deferred_authorization() {
    let policy = PolicyExpr::and(vec![
        PolicyExpr::eq_session("$createdBy", vec!["user".into()]),
        insert(parent(true)),
    ]);
    let f = Fixture::new(policy);
    let tx = f.commit(
        &[
            (1, cells(2, false, true, "room")),
            (2, cells(2, true, true, "room")),
        ],
        true,
    );
    f.connect();
    f.settle(tx).unwrap();
    assert_eq!(rows(&f.bob).len(), 2);
}
/// The proof's work bound rejects the complete unit rather than falling back
/// to an unbounded graph walk or accepting an unchecked tail of the batch.
#[test]
fn proof_budget_exhaustion_rolls_back_the_whole_unit() {
    let f = Fixture::new(insert(parent(true)));
    let values = (1..=150)
        .map(|id| {
            (
                id,
                cells(if id == 150 { id } else { id + 1 }, id == 150, true, "room"),
            )
        })
        .collect::<Vec<_>>();
    let tx = f.commit(&values, true);
    f.connect();
    let error = f.settle(tx).unwrap_err();
    assert!(error.message.contains("bounded work"), "{error}");
    assert!(rows(&f.bob).is_empty());
    assert!(rows(&f.alice).is_empty());
}
/// Mallory cannot use a provisional, ungrounded membership to read Bob's
/// existing secret during local staging, before the final unit is refused.
#[test]
fn provisional_staging_does_not_grant_reads_through_unproven_creates() {
    let public = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("nodes")
                .fk_column("parent", "nodes")
                .column("scope", ColumnType::Text)
                .column("seed", ColumnType::Boolean)
                .column("allowed", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(insert(parent(true))),
                ),
        )
        .table(
            TableSchemaBuilder::new("secrets")
                .fk_column("node", "nodes")
                .column("text", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::Exists {
                    table: "nodes".into(),
                    condition: Box::new(outer("id", "node")),
                })),
        )
        .build();
    let bob = open(common::compile_schema(&public), 2, true);
    block_on(bob.insert(
        "secrets",
        BTreeMap::from([
            ("node".into(), Value::Uuid(row(1).0)),
            ("text".into(), Value::String("private".into())),
        ]),
        Default::default(),
    ))
    .unwrap();
    let id = OpenTransactionId::new();
    block_on(bob.begin_serializable_for_identity(id, author("mallory"))).unwrap();
    block_on(bob.serializable_tx_ref(id).upsert(
        "nodes",
        row(1),
        cells(1, false, true, "room"),
        Default::default(),
    ))
    .unwrap();
    let query = bob.prepare_query(&Query::from("secrets")).unwrap();
    assert!(
        block_on(bob.serializable_tx_ref(id).all_prepared(&query))
            .unwrap()
            .is_empty()
    );
    assert!(block_on(bob.commit_serializable_handle(id)).is_err());
    assert!(rows(&bob).is_empty());
}
/// Non-positive and non-INSERT uses remain rejected when admitting the schema;
/// the proof engine never gives cyclic negative predicates a meaning.
#[test]
fn nonmonotone_and_noninsert_markers_remain_invalid() {
    let marked = parent(true);
    for policies in [
        TablePolicies::new().with_select(marked.clone()),
        TablePolicies::new().with_update(None, marked.clone()),
        TablePolicies::new().with_delete(marked.clone()),
        TablePolicies::new().with_insert(PolicyExpr::Not(Box::new(marked))),
    ] {
        let public = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("nodes")
                    .fk_column("parent", "nodes")
                    .column("scope", ColumnType::Text)
                    .policies(policies),
            )
            .build();
        assert!(jazz::schema::JazzSchema::new(&public).is_err());
    }
}

/// Alice cannot keep an UPDATE WITH CHECK grant that she removes later in the
/// same unit. Bob rolls back both the edit and the revocation.
#[test]
fn update_with_check_revalidates_final_dependencies() {
    let public = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("nodes")
                .fk_column("parent", "nodes")
                .column("scope", ColumnType::Text)
                .column("seed", ColumnType::Boolean)
                .column("allowed", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(insert(parent(true)))
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                ),
        )
        .table(
            TableSchemaBuilder::new("notes")
                .fk_column("parent", "nodes")
                .column("scope", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), parent(false)),
                ),
        )
        .build();
    let bob = open(common::compile_schema(&public), 2, true);
    block_on(bob.upsert(
        "nodes",
        row(10),
        cells(10, true, true, "old"),
        Default::default(),
    ))
    .unwrap();
    block_on(bob.upsert(
        "notes",
        row(1),
        BTreeMap::from([
            ("parent".into(), Value::Uuid(row(10).0)),
            ("scope".into(), Value::String("old".into())),
        ]),
        Default::default(),
    ))
    .unwrap();
    let id = OpenTransactionId::new();
    block_on(bob.begin_serializable_for_identity(id, author("alice"))).unwrap();
    block_on(bob.serializable_tx_ref(id).upsert(
        "notes",
        row(1),
        BTreeMap::from([("scope".into(), Value::String("old".into()))]),
        Default::default(),
    ))
    .unwrap();
    block_on(bob.serializable_tx_ref(id).upsert(
        "nodes",
        row(10),
        cells(10, true, true, "new"),
        Default::default(),
    ))
    .unwrap();
    assert!(block_on(bob.commit_serializable_handle(id)).is_err());
    assert_eq!(rows(&bob), vec![(row(10), "old".into())]);
}

/// A marker in an unselected INSERT policy does not turn ordinary UPDATE
/// checks into mutually manufactured grants for Alice's two existing rows.
#[test]
fn unselected_insert_marker_cannot_unlock_mutual_updates() {
    let public = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("nodes")
                .fk_column("parent", "nodes")
                .column("scope", ColumnType::Text)
                .column("seed", ColumnType::Boolean)
                .column("allowed", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(insert(parent(true)))
                        .with_update(
                            Some(PolicyExpr::Exists {
                                table: "nodes".into(),
                                condition: Box::new(PolicyExpr::and(vec![
                                    outer("id", "parent"),
                                    PolicyExpr::eq_literal(
                                        "allowed",
                                        jazz::tools::Value::Boolean(true),
                                    ),
                                ])),
                            }),
                            PolicyExpr::True,
                        ),
                ),
        )
        .build();
    let bob = open(common::compile_schema(&public), 2, true);
    for (id, parent) in [(1, 2), (2, 1)] {
        block_on(bob.upsert(
            "nodes",
            row(id),
            cells(parent, true, false, "old"),
            Default::default(),
        ))
        .unwrap();
    }
    let id = OpenTransactionId::new();
    block_on(bob.begin_serializable_for_identity(id, author("alice"))).unwrap();
    for (row_id, parent) in [(1, 2), (2, 1)] {
        block_on(bob.serializable_tx_ref(id).upsert(
            "nodes",
            row(row_id),
            cells(parent, true, true, "new"),
            Default::default(),
        ))
        .unwrap();
    }
    assert!(block_on(bob.commit_serializable_handle(id)).is_err());
    assert_eq!(
        rows(&bob),
        vec![(row(1), "old".into()), (row(2), "old".into())]
    );
}

/// A rejected unit's independent seed cannot leak into a later proof, and
/// rejection must not leave private scratch state blocking Alice's next unit.
#[test]
fn rejected_proofs_do_not_leak_witnesses_or_poison_later_transactions() {
    let f = Fixture::new(insert(parent(true)));
    let rejected = f.commit(
        &[
            (1, cells(1, true, true, "room")),
            (2, cells(2, false, true, "room")),
        ],
        true,
    );
    f.connect();
    assert!(f.settle(rejected).is_err());
    let reused = f.commit(&[(3, cells(1, false, true, "room"))], true);
    assert!(f.settle(reused).is_err());
    let allowed = f.commit(
        &[
            (4, cells(5, false, true, "room")),
            (5, cells(5, true, true, "room")),
        ],
        true,
    );
    f.settle(allowed).unwrap();
    assert_eq!(
        rows(&f.bob),
        vec![(row(4), "room".into()), (row(5), "room".into())]
    );
}

/// Alice's correlated chunked strings compare by logical contents, even when
/// separately encoded. A shared 70 KiB prefix cannot hide a different suffix.
#[test]
fn chunked_correlations_compare_exact_logical_values() {
    let scope = "a".repeat(70 * 1024);
    for matching in [true, false] {
        let f = Fixture::new(insert(parent(true)));
        let child = format!("{scope}{}", if matching { "x" } else { "y" });
        let seed = format!("{scope}x");
        let tx = f.commit(
            &[
                (1, cells(2, false, true, &child)),
                (2, cells(2, true, true, &seed)),
            ],
            true,
        );
        f.connect();
        assert_eq!(f.settle(tx).is_ok(), matching);
        assert_eq!(rows(&f.bob).len(), if matching { 2 } else { 0 });
    }
}

/// Bob's accepted historical scope must be prepared even when Alice changes
/// its current chunked value in the same unit. The final-state check refuses it.
#[test]
fn historical_chunked_grants_are_checked_in_both_phases() {
    let old = format!("{}old", "a".repeat(70 * 1024));
    let new = format!("{}new", "a".repeat(70 * 1024));
    let f = Fixture::new(insert(parent(true)));
    let seed = f.commit(&[(10, cells(10, true, true, &old))], false);
    f.connect();
    f.settle(seed).unwrap();
    let tx = f.commit(
        &[
            (1, cells(10, false, true, &old)),
            (10, cells(10, true, true, &new)),
        ],
        true,
    );
    assert!(f.settle(tx).is_err());
    assert_eq!(rows(&f.bob), vec![(row(10), old)]);
}

/// Bob's proof reads only related large cells. Deliberately corrupting an
/// unrelated row's chunks must not affect Alice's independent candidate unit.
/// Raw SQLite is needed solely to inject this storage fault; all operations
/// and success assertions use the public API and isolated temporary storage.
#[test]
fn proof_preparation_does_not_touch_unrelated_chunked_values() {
    use jazz::db::FileOptions;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("proof.sqlite");
    let config = || {
        DbConfig::new(
            schema(insert(parent(true))),
            DbIdentity {
                node: NodeUuid::from_bytes([9; 16]),
                author: author("bob"),
            },
        )
    };
    {
        let bob = block_on(Db::open_at(config(), &path, FileOptions::default())).unwrap();
        bob.set_authority();
        bob.initialize_authority_catalogue().unwrap();
        block_on(bob.upsert(
            "nodes",
            row(99),
            cells(99, true, true, &"z".repeat(70 * 1024)),
            Default::default(),
        ))
        .unwrap();
    }
    {
        let storage = rusqlite::Connection::open(&path).unwrap();
        assert!(storage.execute("DELETE FROM _jazz_lv_node", []).unwrap() > 0);
    }
    let bob = block_on(Db::open_at(config(), &path, FileOptions::default())).unwrap();
    bob.set_authority();
    let id = OpenTransactionId::new();
    block_on(bob.begin_serializable_for_identity(id, author("alice"))).unwrap();
    for (row_id, parent, seed) in [(1, 2, false), (2, 2, true)] {
        block_on(bob.serializable_tx_ref(id).upsert(
            "nodes",
            row(row_id),
            cells(parent, seed, true, "room"),
            Default::default(),
        ))
        .unwrap();
    }
    block_on(bob.commit_serializable_handle(id)).unwrap();
    let query = bob
        .prepare_query(&Query::from("nodes").filter(jazz::query::eq(
            jazz::query::col("id"),
            jazz::query::lit(Value::Uuid(row(1).0)),
        )))
        .unwrap();
    assert_eq!(
        block_on(bob.all(&query, ReadOpts::default()))
            .unwrap()
            .len(),
        1
    );
}

/// Alice's baseline proof starts at node 1, while the surviving post-state
/// proof starts at node 2. Each phase must compute its own least fixed point.
#[test]
fn baseline_and_final_proofs_may_have_different_seed_orders() {
    let independent = PolicyExpr::Exists {
        table: "grants".into(),
        condition: Box::new(PolicyExpr::and(vec![
            outer("id", "grant"),
            outer("phase", "seed"),
        ])),
    };
    let dependent = PolicyExpr::exists_including_created("nodes", outer("id", "parent"));
    let public = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("nodes")
                .fk_column("parent", "nodes")
                .fk_column("grant", "grants")
                .column("scope", ColumnType::Text)
                .column("seed", ColumnType::Boolean)
                .column("allowed", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::or(vec![independent, dependent])),
                ),
        )
        .table(
            TableSchemaBuilder::new("grants")
                .column("phase", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                ),
        )
        .build();
    let bob = open(common::compile_schema(&public), 2, true);
    block_on(bob.upsert(
        "grants",
        row(10),
        BTreeMap::from([("phase".into(), Value::Bool(false))]),
        Default::default(),
    ))
    .unwrap();
    let id = OpenTransactionId::new();
    block_on(bob.begin_serializable_for_identity(id, author("alice"))).unwrap();
    for (row_id, parent, seed) in [(1, 2, false), (2, 1, true)] {
        let mut values = cells(parent, seed, true, "room");
        values.insert("grant".into(), Value::Uuid(row(10).0));
        block_on(bob.serializable_tx_ref(id).upsert(
            "nodes",
            row(row_id),
            values,
            Default::default(),
        ))
        .unwrap();
    }
    block_on(bob.serializable_tx_ref(id).upsert(
        "grants",
        row(10),
        BTreeMap::from([("phase".into(), Value::Bool(true))]),
        Default::default(),
    ))
    .unwrap();
    block_on(bob.commit_serializable_handle(id)).unwrap();
    assert_eq!(rows(&bob).len(), 2);
}

/// A newly inserted coordinate written twice is not a single proven create.
/// Alice cannot use its final image to authorize another row in the unit.
#[test]
fn multiply_written_coordinates_cannot_supply_created_evidence() {
    let f = Fixture::new(insert(parent(true)));
    let tx = f.commit(
        &[
            (1, cells(2, false, true, "room")),
            (2, cells(2, true, true, "room")),
            (2, cells(2, true, true, "room")),
        ],
        true,
    );
    f.connect();
    assert!(f.settle(tx).is_err());
    assert!(rows(&f.bob).is_empty());
}

/// Provisional string-pair reads are refused explicitly until their historical
/// large-value preparation is bounded. Mallory cannot get an incomplete proof.
#[test]
fn provisional_large_value_reads_fail_explicitly() {
    let public = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("nodes")
                .fk_column("parent", "nodes")
                .column("scope", ColumnType::Text)
                .column("seed", ColumnType::Boolean)
                .column("allowed", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(parent(false))
                        .with_insert(insert(parent(true))),
                ),
        )
        .build();
    let bob = open(common::compile_schema(&public), 2, true);
    let id = OpenTransactionId::new();
    block_on(bob.begin_serializable_for_identity(id, author("mallory"))).unwrap();
    block_on(bob.serializable_tx_ref(id).upsert(
        "nodes",
        row(1),
        cells(1, false, true, "room"),
        Default::default(),
    ))
    .unwrap();
    let query = bob.prepare_query(&Query::from("nodes")).unwrap();
    let error = block_on(bob.serializable_tx_ref(id).all_prepared(&query)).unwrap_err();
    assert!(
        error
            .message
            .contains("provisional reads requiring large-value preprocessing"),
        "{error}"
    );
    assert!(block_on(bob.commit_serializable_handle(id)).is_err());
}

/// Alice's unit must not fall back to scanning all accepted rows for an
/// uncorrelated dependency, even when another branch has an independent seed.
#[test]
fn unbounded_dependency_sources_are_refused_explicitly() {
    let unbounded = PolicyExpr::Exists {
        table: "nodes".into(),
        condition: Box::new(PolicyExpr::True),
    };
    let f = Fixture::new(insert(PolicyExpr::and(vec![parent(true), unbounded])));
    let tx = f.commit(&[(1, cells(1, true, true, "room"))], true);
    f.connect();
    let error = f.settle(tx).unwrap_err();
    assert!(
        error
            .message
            .contains("scalar declared-reference correlation"),
        "{error}"
    );
    assert!(rows(&f.bob).is_empty());
}

fn inherited(operation: jazz::tools::Operation, depth: Option<usize>) -> PolicyExpr {
    PolicyExpr::Inherits {
        operation,
        via_column: "parent".into(),
        max_depth: depth,
    }
}
fn marker_only() -> PolicyExpr {
    PolicyExpr::exists_including_created(
        "nodes",
        PolicyExpr::and(vec![outer("id", "parent"), PolicyExpr::False]),
    )
}
fn inherited_schema(
    dependent: PolicyExpr,
    select: PolicyExpr,
    using: PolicyExpr,
    with_check: PolicyExpr,
) -> jazz::schema::JazzSchema {
    common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("nodes")
                    .fk_column("parent", "nodes")
                    .column("scope", ColumnType::Text)
                    .column("seed", ColumnType::Boolean)
                    .column("allowed", ColumnType::Boolean)
                    .policies(
                        TablePolicies::new()
                            .with_select(select)
                            .with_insert(PolicyExpr::or(vec![
                                PolicyExpr::eq_literal("seed", jazz::tools::Value::Boolean(true)),
                                dependent,
                                marker_only(),
                            ]))
                            .with_update(Some(using), with_check),
                    ),
            )
            .build(),
    )
}
fn fixture_schema(schema: jazz::schema::JazzSchema) -> Fixture {
    let alice = open(schema.clone(), 1, false);
    alice.set_remote_link_hint(RemoteLinkHint::NoServer);
    Fixture {
        alice,
        bob: open(schema, 2, true),
    }
}
fn allowed() -> PolicyExpr {
    PolicyExpr::eq_literal("allowed", jazz::tools::Value::Boolean(true))
}

/// Alice inherits Bob's accepted UPDATE capability, not its WITH CHECK. An
/// immutable parent can grant child creation while its own edit still rejects.
#[test]
fn inherited_update_uses_using_without_requiring_parent_mutability() {
    let f = fixture_schema(inherited_schema(
        inherited(jazz::tools::Operation::Update, None),
        PolicyExpr::True,
        allowed(),
        PolicyExpr::False,
    ));
    let seed = f.commit(&[(10, cells(10, true, true, "parent"))], false);
    f.connect();
    f.settle(seed).unwrap();
    let child = f.commit(&[(1, cells(10, false, false, "child"))], true);
    f.settle(child).unwrap();
    let edit = f.commit(&[(10, cells(10, true, true, "changed"))], true);
    assert!(f.settle(edit).is_err());
    assert_eq!(
        rows(&f.bob),
        vec![(row(1), "child".into()), (row(10), "parent".into())]
    );
}

/// A newly permitted parent is still not an accepted inherited grant. Alice's
/// unmarked inheritance cannot borrow a marked occurrence's candidate source.
#[test]
fn newly_authorized_parent_cannot_supply_inherited_evidence() {
    let f = fixture_schema(inherited_schema(
        inherited(jazz::tools::Operation::Update, None),
        PolicyExpr::True,
        allowed(),
        PolicyExpr::True,
    ));
    let tx = f.commit(
        &[
            (1, cells(10, false, false, "child")),
            (10, cells(10, true, true, "parent")),
        ],
        true,
    );
    f.connect();
    assert!(f.settle(tx).is_err());
    assert!(rows(&f.bob).is_empty());
}

/// Revoking Bob's accepted capability is allowed by itself, but Alice cannot
/// use the revoked grant to publish a dependent child in the same unit.
#[test]
fn inherited_grants_must_survive_the_final_state() {
    for child in [false, true] {
        let f = fixture_schema(inherited_schema(
            inherited(jazz::tools::Operation::Update, None),
            PolicyExpr::True,
            allowed(),
            PolicyExpr::True,
        ));
        let seed = f.commit(&[(10, cells(10, true, true, "parent"))], false);
        f.connect();
        f.settle(seed).unwrap();
        let mut values = vec![(10, cells(10, true, false, "revoked"))];
        if child {
            values.insert(0, (1, cells(10, false, false, "child")));
        }
        let tx = f.commit(&values, true);
        assert_eq!(f.settle(tx).is_ok(), !child);
        assert_eq!(
            rows(&f.bob),
            vec![(row(10), if child { "parent" } else { "revoked" }.into())]
        );
    }
}

/// Nested inherited permission checks prepare both sides of related chunked
/// string comparisons. Sharing a long prefix cannot grant Mallory permission.
#[test]
fn inherited_nested_large_value_comparisons_keep_exact_dependencies() {
    let prefix = "a".repeat(70 * 1024);
    for matching in [true, false] {
        let f = fixture_schema(inherited_schema(
            inherited(jazz::tools::Operation::Update, None),
            PolicyExpr::True,
            parent(false),
            PolicyExpr::True,
        ));
        let seed = f.commit(
            &[
                (10, cells(20, true, false, &format!("{prefix}x"))),
                (
                    20,
                    cells(
                        20,
                        true,
                        false,
                        &format!("{prefix}{}", if matching { "x" } else { "y" }),
                    ),
                ),
            ],
            false,
        );
        f.connect();
        f.settle(seed).unwrap();
        let tx = f.commit(&[(1, cells(10, false, false, "child"))], true);
        assert_eq!(f.settle(tx).is_ok(), matching);
        assert_eq!(rows(&f.bob).len(), if matching { 3 } else { 2 });
    }
}

/// Alice's inherited SELECT follows the same explicit recursion bound in SQL
/// and large-value preparation. Zero/one hop cannot reach the two-hop grant.
#[test]
fn inheritance_depth_limits_match_during_large_value_preparation() {
    let value = "a".repeat(70 * 1024);
    for depth in [0, 1, 2] {
        let select = PolicyExpr::or(vec![
            PolicyExpr::and(vec![
                allowed(),
                PolicyExpr::eq_literal("scope", jazz::tools::Value::Text(value.clone())),
            ]),
            inherited(jazz::tools::Operation::Select, None),
        ]);
        let f = fixture_schema(inherited_schema(
            inherited(jazz::tools::Operation::Select, Some(depth)),
            select,
            PolicyExpr::True,
            PolicyExpr::True,
        ));
        let seed = f.commit(
            &[
                (10, cells(20, true, false, &value)),
                (20, cells(20, true, true, &value)),
            ],
            false,
        );
        f.connect();
        f.settle(seed).unwrap();
        let tx = f.commit(&[(1, cells(10, false, false, "child"))], true);
        assert_eq!(f.settle(tx).is_ok(), depth == 2);
        assert_eq!(rows(&f.bob).len(), if depth == 2 { 3 } else { 2 });
    }
}

/// A provisional grant cannot unlock Bob's secret through inherited SELECT.
/// After the independent grant commits, Alice can read the secret normally.
#[test]
fn staged_inherited_grants_do_not_unlock_existing_secrets() {
    let public = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("nodes")
                .fk_column("parent", "nodes")
                .column("scope", ColumnType::Text)
                .column("seed", ColumnType::Boolean)
                .column("allowed", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(allowed())
                        .with_insert(insert(parent(true))),
                ),
        )
        .table(
            TableSchemaBuilder::new("secrets")
                .fk_column("parent", "nodes")
                .column("text", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(inherited(jazz::tools::Operation::Select, None)),
                ),
        )
        .build();
    let bob = open(common::compile_schema(&public), 2, true);
    block_on(bob.insert(
        "secrets",
        BTreeMap::from([
            ("parent".into(), Value::Uuid(row(10).0)),
            ("text".into(), Value::String("private".into())),
        ]),
        Default::default(),
    ))
    .unwrap();
    let id = OpenTransactionId::new();
    block_on(bob.begin_serializable_for_identity(id, author("alice"))).unwrap();
    block_on(bob.serializable_tx_ref(id).upsert(
        "nodes",
        row(10),
        cells(10, true, true, "room"),
        Default::default(),
    ))
    .unwrap();
    let query = bob.prepare_query(&Query::from("secrets")).unwrap();
    assert!(
        block_on(bob.serializable_tx_ref(id).all_prepared(&query))
            .unwrap()
            .is_empty()
    );
    block_on(bob.commit_serializable_handle(id)).unwrap();
    assert_eq!(
        block_on(bob.all_for_identity(&query, ReadOpts::default(), author("alice")))
            .unwrap()
            .len(),
        1
    );
}

// This public-builder graph follows compileSpaceAdministration's structure:
// immutable roots <- reverse UPDATE grants from administration rows; a first
// grant has a separate marked root lookup, while later lifecycle rows inherit.
fn managed_permission_schema(
    mutable_admin: bool,
    independent_admin: bool,
    cap_scope: Option<&str>,
) -> jazz::schema::JazzSchema {
    let owns = PolicyExpr::eq_session(
        "owner",
        vec!["user".into(), "identity".into(), "subject".into()],
    );
    let capability = PolicyExpr::and(vec![
        owns.clone(),
        cap_scope.map_or(PolicyExpr::True, |scope| {
            PolicyExpr::eq_literal("scope", jazz::tools::Value::Text(scope.into()))
        }),
    ]);
    let source = PolicyExpr::InheritsReferencing {
        operation: jazz::tools::Operation::Update,
        source_table: "admins".into(),
        via_column: "parent".into(),
        max_depth: None,
    };
    let root = PolicyExpr::exists_including_created(
        "nodes",
        PolicyExpr::and(vec![
            outer("id", "parent"),
            outer("scope", "scope"),
            PolicyExpr::eq_session("$createdBy", vec!["user".into()]),
        ]),
    );
    let initial = PolicyExpr::exists_including_created(
        "nodes",
        PolicyExpr::and(vec![
            outer("id", "parent"),
            outer("scope", "scope"),
            outer("initialGrant", "id"),
            PolicyExpr::eq_session("$createdBy", vec!["user".into()]),
        ]),
    );
    common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("nodes")
                    .fk_column("parent", "nodes")
                    .fk_column("initialGrant", "grants")
                    .column("scope", ColumnType::Text)
                    .column("seed", ColumnType::Boolean)
                    .column("allowed", ColumnType::Boolean)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::eq_literal(
                                "seed",
                                jazz::tools::Value::Boolean(true),
                            ))
                            .with_update(Some(source), PolicyExpr::False),
                    ),
            )
            .table(
                TableSchemaBuilder::new("admins")
                    .fk_column("parent", "nodes")
                    .column("scope", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(if independent_admin {
                                PolicyExpr::True
                            } else {
                                PolicyExpr::and(vec![owns, root])
                            })
                            .with_update(
                                Some(capability),
                                if mutable_admin {
                                    PolicyExpr::True
                                } else {
                                    PolicyExpr::False
                                },
                            ),
                    ),
            )
            .table(
                TableSchemaBuilder::new("grants")
                    .fk_column("parent", "nodes")
                    .column("scope", ColumnType::Text)
                    .column("operation", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::or(vec![
                                inherited(jazz::tools::Operation::Update, None),
                                PolicyExpr::and(vec![
                                    PolicyExpr::eq_literal(
                                        "operation",
                                        jazz::tools::Value::Text("add".into()),
                                    ),
                                    initial,
                                ]),
                            ])),
                    ),
            )
            .build(),
    )
}
fn root_cells(scope: &str) -> BTreeMap<String, Value> {
    let mut values = cells(1, true, true, scope);
    values.insert("initialGrant".into(), Value::Uuid(row(3).0));
    values
}
fn admin_cells(parent: u128, scope: &str, owner: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("parent".into(), Value::Uuid(row(parent).0)),
        ("scope".into(), Value::String(scope.into())),
        ("owner".into(), Value::String(owner.into())),
    ])
}
fn grant_cells(parent: u128, scope: &str, operation: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("parent".into(), Value::Uuid(row(parent).0)),
        ("scope".into(), Value::String(scope.into())),
        ("operation".into(), Value::String(operation.into())),
    ])
}
fn named_commit(db: &Db, values: &[(&str, u128, BTreeMap<String, Value>)]) -> u64 {
    let id = OpenTransactionId::new();
    block_on(db.begin_serializable(id)).unwrap();
    for (table, row_id, value) in values {
        block_on(db.serializable_tx_ref(id).upsert(
            table,
            row(*row_id),
            value.clone(),
            Default::default(),
        ))
        .unwrap();
    }
    block_on(db.commit_serializable_handle(id)).unwrap().0
}
fn table_count(db: &Db, table: &str) -> usize {
    let query = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(&query, ReadOpts::default())).unwrap().len()
}
fn founder(f: &Fixture, scope: &str) -> u64 {
    named_commit(
        &f.alice,
        &[
            ("grants", 3, grant_cells(1, scope, "add")),
            ("admins", 2, admin_cells(1, scope, "alice")),
            ("nodes", 1, root_cells(scope)),
        ],
    )
}

/// Alice's founder unit uses the generated space-policy shape: the first grant
/// and administration row may precede their immutable root. The inherited OR
/// arm must stay accepted-only while the marked initial-grant arm proves them.
#[test]
fn founder_graph_combines_marked_creation_with_inherited_alternatives() {
    for scope in ["room".to_owned(), "a".repeat(70 * 1024)] {
        let f = fixture_schema(managed_permission_schema(false, false, None));
        let tx = founder(&f, &scope);
        f.connect();
        f.settle(tx).unwrap();
        for table in ["nodes", "admins", "grants"] {
            assert_eq!(table_count(&f.bob, table), 1);
        }
    }
}

/// Alice cannot substitute another initial grant and rely on the newly
/// created administration row to supply an inherited UPDATE grant instead.
#[test]
fn founder_cannot_borrow_new_reverse_grants_for_the_wrong_initial_row() {
    let f = fixture_schema(managed_permission_schema(false, false, None));
    let tx = named_commit(
        &f.alice,
        &[
            ("grants", 4, grant_cells(1, "room", "add")),
            ("admins", 2, admin_cells(1, "room", "alice")),
            ("nodes", 1, root_cells("room")),
        ],
    );
    f.connect();
    assert!(f.settle(tx).is_err());
    for table in ["nodes", "admins", "grants"] {
        assert_eq!(table_count(&f.bob, table), 0);
    }
}

/// Once Bob accepts Alice's founder graph, later lifecycle rows inherit through
/// the immutable root and its reverse administration grant. Mallory cannot use it.
#[test]
fn accepted_forward_and_reverse_grants_authorize_only_their_session() {
    let f = fixture_schema(managed_permission_schema(false, false, None));
    let tx = founder(&f, "room");
    f.connect();
    f.settle(tx).unwrap();
    let next = named_commit(&f.alice, &[("grants", 4, grant_cells(1, "room", "rotate"))]);
    f.settle(next).unwrap();
    let id = OpenTransactionId::new();
    block_on(f.bob.begin_serializable_for_identity(id, author("mallory"))).unwrap();
    block_on(f.bob.serializable_tx_ref(id).upsert(
        "grants",
        row(5),
        grant_cells(1, "room", "rotate"),
        Default::default(),
    ))
    .unwrap();
    assert!(block_on(f.bob.commit_serializable_handle(id)).is_err());
    assert_eq!(table_count(&f.bob, "grants"), 2);
}

/// A reverse grant created by an independently allowed INSERT remains excluded
/// from inheritance until it settles in a separate transaction.
#[test]
fn new_reverse_grants_become_inherited_evidence_only_after_acceptance() {
    let f = fixture_schema(managed_permission_schema(false, true, None));
    let tx = named_commit(&f.alice, &[("nodes", 1, root_cells("room"))]);
    f.connect();
    f.settle(tx).unwrap();
    let tx = named_commit(
        &f.alice,
        &[
            ("admins", 2, admin_cells(1, "room", "alice")),
            ("grants", 4, grant_cells(1, "room", "rotate")),
        ],
    );
    assert!(f.settle(tx).is_err());
    assert_eq!(table_count(&f.bob, "admins"), 0);
    let admin = named_commit(&f.alice, &[("admins", 2, admin_cells(1, "room", "alice"))]);
    f.settle(admin).unwrap();
    let grant = named_commit(&f.alice, &[("grants", 4, grant_cells(1, "room", "rotate"))]);
    f.settle(grant).unwrap();
    assert_eq!(table_count(&f.bob, "grants"), 1);
}

/// Demoting Alice's reverse administration grant is allowed on its own. Adding
/// a dependent lifecycle write to that unit fails final-state revalidation.
#[test]
fn reverse_grant_revocation_invalidates_dependent_lifecycle_writes() {
    for dependent in [false, true] {
        let f = fixture_schema(managed_permission_schema(true, false, None));
        let tx = founder(&f, "room");
        f.connect();
        f.settle(tx).unwrap();
        let mut values = vec![("admins", 2, admin_cells(1, "room", "bob"))];
        if dependent {
            values.insert(0, ("grants", 4, grant_cells(1, "room", "rotate")));
        }
        let tx = named_commit(&f.alice, &values);
        assert_eq!(f.settle(tx).is_ok(), !dependent);
        assert_eq!(table_count(&f.bob, "grants"), 1);
        let next = named_commit(&f.alice, &[("grants", 5, grant_cells(1, "room", "rotate"))]);
        assert_eq!(f.settle(next).is_ok(), dependent);
    }
}

/// Reverse inheritance prepares the accepted administration row's exact large
/// scope. A changed suffix cannot satisfy the expected scope through a common prefix.
#[test]
fn reverse_inherited_large_values_use_exact_scoped_preparation() {
    let scope = format!("{}x", "a".repeat(70 * 1024));
    let fake = format!("{}y", "a".repeat(70 * 1024));
    for matching in [true, false] {
        let f = fixture_schema(managed_permission_schema(false, true, Some(&scope)));
        let tx = named_commit(
            &f.alice,
            &[
                ("nodes", 1, root_cells(&scope)),
                (
                    "admins",
                    2,
                    admin_cells(1, if matching { &scope } else { &fake }, "alice"),
                ),
            ],
        );
        f.connect();
        f.settle(tx).unwrap();
        let tx = named_commit(&f.alice, &[("grants", 4, grant_cells(1, &scope, "rotate"))]);
        assert_eq!(f.settle(tx).is_ok(), matching);
    }
}

/// A marked and an ordinary EXISTS may both use the same accepted parent.
/// Standalone baseline preparation must bind only SQL parameters it uses.
#[test]
fn marked_and_unmarked_large_comparisons_share_accepted_parent_evidence() {
    let f = Fixture::new(insert(PolicyExpr::and(vec![parent(true), parent(false)])));
    let scope = "a".repeat(70 * 1024);
    let seed = f.commit(&[(10, cells(10, true, true, &scope))], false);
    f.connect();
    f.settle(seed).unwrap();
    let child = f.commit(&[(1, cells(10, false, true, &scope))], true);
    f.settle(child).unwrap();
    assert_eq!(rows(&f.bob).len(), 2);
}

/// Alice's accepted array-backed owner supplies inherited UPDATE evidence.
/// The same serializable candidate now succeeds through indexed membership.
#[test]
fn array_inherited_sources_use_accepted_indexed_membership() {
    let reverse = PolicyExpr::InheritsReferencing {
        operation: jazz::tools::Operation::Update,
        source_table: "owners".into(),
        via_column: "parents".into(),
        max_depth: None,
    };
    let public = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("nodes")
                .fk_column("parent", "nodes")
                .column("scope", ColumnType::Text)
                .column("seed", ColumnType::Boolean)
                .column("allowed", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::or(vec![reverse, marker_only()])),
                ),
        )
        .table(
            TableSchemaBuilder::new("owners")
                .array_fk_column("parents", "nodes")
                .policies(
                    TablePolicies::new().with_update(Some(PolicyExpr::True), PolicyExpr::True),
                ),
        )
        .build();
    let f = fixture_schema(common::compile_schema(&public));
    block_on(f.bob.insert(
        "owners",
        BTreeMap::from([("parents".into(), Value::Array(vec![Value::Uuid(row(1).0)]))]),
        Default::default(),
    ))
    .unwrap();
    let tx = f.commit(&[(1, cells(1, false, true, "room"))], true);
    f.connect();
    f.settle(tx).unwrap();
    assert_eq!(rows(&f.bob), vec![(row(1), "room".into())]);
}

/// An accepted row's INSERT policy cannot launder newly created evidence
/// through inheritance. Bob rejects Alice's entire unit at that boundary.
#[test]
fn inheritance_cannot_expose_a_targets_creation_aware_insert_policy() {
    let public = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("nodes")
                .fk_column("parent", "nodes")
                .column("scope", ColumnType::Text)
                .column("seed", ColumnType::Boolean)
                .column("allowed", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(insert(parent(true))),
                ),
        )
        .table(
            TableSchemaBuilder::new("leases")
                .fk_column("parent", "nodes")
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(inherited(jazz::tools::Operation::Insert, None)),
                ),
        )
        .build();
    let f = fixture_schema(common::compile_schema(&public));
    block_on(f.bob.upsert(
        "nodes",
        row(10),
        cells(11, false, true, "room"),
        Default::default(),
    ))
    .unwrap();
    let tx = named_commit(
        &f.alice,
        &[
            ("nodes", 11, cells(11, true, true, "room")),
            (
                "leases",
                1,
                BTreeMap::from([("parent".into(), Value::Uuid(row(10).0))]),
            ),
        ],
    );
    f.connect();
    let error = f.settle(tx).unwrap_err();
    assert!(
        error
            .message
            .contains("ExistsIncludingCreated through inherited permission policies"),
        "{error}"
    );
    assert_eq!(rows(&f.bob), vec![(row(10), "room".into())]);
    assert_eq!(table_count(&f.bob, "leases"), 0);
}

/// Updating both dependency rows to matching large values cannot manufacture a
/// baseline inherited grant. Bob compares both historical operands exactly;
/// Alice's whole unit succeeds only if the grant already existed and survives.
#[test]
fn inherited_large_grants_use_historical_values_before_same_unit_updates() {
    let public = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("nodes")
                .fk_column("parent", "nodes")
                .column("scope", ColumnType::Text)
                .column("seed", ColumnType::Boolean)
                .column("allowed", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::or(vec![
                            PolicyExpr::eq_literal("seed", jazz::tools::Value::Boolean(true)),
                            inherited(jazz::tools::Operation::Delete, None),
                            marker_only(),
                        ]))
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                        .with_delete(parent(false)),
                ),
        )
        .build();
    let prefix = "a".repeat(70 * 1024);
    let old = format!("{prefix}old");
    let bad = format!("{prefix}bad");
    let new = format!("{prefix}new");
    for existed in [true, false] {
        let f = fixture_schema(common::compile_schema(&public));
        let seed = f.commit(
            &[
                (10, cells(20, true, true, &old)),
                (20, cells(20, true, true, if existed { &old } else { &bad })),
            ],
            false,
        );
        f.connect();
        f.settle(seed).unwrap();
        let tx = f.commit(
            &[
                (1, cells(10, false, true, "child")),
                (10, cells(20, true, true, &new)),
                (20, cells(20, true, true, &new)),
            ],
            true,
        );
        assert_eq!(f.settle(tx).is_ok(), existed);
        assert_eq!(rows(&f.bob).len(), if existed { 3 } else { 2 });
        if !existed {
            assert_eq!(
                rows(&f.bob),
                vec![(row(10), old.clone()), (row(20), bad.clone())]
            );
        }
    }
}

/// Alice seals a same-unit founder graph, then creates another graph against
/// that publication. Bob checks both the sealed unit and the later serializable
/// upload carrying the reservation-to-publication origin map.
#[test]
fn sealed_and_origin_mapped_units_keep_candidate_authorization() {
    let f = Fixture::new(insert(parent(true)));
    let id = OpenTransactionId::new();
    block_on(f.alice.begin_serializable(id)).unwrap();
    for (row_id, parent, seed) in [(1, 2, false), (2, 2, true)] {
        f.alice
            .prepare_initialization_insert(id, "nodes", row(row_id))
            .unwrap();
        block_on(f.alice.serializable_tx_ref(id).upsert(
            "nodes",
            row(row_id),
            cells(parent, seed, true, "room"),
            Default::default(),
        ))
        .unwrap();
    }
    let seal = f.alice.seal_initialization_transaction(id).unwrap();
    assert!(rows(&f.alice).is_empty());
    let published = f.alice.publish_initialization_transaction(seal).unwrap().0;
    let next = f.commit(
        &[
            (3, cells(4, false, true, "room")),
            (4, cells(4, true, true, "room")),
        ],
        true,
    );
    f.connect();
    f.settle(published).unwrap();
    f.settle(next).unwrap();
    assert_eq!(rows(&f.bob).len(), 4);
}

/// E17's catalogue UUID comes from Bob's accepted manifest during deferred
/// checks too. Mallory's claims cannot substitute a fake scope for a new seed.
#[test]
fn candidate_checks_keep_the_accepted_catalogue_identity_provider() {
    for matching in [true, false] {
        let policy = PolicyExpr::and(vec![
            PolicyExpr::eq_session(
                "scopeId",
                vec!["__jazz_table_identity_v1".into(), "nodes".into()],
            ),
            insert(parent(true)),
        ]);
        let public = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("nodes")
                    .fk_column("parent", "nodes")
                    .column("scope", ColumnType::Text)
                    .column("scopeId", ColumnType::Uuid)
                    .column("seed", ColumnType::Boolean)
                    .column("allowed", ColumnType::Boolean)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(policy),
                    ),
            )
            .build();
        let bob = open(common::compile_schema(&public), 2, true);
        let scope = bob.table_identity("nodes").unwrap().unwrap().0;
        let fake = uuid::Uuid::from_bytes([91; 16]);
        bob.set_identity_claims(
            author("mallory"),
            BTreeMap::from([
                ("__jazz_table_identity_v1".into(), Value::Uuid(fake)),
                (
                    jazz::query::provider_claim_key("__jazz_table_identity_v1"),
                    Value::Uuid(fake),
                ),
            ]),
        );
        let id = OpenTransactionId::new();
        block_on(bob.begin_serializable_for_identity(id, author("mallory"))).unwrap();
        for (row_id, parent, seed) in [(1, 2, false), (2, 2, true)] {
            let mut values = cells(parent, seed, true, "room");
            values.insert(
                "scopeId".into(),
                Value::Uuid(if matching { scope } else { fake }),
            );
            block_on(bob.serializable_tx_ref(id).upsert(
                "nodes",
                row(row_id),
                values,
                Default::default(),
            ))
            .unwrap();
        }
        assert_eq!(
            block_on(bob.commit_serializable_handle(id)).is_ok(),
            matching
        );
        assert_eq!(rows(&bob).len(), if matching { 2 } else { 0 });
    }
}

fn array_candidate_schema() -> jazz::schema::JazzSchema {
    common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("nodes")
                    .fk_column("parent", "nodes")
                    .column("scope", ColumnType::Text)
                    .column("seed", ColumnType::Boolean)
                    .column("allowed", ColumnType::Boolean)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::or(vec![
                                PolicyExpr::InheritsReferencing {
                                    operation: jazz::tools::Operation::Update,
                                    source_table: "owners".into(),
                                    via_column: "parents".into(),
                                    max_depth: None,
                                },
                                marker_only(),
                            ])),
                    ),
            )
            .table(
                TableSchemaBuilder::new("owners")
                    .array_fk_column("parents", "nodes")
                    .column("owner", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(
                                Some(PolicyExpr::eq_session(
                                    "owner",
                                    vec!["user".into(), "identity".into(), "subject".into()],
                                )),
                                PolicyExpr::True,
                            ),
                    ),
            )
            .build(),
    )
}
fn array_owner(parents: &[u128]) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "parents".into(),
            Value::Array(parents.iter().map(|n| Value::Uuid(row(*n).0)).collect()),
        ),
        ("owner".into(), Value::String("alice".into())),
    ])
}

/// Alice cannot use a newly inserted array grant as accepted inherited
/// evidence. Bob rolls back both rows; accepting the grant separately allows
/// the same dependent write, including duplicate array membership.
#[test]
fn newly_created_array_grants_do_not_unlock_their_own_unit() {
    let f = fixture_schema(array_candidate_schema());
    let tx = named_commit(
        &f.alice,
        &[
            ("owners", 2, array_owner(&[1, 1])),
            ("nodes", 1, cells(1, false, true, "room")),
        ],
    );
    f.connect();
    assert!(f.settle(tx).is_err());
    assert!(rows(&f.bob).is_empty());
    assert_eq!(table_count(&f.bob, "owners"), 0);
    let grant = named_commit(&f.alice, &[("owners", 2, array_owner(&[1, 1]))]);
    f.settle(grant).unwrap();
    let child = named_commit(&f.alice, &[("nodes", 1, cells(1, false, true, "room"))]);
    f.settle(child).unwrap();
    assert_eq!(rows(&f.bob), vec![(row(1), "room".into())]);
}

/// Alice revokes an accepted array grant in the same unit as a dependent write.
/// Bob checks both accepted and final images, rejecting the whole unit in either
/// operation order. The rolled-back index must still authorize a later write.
#[test]
fn array_revocation_revalidates_the_final_unit_and_rolls_back_membership() {
    for revoke_first in [false, true] {
        let f = fixture_schema(array_candidate_schema());
        let grant = named_commit(&f.alice, &[("owners", 2, array_owner(&[1]))]);
        f.connect();
        f.settle(grant).unwrap();
        let mut writes = vec![
            ("nodes", 1, cells(1, false, true, "room")),
            ("owners", 2, array_owner(&[])),
        ];
        if revoke_first {
            writes.reverse();
        }
        let tx = named_commit(&f.alice, &writes);
        assert!(f.settle(tx).is_err());
        assert!(rows(&f.bob).is_empty());
        assert_eq!(table_count(&f.bob, "owners"), 1);
        let next = named_commit(&f.alice, &[("nodes", 1, cells(1, false, true, "room"))]);
        f.settle(next).unwrap();
        assert_eq!(rows(&f.bob), vec![(row(1), "room".into())]);
    }
}

/// Alice's ordinary EXISTS graph uses independently grounded surviving writes.
/// Bob accepts reverse staging and seeded cycles for either transaction kind.
#[test]
fn ordinary_exists_grounds_reverse_chains_and_seeded_cycles() {
    for serializable in [false, true] {
        for values in [
            vec![
                (1, cells(2, false, true, "room")),
                (2, cells(3, false, true, "room")),
                (3, cells(3, true, true, "room")),
            ],
            vec![
                (1, cells(2, false, true, "room")),
                (2, cells(1, true, true, "room")),
            ],
        ] {
            let f = Fixture::new(insert(parent(false)));
            let tx = f.commit(&values, serializable);
            f.connect();
            f.settle(tx).unwrap();
            assert_eq!(rows(&f.bob).len(), values.len());
        }
    }
}

/// Mallory cannot bootstrap ordinary permission from herself, an unseeded cycle,
/// an unauthorised seed or a parent with a different correlated scope.
#[test]
fn ordinary_exists_refuses_ungrounded_and_wrong_scope_writes() {
    for serializable in [false, true] {
        for values in [
            vec![(1, cells(1, false, true, "room"))],
            vec![
                (1, cells(2, false, true, "room")),
                (2, cells(1, false, true, "room")),
            ],
            vec![
                (1, cells(2, false, true, "room")),
                (2, cells(2, true, false, "room")),
            ],
            vec![
                (1, cells(2, false, true, "first")),
                (2, cells(2, true, true, "second")),
            ],
        ] {
            let f = Fixture::new(insert(parent(false)));
            let tx = f.commit(&values, serializable);
            f.connect();
            assert!(f.settle(tx).unwrap_err().is_authorization_denied());
            assert!(rows(&f.bob).is_empty());
            assert!(rows(&f.alice).is_empty());
        }
    }
}

/// Alice's newly created-and-deleted parent cannot authorise her child, while an
/// already accepted parent remains evidence when deleted in that same unit.
#[test]
fn ordinary_exists_distinguishes_deleted_new_and_accepted_evidence() {
    for accepted in [false, true] {
        let f = Fixture::new(insert(parent(false)));
        f.connect();
        if accepted {
            let tx = f.commit(&[(2, cells(2, true, true, "room"))], false);
            f.settle(tx).unwrap();
        }
        let tx = OpenTransactionId::new();
        block_on(f.alice.begin_mergeable(tx)).unwrap();
        let staged = f.alice.mergeable_tx_ref(tx);
        if !accepted {
            block_on(staged.upsert(
                "nodes",
                row(2),
                cells(2, true, true, "room"),
                Default::default(),
            ))
            .unwrap();
        }
        block_on(staged.upsert(
            "nodes",
            row(1),
            cells(2, false, true, "room"),
            Default::default(),
        ))
        .unwrap();
        block_on(staged.delete("nodes", row(2), Default::default())).unwrap();
        let receipt = block_on(f.alice.commit_mergeable_handle(tx)).unwrap();
        let settled = f.settle(receipt.0);
        if accepted {
            settled.unwrap();
            assert_eq!(rows(&f.bob), vec![(row(1), "room".into())]);
        } else {
            assert!(settled.unwrap_err().is_authorization_denied());
            assert!(rows(&f.bob).is_empty());
        }
    }
}

/// Bob evaluates Alice's ordinary child against the final surviving parent
/// update, so a same-unit scope revocation cannot be hidden by staging order.
#[test]
fn ordinary_exists_revalidates_updated_evidence_in_both_orders() {
    for child_first in [false, true] {
        let f = Fixture::new(insert(parent(false)));
        f.connect();
        let tx = f.commit(&[(2, cells(2, true, true, "room"))], false);
        f.settle(tx).unwrap();
        let mut values = vec![
            (1, cells(2, false, true, "room")),
            (2, cells(2, true, true, "revoked")),
        ];
        if !child_first {
            values.reverse();
        }
        let tx = f.commit(&values, false);
        assert!(f.settle(tx).unwrap_err().is_authorization_denied());
        assert_eq!(rows(&f.bob), vec![(row(2), "room".into())]);
    }
}

/// Alice's callback collects ordinary proofs until commit. An explicit error or
/// a cancelled callback discards its rows and proofs; Bob's next unit can run.
#[test]
fn ordinary_callback_failure_and_cancellation_leave_no_rows_or_pending_proofs() {
    for cancel in [false, true] {
        let db = open(schema(insert(parent(false))), 2, true);
        if cancel {
            let mut transaction = Box::pin(db.transaction(async |tx| {
                tx.upsert(
                    "nodes",
                    row(1),
                    cells(1, true, true, "cancelled"),
                    Default::default(),
                )
                .await?;
                std::future::pending::<()>().await;
                Ok(())
            }));
            assert!(
                transaction
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            drop(transaction);
        } else {
            let result: Result<((), _), _> = block_on(db.transaction(async |tx| {
                tx.upsert(
                    "nodes",
                    row(1),
                    cells(1, true, true, "aborted"),
                    Default::default(),
                )
                .await?;
                Err(Error::new(
                    jazz::db::ErrorCode::WriteRejected,
                    "application abort",
                ))
            }));
            assert!(result.is_err());
        }
        assert!(rows(&db).is_empty());
        block_on(db.transaction(async |tx| {
            tx.upsert(
                "nodes",
                row(2),
                cells(3, false, true, "room"),
                Default::default(),
            )
            .await?;
            tx.upsert(
                "nodes",
                row(3),
                cells(3, true, true, "room"),
                Default::default(),
            )
            .await?;
            Ok(())
        }))
        .unwrap();
        assert_eq!(
            rows(&db),
            vec![(row(2), "room".into()), (row(3), "room".into())]
        );
    }
}

/// Bob's callback cannot authorise Alice's child using a parent it creates and
/// then deletes. The rejected whole unit must leave the authority unchanged.
#[test]
fn ordinary_callback_rejects_a_deleted_new_parent_atomically() {
    let db = open(schema(insert(parent(false))), 2, true);
    let result = block_on(db.transaction(async |_tx| {
        db.upsert(
            "nodes",
            row(2),
            cells(2, true, true, "room"),
            jazz::db::UpsertOptions {
                identity: jazz::db::WriteIdentity::Session(author("bob")),
                ..Default::default()
            },
        )
        .await?;
        db.upsert(
            "nodes",
            row(1),
            cells(2, false, true, "room"),
            jazz::db::UpsertOptions {
                identity: jazz::db::WriteIdentity::Session(author("bob")),
                ..Default::default()
            },
        )
        .await?;
        db.delete(
            "nodes",
            row(2),
            jazz::db::DeleteOptions {
                identity: jazz::db::WriteIdentity::Session(author("bob")),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }));
    assert!(result.unwrap_err().is_authorization_denied());
    assert!(rows(&db).is_empty());
}
