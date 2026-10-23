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
