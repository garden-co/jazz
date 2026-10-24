//! Candidate permissions preserve the accepted graph behind relation helpers.
use jazz::db::{Db, DbConfig, DbIdentity, Error, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr, RelationJoinCondition,
    RelationJoinKind, RelationKeyRef, RelationPredicate, RelationProjectColumn,
    RelationProjectExpr, RelationRowIdRef, RelationValueRef,
};
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use std::collections::BTreeMap;

fn author(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}
fn id(n: u128) -> RowUuid {
    RowUuid(uuid::Uuid::from_u128(n))
}
fn col(table: &str, column: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some(table.into()),
        column: column.into(),
    }
}
fn scan(table: &str) -> RelationExpr {
    RelationExpr::TableScan {
        table: table.into(),
        alias: None,
    }
}
fn filter(input: RelationExpr, left: RelationColumnRef, right: RelationValueRef) -> RelationExpr {
    RelationExpr::Filter {
        input: Box::new(input),
        predicate: RelationPredicate::Cmp {
            left,
            op: RelationCmpOp::Eq,
            right,
        },
    }
}
fn project(input: RelationExpr, column: RelationColumnRef) -> RelationExpr {
    RelationExpr::Project {
        input: Box::new(input),
        columns: vec![RelationProjectColumn {
            alias: "id".into(),
            expr: RelationProjectExpr::Column(column),
        }],
    }
}
fn relation(max_depth: usize) -> RelationExpr {
    let seed = project(
        RelationExpr::Filter {
            input: Box::new(filter(
                scan("members"),
                col("members", "recipient"),
                RelationValueRef::SessionRef(vec!["user".into(), "account".into()]),
            )),
            predicate: RelationPredicate::IsNotNull {
                column: col("members", "activeAccount"),
            },
        },
        col("members", "parent"),
    );
    let step = project(
        filter(
            scan("members"),
            col("members", "child"),
            RelationValueRef::RowId(RelationRowIdRef::Frontier),
        ),
        col("members", "parent"),
    );
    let groups = RelationExpr::Gather {
        seed: Box::new(seed),
        step: Box::new(step),
        frontier_key: RelationKeyRef::RowId(RelationRowIdRef::Current),
        bound: RecursionBound::MaxDepth(max_depth),
        dedupe_key: vec![RelationKeyRef::RowId(RelationRowIdRef::Current)],
    };
    filter(
        RelationExpr::Join {
            left: Box::new(groups),
            right: Box::new(scan("access")),
            on: vec![RelationJoinCondition {
                left: RelationColumnRef {
                    scope: None,
                    column: "id".into(),
                },
                right: col("access", "group"),
            }],
            join_kind: RelationJoinKind::Inner,
        },
        col("access", "resource"),
        RelationValueRef::RowId(RelationRowIdRef::Outer),
    )
}
fn open(indexed: bool) -> Db {
    open_with_depth(indexed, 8)
}
fn open_with_depth(indexed: bool, max_depth: usize) -> Db {
    open_with_relation(indexed, relation(max_depth))
}
fn definition(indexed: bool, relation: RelationExpr) -> JazzSchema {
    let free = || {
        TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True)
    };
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("resources")
                    .column("label", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::ExistsRel {
                                rel: relation.clone(),
                            })
                            .with_insert(PolicyExpr::ExistsRel { rel: relation }),
                    ),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .column("recipient", ColumnType::Uuid)
                    .nullable_column("activeAccount", ColumnType::Uuid)
                    .fk_column("child", "groups")
                    .fk_column("parent", "groups")
                    .column("label", ColumnType::Text)
                    .index_only(if indexed {
                        vec!["recipient", "child"]
                    } else {
                        vec!["child"]
                    })
                    .policies(free()),
            )
            .table(
                TableSchemaBuilder::new("access")
                    .fk_column("resource", "resources")
                    .fk_column("group", "groups")
                    .index_only(vec!["resource", "group"])
                    .policies(free()),
            )
            .table(TableSchemaBuilder::new("groups"))
            .table(
                TableSchemaBuilder::new("markers")
                    .fk_column("resource", "resources")
                    .policies(TablePolicies::new().with_insert(
                        PolicyExpr::exists_including_created(
                            "resources",
                            PolicyExpr::eq_session(
                                "id",
                                vec!["__jazz_outer_row".into(), "resource".into()],
                            ),
                        ),
                    )),
            )
            .build(),
    )
    .unwrap();
    schema
}
fn open_with_relation(indexed: bool, relation: RelationExpr) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        definition(indexed, relation),
        DbIdentity {
            node: NodeUuid::from_bytes([73; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    db.initialize_authority_catalogue().unwrap();
    db
}
fn fields(recipient: uuid::Uuid, child: u128, parent: u128) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("recipient".into(), Value::Uuid(recipient)),
        (
            "activeAccount".into(),
            Value::Nullable((!recipient.is_nil()).then(|| Box::new(Value::Uuid(recipient)))),
        ),
        ("child".into(), Value::Uuid(id(child).0)),
        ("parent".into(), Value::Uuid(id(parent).0)),
        ("label".into(), Value::String("member".into())),
    ])
}
fn graph(db: &Db, tx: Option<OpenTransactionId>) {
    let rows = [
        (1, fields(author(1).account_id().unwrap().0, 0, 10)),
        (2, fields(id(0).0, 10, 20)),
        (3, fields(id(0).0, 20, 30)),
    ];
    for (n, cells) in rows {
        let opts = InsertOptions {
            row_id: Some(id(n)),
            ..Default::default()
        };
        if let Some(tx) = tx {
            block_on(db.mergeable_tx_ref(tx).insert("members", cells, opts)).unwrap();
        } else {
            block_on(db.insert("members", cells, opts)).unwrap();
        }
    }
    let cells = BTreeMap::from([
        ("resource".into(), Value::Uuid(id(100).0)),
        ("group".into(), Value::Uuid(id(30).0)),
    ]);
    let opts = InsertOptions {
        row_id: Some(id(4)),
        ..Default::default()
    };
    if let Some(tx) = tx {
        block_on(db.mergeable_tx_ref(tx).insert("access", cells, opts)).unwrap();
    } else {
        block_on(db.insert("access", cells, opts)).unwrap();
    }
}
fn begin(db: &Db, who: u8) -> OpenTransactionId {
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable_for_identity(tx, author(who))).unwrap();
    tx
}
fn resource(db: &Db, tx: OpenTransactionId, n: u128) {
    block_on(db.mergeable_tx_ref(tx).insert(
        "resources",
        BTreeMap::from([("label".into(), Value::String("protected".into()))]),
        InsertOptions {
            row_id: Some(id(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}
fn finish(db: &Db, tx: OpenTransactionId) -> Result<(), Error> {
    block_on(db.commit_serializable_handle(tx)).map(|_| ())
}
fn count(db: &Db, table: &str) -> usize {
    block_on(db.all(
        &db.prepare_query(&Query::from(table)).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap()
    .len()
}
fn denied(result: Result<(), Error>) {
    let e = result.unwrap_err();
    assert!(
        e.is_authorization_denied(),
        "expected permission denial, got {e}"
    );
}

/// Alice reaches her accepted ancestor group through projected keys, without
/// materialized group rows. Mallory and a different resource cannot borrow it.
#[test]
fn accepted_recursive_memberships_authorize_only_the_bound_account_and_resource() {
    for (who, resource_id, allowed) in [(1, 100, true), (2, 100, false), (1, 101, false)] {
        let db = open(true);
        graph(&db, None);
        let tx = begin(&db, who);
        resource(&db, tx, resource_id);
        if allowed {
            finish(&db, tx).unwrap();
        } else {
            denied(finish(&db, tx));
        }
        assert_eq!(count(&db, "resources"), usize::from(allowed));
    }
}

/// Alice's independently allowed membership inserts do not become accepted
/// evidence for an ordinary relation occurrence in the same transaction.
#[test]
fn recursive_relations_cannot_borrow_same_unit_created_memberships() {
    let db = open(true);
    let tx = begin(&db, 1);
    resource(&db, tx, 100);
    graph(&db, Some(tx));
    denied(finish(&db, tx));
    assert_eq!(count(&db, "resources"), 0);
    assert_eq!(count(&db, "members"), 0);
}

/// Alice cannot publish a protected resource while removing an edge needed by
/// its proof. The authority checks the final graph and rolls back both writes.
#[test]
fn deleting_an_accepted_hop_revokes_the_final_proof() {
    for (table, n) in [("members", 2), ("access", 4)] {
        let db = open(true);
        graph(&db, None);
        let tx = begin(&db, 1);
        resource(&db, tx, 100);
        block_on(
            db.mergeable_tx_ref(tx)
                .delete(table, id(n), Default::default()),
        )
        .unwrap();
        denied(finish(&db, tx));
        assert_eq!(count(&db, "resources"), 0);
        assert_eq!(count(&db, table), if table == "members" { 3 } else { 1 });
    }
}

/// Alice gets a typed unsupported error if the account seed has no declared
/// index. The authority cannot enumerate unrelated membership rows instead.
#[test]
fn an_unindexed_recursive_seed_is_rejected_without_publishing() {
    let db = open(false);
    graph(&db, None);
    let tx = begin(&db, 1);
    resource(&db, tx, 100);
    let e = finish(&db, tx).unwrap_err();
    assert!(e.to_string().contains("not supported"), "{e}");
    assert_eq!(count(&db, "resources"), 0);
}

/// Alice's proof keeps using exact accepted images when this unit updates all
/// the rows in its path; the final proof then sees the surviving current rows.
#[test]
fn accepted_historical_hops_survive_unrelated_same_unit_updates() {
    let db = open(true);
    graph(&db, None);
    let tx = begin(&db, 1);
    resource(&db, tx, 100);
    for n in [1, 2, 3] {
        block_on(db.mergeable_tx_ref(tx).update(
            "members",
            id(n),
            BTreeMap::from([("label".into(), Value::String("still a member".into()))]),
            Default::default(),
        ))
        .unwrap();
    }
    finish(&db, tx).unwrap();
    assert_eq!(count(&db, "resources"), 1);
}

/// Mallory cannot make an unrelated accepted edge authorize Alice's write by
/// retargeting it in the same unit. A final path cannot replace a missing
/// accepted-baseline proof, and both the retarget and resource roll back.
#[test]
fn retargeting_an_accepted_edge_cannot_manufacture_a_baseline_grant() {
    let db = open(true);
    graph(&db, None);
    block_on(db.update(
        "members",
        id(2),
        BTreeMap::from([("child".into(), Value::Uuid(id(99).0))]),
        Default::default(),
    ))
    .unwrap();
    let tx = begin(&db, 1);
    resource(&db, tx, 100);
    block_on(db.mergeable_tx_ref(tx).update(
        "members",
        id(2),
        BTreeMap::from([("child".into(), Value::Uuid(id(10).0))]),
        Default::default(),
    ))
    .unwrap();
    denied(finish(&db, tx));
    assert_eq!(count(&db, "resources"), 0);
    // The rejected retarget cannot leave permission available to the next unit.
    let next = begin(&db, 1);
    resource(&db, next, 100);
    denied(finish(&db, next));
}

/// Alice's cyclic graph cannot consume unbounded SQL work while proving a
/// missing grant. Exhaustion rolls back the write and removes the meter so a
/// later short proof on the same connection can still succeed.
#[test]
fn recursive_sql_work_is_bounded_and_does_not_poison_later_proofs() {
    let db = open_with_depth(true, 1_000_000);
    graph(&db, None);
    block_on(db.insert(
        "members",
        fields(id(0).0, 30, 10),
        InsertOptions {
            row_id: Some(id(5)),
            ..Default::default()
        },
    ))
    .unwrap();
    let tx = begin(&db, 1);
    resource(&db, tx, 101);
    let error = finish(&db, tx).unwrap_err();
    assert!(
        error.to_string().contains("SQL instruction budget"),
        "{error}"
    );
    assert_eq!(count(&db, "resources"), 0);
    block_on(db.delete("members", id(5), Default::default())).unwrap();
    let tx = begin(&db, 1);
    resource(&db, tx, 100);
    finish(&db, tx).unwrap();
    assert_eq!(count(&db, "resources"), 1);
}

/// Alice's account-bound membership joins an accepted resource grant.
/// Current and historical combinations both
/// preserve the resource predicate; Mallory never gains Alice's membership.
#[test]
fn joined_relation_sources_use_bound_keys_instead_of_sibling_subqueries() {
    for who in [1, 2] {
        let joined = RelationExpr::Join {
            left: Box::new(scan("access")),
            right: Box::new(filter(
                scan("members"),
                col("members", "recipient"),
                RelationValueRef::SessionRef(vec!["user".into(), "account".into()]),
            )),
            on: vec![RelationJoinCondition {
                left: col("access", "group"),
                right: col("members", "parent"),
            }],
            join_kind: RelationJoinKind::Inner,
        };
        let relation = RelationExpr::Filter {
            input: Box::new(joined),
            predicate: RelationPredicate::Cmp {
                left: col("access", "resource"),
                op: RelationCmpOp::Eq,
                right: RelationValueRef::RowId(RelationRowIdRef::Outer),
            },
        };
        let db = open_with_relation(true, relation);
        graph(&db, None);
        block_on(db.update(
            "members",
            id(1),
            BTreeMap::from([("parent".into(), Value::Uuid(id(30).0))]),
            Default::default(),
        ))
        .unwrap();
        let tx = begin(&db, who);
        resource(&db, tx, 100);
        block_on(db.mergeable_tx_ref(tx).update(
            "members",
            id(1),
            BTreeMap::from([("label".into(), Value::String("updated".into()))]),
            Default::default(),
        ))
        .unwrap();
        block_on(db.mergeable_tx_ref(tx).update(
            "access",
            id(4),
            BTreeMap::from([("resource".into(), Value::Uuid(id(100).0))]),
            Default::default(),
        ))
        .unwrap();
        if who == 1 {
            finish(&db, tx).unwrap();
        } else {
            denied(finish(&db, tx));
        }
        assert_eq!(count(&db, "resources"), usize::from(who == 1));
    }
}

/// Alice's account seed must remain active even though its stable recipient
/// key survives revocation. An accepted nullable endpoint cleared by Bob stops
/// both ordinary reads and later candidate writes through that group.
#[test]
fn clearing_the_nullable_account_endpoint_revokes_reads_and_writes() {
    let db = open(true);
    graph(&db, None);
    block_on(db.insert(
        "resources",
        BTreeMap::from([("label".into(), Value::String("secret".into()))]),
        InsertOptions {
            row_id: Some(id(100)),
            ..Default::default()
        },
    ))
    .unwrap();
    let query = db.prepare_query(&Query::from("resources")).unwrap();
    assert_eq!(
        block_on(db.all_for_identity(&query, ReadOpts::default(), author(1)))
            .unwrap()
            .len(),
        1
    );
    block_on(db.update(
        "members",
        id(1),
        BTreeMap::from([("activeAccount".into(), Value::Nullable(None))]),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(
        block_on(db.all_for_identity(&query, ReadOpts::default(), author(1)))
            .unwrap()
            .len(),
        0
    );
    block_on(db.update(
        "access",
        id(4),
        BTreeMap::from([("resource".into(), Value::Uuid(id(101).0))]),
        Default::default(),
    ))
    .unwrap();
    let tx = begin(&db, 1);
    resource(&db, tx, 101);
    denied(finish(&db, tx));
}

/// Alice has already cached a protected row when Bob revokes her group seed.
/// A later strict remote read on her replica returns the authority's empty
/// answer. Unsupported cross-row reads through a scoped backend fail closed.
#[test]
fn remote_reads_do_not_reuse_a_cached_group_grant_after_revocation() {
    use jazz::db::{ReadTier, SerializedReadResult, duplex};
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };
    fn drive<T>(server: &Db, replica: &Db, future: impl Future<Output = T>) -> T {
        let mut pending = pin!(future);
        for _ in 0..100 {
            if let Poll::Ready(value) = pending
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                return value;
            }
            block_on(server.tick()).unwrap();
            block_on(replica.tick()).unwrap();
        }
        panic!("remote read did not settle")
    }
    for scoped in [false, true] {
        let server = open(true);
        graph(&server, None);
        block_on(server.insert(
            "resources",
            BTreeMap::from([("label".into(), Value::String("secret".into()))]),
            InsertOptions {
                row_id: Some(id(100)),
                ..Default::default()
            },
        ))
        .unwrap();
        let replica = block_on(Db::open(DbConfig::new(
            definition(true, relation(8)).without_permissions(),
            DbIdentity {
                node: NodeUuid::from_bytes([27; 16]),
                author: if scoped {
                    AuthorSubject::SYSTEM
                } else {
                    author(1)
                },
            },
        )))
        .unwrap();
        let (up, down) = duplex();
        block_on(replica.connect_upstream(up));
        if scoped {
            server.accept_subscriber(down, AuthorSubject::SYSTEM);
        } else {
            server.accept_subscriber(down, author(1));
        }
        let query = postcard::to_allocvec(&Query::from("resources")).unwrap();
        let read = || {
            replica.all_serialized_query(
                &query,
                ReadOpts::for_read_tier(ReadTier::Remote),
                None,
                scoped.then(|| (author(1), BTreeMap::new())),
                scoped.then(|| author(1)),
                false,
                || false,
                |_| {},
            )
        };
        let before = drive(&server, &replica, read());
        if scoped {
            let error = before.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("scoped remote reads with cross-row permission evidence"),
                "{error}"
            );
            continue;
        }
        let SerializedReadResult::Rows(before) = before.unwrap() else {
            panic!("rows")
        };
        assert_eq!(before.len(), 1);
        block_on(server.update(
            "members",
            id(1),
            BTreeMap::from([("activeAccount".into(), Value::Nullable(None))]),
            Default::default(),
        ))
        .unwrap();
        let SerializedReadResult::Rows(after) = drive(&server, &replica, read()).unwrap() else {
            panic!("rows")
        };
        assert!(
            after.is_empty(),
            "scoped={scoped}: a revoked group grant survived the authority read"
        );
    }
}
