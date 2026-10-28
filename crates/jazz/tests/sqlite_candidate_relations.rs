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
            block_on(db.tx_ref(tx).insert("members", cells, opts)).unwrap();
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
        block_on(db.tx_ref(tx).insert("access", cells, opts)).unwrap();
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
    block_on(db.tx_ref(tx).insert(
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
        block_on(db.tx_ref(tx).delete(table, id(n), Default::default())).unwrap();
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
        block_on(db.tx_ref(tx).update(
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
    block_on(db.tx_ref(tx).update(
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
        block_on(db.tx_ref(tx).update(
            "members",
            id(1),
            BTreeMap::from([("label".into(), Value::String("updated".into()))]),
            Default::default(),
        ))
        .unwrap();
        block_on(db.tx_ref(tx).update(
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

fn chunked_relation(expr: RelationExpr) -> RelationExpr {
    match expr {
        RelationExpr::TableScan { ref table, .. } if table == "members" => RelationExpr::Filter {
            input: Box::new(expr),
            predicate: RelationPredicate::And(vec![
                RelationPredicate::Cmp {
                    left: col("members", "label"),
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::Literal(serde_json::json!(format!(
                        "{}permit",
                        "x".repeat(70 * 1024)
                    ))),
                },
                RelationPredicate::Contains {
                    left: col("members", "label"),
                    right: RelationValueRef::Literal(serde_json::json!("permit")),
                },
            ]),
        },
        RelationExpr::Filter { input, predicate } => RelationExpr::Filter {
            input: Box::new(chunked_relation(*input)),
            predicate,
        },
        RelationExpr::Project { input, columns } => RelationExpr::Project {
            input: Box::new(chunked_relation(*input)),
            columns,
        },
        RelationExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => RelationExpr::Join {
            left: Box::new(chunked_relation(*left)),
            right: Box::new(chunked_relation(*right)),
            on,
            join_kind,
        },
        RelationExpr::Gather {
            seed,
            step,
            frontier_key,
            bound,
            dedupe_key,
        } => RelationExpr::Gather {
            seed: Box::new(chunked_relation(*seed)),
            step: Box::new(chunked_relation(*step)),
            frontier_key,
            bound,
            dedupe_key,
        },
        other => other,
    }
}
fn label_graph(db: &Db, label: &str) {
    graph(db, None);
    for n in 1..=3 {
        block_on(db.update(
            "members",
            id(n),
            BTreeMap::from([("label".into(), Value::String(label.into()))]),
            Default::default(),
        ))
        .unwrap();
    }
}
fn chunked_resource(db: &Db, tx: OpenTransactionId, label: &str) {
    block_on(db.tx_ref(tx).insert(
        "resources",
        BTreeMap::from([("label".into(), Value::String(label.into()))]),
        InsertOptions {
            row_id: Some(id(100)),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Alice's recursive grant compares complete 70 KiB strings, including a
/// suffix beyond the stored sort prefix. Mallory cannot borrow her graph.
/// An unrelated same-unit edit still uses the original accepted hop image.
#[test]
fn recursive_candidate_relations_prepare_chunked_seed_and_step_values() {
    let label = format!("{}permit", "x".repeat(70 * 1024));
    for (who, matches, allowed) in [(1, true, true), (1, false, false), (2, true, false)] {
        let db = open_with_relation(true, chunked_relation(relation(8)));
        let other = format!("{}denied", "x".repeat(70 * 1024));
        label_graph(&db, if matches { &label } else { &other });
        let tx = begin(&db, who);
        block_on(db.tx_ref(tx).update(
            "members",
            id(2),
            BTreeMap::from([("recipient".into(), Value::Uuid(id(99).0))]),
            Default::default(),
        ))
        .unwrap();
        chunked_resource(&db, tx, &label);
        if allowed {
            finish(&db, tx).unwrap();
        } else {
            denied(finish(&db, tx));
        }
        assert_eq!(count(&db, "resources"), usize::from(allowed));
    }
}

/// Alice cannot change a required accepted chunked hop while publishing the
/// protected resource. Bob's original grant survives the rejected unit.
#[test]
fn recursive_chunked_permission_checks_revalidate_post_state() {
    let label = format!("{}permit", "x".repeat(70 * 1024));
    let db = open_with_relation(true, chunked_relation(relation(8)));
    label_graph(&db, &label);
    let tx = begin(&db, 1);
    chunked_resource(&db, tx, &label);
    block_on(db.tx_ref(tx).update(
        "members",
        id(2),
        BTreeMap::from([(
            "label".into(),
            Value::String(format!("{}denied", "x".repeat(70 * 1024))),
        )]),
        Default::default(),
    ))
    .unwrap();
    denied(finish(&db, tx));
    assert_eq!(count(&db, "resources"), 0);
    let tx = begin(&db, 1);
    chunked_resource(&db, tx, &label);
    finish(&db, tx).unwrap();
    assert_eq!(count(&db, "resources"), 1);
}

/// Bob has an unrelated broken chunked value. Alice's bounded relation proof
/// must never read it. Raw SQLite is used only to inject that storage fault;
/// schema setup, writes and assertions use public APIs and isolated storage.
#[test]
fn recursive_chunked_preparation_does_not_read_unrelated_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("relation.sqlite");
    let config = || {
        DbConfig::new(
            definition(true, chunked_relation(relation(8))),
            DbIdentity {
                node: NodeUuid::from_bytes([99; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
    };
    {
        let db = block_on(Db::open_at(config(), &path, Default::default())).unwrap();
        db.set_authority();
        db.initialize_authority_catalogue().unwrap();
        let mut values = fields(id(90).0, 91, 92);
        values.insert("label".into(), Value::String("unrelated".repeat(10_000)));
        block_on(db.insert(
            "members",
            values,
            InsertOptions {
                row_id: Some(id(99)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        assert!(conn.execute("DELETE FROM _jazz_lv_node", []).unwrap() > 0);
    }
    let db = block_on(Db::open_at(config(), &path, Default::default())).unwrap();
    db.set_authority();
    let label = format!("{}permit", "x".repeat(70 * 1024));
    label_graph(&db, &label);
    let tx = begin(&db, 1);
    chunked_resource(&db, tx, &label);
    finish(&db, tx).unwrap();
    assert_eq!(count(&db, "resources"), 1);
}

/// Alice's non-recursive relation compares the protected root with a bound
/// accepted source by logical contents. Equal sort prefixes are insufficient.
#[test]
fn candidate_relation_preparation_compares_chunked_outer_columns() {
    for matches in [true, false] {
        let rel = RelationExpr::Filter {
            input: Box::new(filter(
                scan("members"),
                col("members", "recipient"),
                RelationValueRef::SessionRef(vec!["user".into(), "account".into()]),
            )),
            predicate: RelationPredicate::Cmp {
                left: col("members", "label"),
                op: RelationCmpOp::Eq,
                right: RelationValueRef::OuterColumn(col("resources", "label")),
            },
        };
        let db = open_with_relation(true, rel);
        let label = format!("{}permit", "x".repeat(70 * 1024));
        label_graph(&db, &label);
        let tx = begin(&db, 1);
        let other = format!("{}denied", "x".repeat(70 * 1024));
        chunked_resource(&db, tx, if matches { &label } else { &other });
        if matches {
            finish(&db, tx).unwrap();
        } else {
            denied(finish(&db, tx));
        }
        assert_eq!(count(&db, "resources"), usize::from(matches));
    }
}
