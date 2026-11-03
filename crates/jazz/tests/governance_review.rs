use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, SubscriptionStream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::collections::BTreeMap;
fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([17; 16])
}
fn bob() -> AuthorSubject {
    AuthorSubject::for_test_bytes([18; 16])
}
fn policies(p: PolicyExpr) -> TablePolicies {
    TablePolicies::new()
        .with_select(p)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}
fn schema(p: PolicyExpr, extra: bool) -> JazzSchema {
    let mut t = TableSchemaBuilder::new("notes")
        .column("body", ColumnType::Text)
        .policies(policies(p));
    if extra {
        t = t.nullable_column("extra", ColumnType::Text);
    }
    JazzSchema::new(&SchemaBuilder::new().table(t).build()).unwrap()
}
fn open(s: JazzSchema) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        s,
        DbIdentity {
            node: NodeUuid::from_bytes([9; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    db
}
fn seed(db: &Db, t: &str, n: u8, cells: BTreeMap<String, Value>) {
    db.seed_settled_mergeable_for_bootstrap(
        t,
        RowUuid::from_bytes([n; 16]),
        AuthorSubject::SYSTEM,
        cells,
    )
    .unwrap();
}
fn claims(v: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("\0claims:body".into(), Value::String(v.into()))])
}
fn count(s: &mut SubscriptionStream, mut n: usize) -> usize {
    while let Some(e) = s.try_next_event() {
        match e {
            SubscriptionEvent::Delta {
                reset,
                added,
                removed,
                ..
            } => {
                if reset {
                    n = 0
                }
                n = n.checked_sub(removed.len()).unwrap() + added.len();
            }
            SubscriptionEvent::Rejected { reason } => panic!("{reason}"),
            _ => {}
        }
    }
    n
}
/// Alice executes a query carrying Bob's request claims. Bob's claims are ignored
/// by execution, so Alice's registry remains live and must drive revocation.
#[test]
fn review_cross_subject_request_claims_follow_effective_registry() {
    let db = open(schema(
        PolicyExpr::eq_session("body", vec!["claims".into(), "body".into()]),
        false,
    ));
    seed(
        &db,
        "notes",
        1,
        BTreeMap::from([("body".into(), Value::String("secret".into()))]),
    );
    db.set_identity_claims(alice(), claims("secret"));
    let q = db
        .prepare_query(&Query::from("notes"))
        .unwrap()
        .with_identity_claims(bob(), claims("unused"));
    let mut sub = block_on(db.subscribe_for_identity(&q, ReadOpts::default(), alice())).unwrap();
    assert_eq!(count(&mut sub, 0), 1);
    db.set_identity_claims(alice(), claims("revoked"));
    assert_eq!(
        block_on(db.all_for_identity(&q, ReadOpts::default(), alice()))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        count(&mut sub, 1),
        0,
        "subscription must follow the effective author registry, not unrelated fixed claims"
    );
}
/// Alice's root and retained-name groups converge after Bob activates the
/// retained structure. Either group's removal must preserve sharing for survivors.
#[test]
fn review_converged_group_keys_survive_either_cancellation() {
    for drop_root in [false, true] {
        let db = open(schema(PolicyExpr::True, false));
        seed(
            &db,
            "notes",
            1,
            BTreeMap::from([("body".into(), Value::String("secret".into()))]),
        );
        let view = block_on(db.register_schema_view(schema(PolicyExpr::True, true))).unwrap();
        let rootq = db.prepare_query(&Query::from("notes")).unwrap();
        let viewq = view.prepare_query(&Query::from("notes")).unwrap();
        let mut a = Some(
            block_on(db.subscribe_for_identity(&rootq, ReadOpts::default(), alice())).unwrap(),
        );
        let mut b = Some(
            block_on(view.subscribe_for_identity(&viewq, ReadOpts::default(), alice())).unwrap(),
        );
        assert_eq!(db.fanout_stats().groups, 2);
        assert_eq!(count(a.as_mut().unwrap(), 0), 1);
        assert_eq!(count(b.as_mut().unwrap(), 0), 1);
        block_on(db.activate_schema(schema(PolicyExpr::True, true))).unwrap();
        assert_eq!(count(a.as_mut().unwrap(), 1), 1);
        assert_eq!(count(b.as_mut().unwrap(), 1), 1);
        let mut fresh =
            block_on(db.subscribe_for_identity(&rootq, ReadOpts::default(), alice())).unwrap();
        assert_eq!(count(&mut fresh, 0), 1);
        if drop_root {
            drop(a.take());
        } else {
            drop(b.take());
        }
        block_on(db.tick()).unwrap();
        let mut newest =
            block_on(view.subscribe_for_identity(&viewq, ReadOpts::default(), alice())).unwrap();
        assert_eq!(count(&mut newest, 0), 1);
        block_on(db.activate_schema(schema(PolicyExpr::False, true))).unwrap();
        assert_eq!(count(&mut fresh, 1), 0);
        assert_eq!(count(&mut newest, 1), 0);
        if let Some(a) = a.as_mut() {
            assert_eq!(count(a, 1), 0)
        }
        if let Some(b) = b.as_mut() {
            assert_eq!(count(b, 1), 0)
        }
    }
}
fn array_schema(child: PolicyExpr) -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("parents")
                    .column("name", ColumnType::Text)
                    .policies(policies(PolicyExpr::True)),
            )
            .table(
                TableSchemaBuilder::new("children")
                    .fk_column("parent", "parents")
                    .column("secret", ColumnType::Text)
                    .policies(policies(child)),
            )
            .table(
                TableSchemaBuilder::new("support")
                    .column("name", ColumnType::Text)
                    .policies(policies(PolicyExpr::True)),
            )
            .build(),
    )
    .unwrap()
}
fn row_events(s: &mut SubscriptionStream, mut row: Option<CurrentRow>) -> Option<CurrentRow> {
    while let Some(e) = s.try_next_event() {
        match e {
            SubscriptionEvent::Delta {
                added,
                updated,
                removed,
                ..
            } => {
                assert!(removed.is_empty());
                for r in added {
                    row = Some(r.row)
                }
                for r in updated {
                    row = Some(r.row)
                }
            }
            SubscriptionEvent::Rejected { reason } => panic!("{reason}"),
            _ => {}
        }
    }
    row
}
/// Alice's selected pin restricts nested children by a support table. Bob
/// revokes only support; the subscription must evict the now-hidden child.
#[test]
fn review_nested_policy_support_is_watched_in_selected_context() {
    let db = open(array_schema(PolicyExpr::True));
    seed(
        &db,
        "parents",
        1,
        BTreeMap::from([("name".into(), Value::String("parent".into()))]),
    );
    seed(
        &db,
        "children",
        2,
        BTreeMap::from([
            ("parent".into(), Value::Uuid(RowUuid::from_bytes([1; 16]).0)),
            ("secret".into(), Value::String("secret".into())),
        ]),
    );
    seed(
        &db,
        "support",
        3,
        BTreeMap::from([("name".into(), Value::String("support".into()))]),
    );
    let restricted = array_schema(PolicyExpr::Exists {
        table: "support".into(),
        condition: Box::new(PolicyExpr::True),
    });
    let pin = block_on(db.publish_bundle(restricted.public_schema())).unwrap();
    db.check_session_pin(alice(), Some(&pin)).unwrap();
    let selected = db
        .pinned_view(restricted.version_id(), Some(&pin))
        .unwrap()
        .unwrap();
    let q = db
        .prepare_query(
            &Query::from("parents")
                .array_subquery(ArraySubquery::new("children", "children", "parent", "id")),
        )
        .unwrap();
    let mut stream =
        block_on(selected.subscribe_for_identity(&q, ReadOpts::default(), alice())).unwrap();
    let initial = row_events(&mut stream, None).unwrap();
    let before = initial.cell("children").unwrap();
    assert!(
        matches!(&before,Value::Array(v) if v.len()==1),
        "{before:?}"
    );
    block_on(db.delete("support", RowUuid::from_bytes([3; 16]), Default::default())).unwrap();
    let one_shot = block_on(selected.all_for_identity(&q, ReadOpts::default(), alice())).unwrap();
    assert_eq!(one_shot.len(), 1);
    // A fresh nonsharing query compiles and nests under the same pin.
    let q2 = db.prepare_query(&q.query().clone().limit(2)).unwrap();
    let mut independent =
        block_on(selected.subscribe_for_identity(&q2, ReadOpts::default(), alice())).unwrap();
    let expected = row_events(&mut independent, None)
        .unwrap()
        .cell("children")
        .unwrap();
    assert!(
        matches!(&expected,Value::Array(v) if v.is_empty()),
        "{expected:?}"
    );
    let actual = row_events(&mut stream, Some(initial))
        .unwrap()
        .cell("children")
        .unwrap();
    assert_eq!(
        actual, expected,
        "nested child policy support must invalidate the maintained result"
    );
}

/// Alice's explicit and default-author execution use the same effective claim
/// source: only matching fixed/empty scopes are immune to registry revocation.
#[test]
fn effective_claim_source_matrix_includes_default_author() {
    use jazz::query::{claim, col, eq, provider_claim_key};
    for default_author in [false, true] {
        let db = block_on(Db::open(DbConfig::new(
            schema(PolicyExpr::True, false),
            DbIdentity {
                node: NodeUuid::from_bytes([9; 16]),
                author: alice(),
            },
        )))
        .unwrap();
        db.set_authority();
        seed(
            &db,
            "notes",
            1,
            BTreeMap::from([("body".into(), Value::String("secret".into()))]),
        );
        db.set_identity_claims(alice(), claims("secret"));
        let base = db
            .prepare_query(
                &Query::from("notes").filter(eq(col("body"), claim(provider_claim_key("body")))),
            )
            .unwrap();
        let queries = [
            base.clone(),
            base.clone().with_identity_claims(bob(), claims("other")),
            base.clone().with_identity_claims(alice(), claims("secret")),
            base.with_identity_claims(alice(), BTreeMap::new()),
        ];
        let mut streams = queries
            .iter()
            .map(|query| {
                if default_author {
                    block_on(db.subscribe(query, ReadOpts::default())).unwrap()
                } else {
                    block_on(db.subscribe_for_identity(query, ReadOpts::default(), alice()))
                        .unwrap()
                }
            })
            .collect::<Vec<_>>();
        let mut counts = [0; 4];
        for (i, expected) in [1, 1, 1, 0].into_iter().enumerate() {
            counts[i] = count(&mut streams[i], 0);
            assert_eq!(counts[i], expected);
        }
        db.set_identity_claims(bob(), claims("secret"));
        db.set_identity_claims(alice(), claims("revoked"));
        for (i, expected) in [0, 0, 1, 0].into_iter().enumerate() {
            let fresh = if default_author {
                block_on(db.all(&queries[i], ReadOpts::default()))
            } else {
                block_on(db.all_for_identity(&queries[i], ReadOpts::default(), alice()))
            }
            .unwrap();
            assert_eq!(fresh.len(), expected);
            assert_eq!(
                count(&mut streams[i], counts[i]),
                expected,
                "default={default_author}, scope={i}"
            );
        }
    }
}

fn deep_schema(grandchild_policy: PolicyExpr) -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("parents")
                    .column("name", ColumnType::Text)
                    .policies(policies(PolicyExpr::True)),
            )
            .table(
                TableSchemaBuilder::new("children")
                    .fk_column("parent", "parents")
                    .column("secret", ColumnType::Text)
                    .policies(policies(PolicyExpr::True)),
            )
            .table(
                TableSchemaBuilder::new("grandchildren")
                    .fk_column("child", "children")
                    .column("body", ColumnType::Text)
                    .policies(policies(grandchild_policy)),
            )
            .table(
                TableSchemaBuilder::new("support")
                    .column("name", ColumnType::Text)
                    .policies(policies(PolicyExpr::True)),
            )
            .build(),
    )
    .unwrap()
}

/// Alice's deeper child filters and SELECT support use the selected bundle or
/// newly activated owner. A support-only change must update the nested result.
#[test]
fn recursive_child_filters_and_policy_support_follow_executing_context() {
    use jazz::query::{col, eq, lit};
    for pinned in [false, true] {
        let db = open(deep_schema(PolicyExpr::True));
        seed(
            &db,
            "parents",
            1,
            BTreeMap::from([("name".into(), Value::String("parent".into()))]),
        );
        seed(
            &db,
            "children",
            2,
            BTreeMap::from([
                ("parent".into(), Value::Uuid(RowUuid::from_bytes([1; 16]).0)),
                ("secret".into(), Value::String("secret".into())),
            ]),
        );
        seed(
            &db,
            "grandchildren",
            4,
            BTreeMap::from([
                ("child".into(), Value::Uuid(RowUuid::from_bytes([2; 16]).0)),
                ("body".into(), Value::String("secret".into())),
            ]),
        );
        seed(
            &db,
            "support",
            3,
            BTreeMap::from([("name".into(), Value::String("support".into()))]),
        );
        let restricted = deep_schema(PolicyExpr::Exists {
            table: "support".into(),
            condition: Box::new(PolicyExpr::True),
        });
        let executing = if pinned {
            let pin = block_on(db.publish_bundle(restricted.public_schema())).unwrap();
            db.check_session_pin(alice(), Some(&pin)).unwrap();
            db.pinned_view(restricted.version_id(), Some(&pin))
                .unwrap()
                .unwrap()
        } else {
            block_on(db.register_schema_view(deep_schema(PolicyExpr::True))).unwrap()
        };
        let query = db
            .prepare_query(
                &Query::from("parents").array_subquery(
                    ArraySubquery::new("children", "children", "parent", "id")
                        .filter(eq(col("secret"), lit("secret")))
                        .nested(
                            ArraySubquery::new("grandchildren", "grandchildren", "child", "id")
                                .filter(eq(col("body"), lit("secret"))),
                        ),
                ),
            )
            .unwrap();
        let mut stream =
            block_on(executing.subscribe_for_identity(&query, ReadOpts::default(), alice()))
                .unwrap();
        let mut last = row_events(&mut stream, None).unwrap();
        if !pinned {
            block_on(db.activate_schema(restricted)).unwrap();
            last = row_events(&mut stream, Some(last)).unwrap();
        }
        for action in 0..3 {
            if action < 2 {
                block_on(db.update(
                    "grandchildren",
                    RowUuid::from_bytes([4; 16]),
                    BTreeMap::from([(
                        "body".into(),
                        Value::String(if action == 0 { "hidden" } else { "secret" }.into()),
                    )]),
                    Default::default(),
                ))
                .unwrap();
            } else {
                block_on(db.delete("support", RowUuid::from_bytes([3; 16]), Default::default()))
                    .unwrap();
            }
            let fresh_query = db.prepare_query(&query.query().clone().limit(2)).unwrap();
            let mut fresh = block_on(executing.subscribe_for_identity(
                &fresh_query,
                ReadOpts::default(),
                alice(),
            ))
            .unwrap();
            let expected = row_events(&mut fresh, None).unwrap();
            let previous = last.cell("children").unwrap();
            assert_ne!(
                previous,
                expected.cell("children").unwrap(),
                "fixture must change nested contents"
            );
            last = row_events(&mut stream, Some(last)).unwrap();
            assert_eq!(
                last.cell("children"),
                expected.cell("children"),
                "pin={pinned}, action={action}"
            );
            drop(fresh);
            block_on(db.tick()).unwrap();
        }
    }
}
