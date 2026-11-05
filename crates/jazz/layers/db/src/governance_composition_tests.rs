//! Main-only admission-context regressions. No public pin-selection test API.
use crate::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, SubscriptionStream, block_on,
};
use crate::ids::{AuthorSubject, NodeUuid, RowUuid};
use crate::query::{ArraySubquery, Query};
use crate::records::Value;
use crate::schema::JazzSchema;
use jazz_model::model::public_schema::{
    ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use std::collections::BTreeMap;
fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([17; 16])
}
fn policies(p: PolicyExpr) -> TablePolicies {
    TablePolicies::new()
        .with_select(p)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
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

fn admitted_view(db: &Db, schema: &JazzSchema) -> (Db, Box<dyn super::Transport>) {
    let bundle = block_on(db.publish_bundle(schema.public_schema())).unwrap();
    let (peer, transport) = super::duplex();
    let claims = db
        .inner
        .borrow()
        .claims
        .get(&alice())
        .cloned()
        .unwrap_or_default();
    let subscriber = db
        .accept_pinned_subscriber(transport, alice(), Some(claims), Some(bundle))
        .unwrap();
    (db.session_db(subscriber.slot), peer)
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
            SubscriptionEvent::Rejected { reason, .. } => panic!("{reason}"),
            _ => {}
        }
    }
    row
}
/// Alice's selected pin restricts nested children by a support table. Bob
/// revokes only support; the subscription must evict the now-hidden child.
/// This lower-crate test needs the private session handle after real subscriber
/// admission: main intentionally exposes no public unauthenticated pin selector.
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
    let (selected, _transport) = admitted_view(&db, &restricted);
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
/// This lower-crate test needs access to a real admitted session's private Db
/// handle to compare its live result with a fresh one under exactly the same pin.
#[test]
fn recursive_child_filters_and_policy_support_follow_executing_context() {
    use crate::query::{col, eq, lit};
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
        let admitted = pinned.then(|| admitted_view(&db, &restricted));
        let retained;
        let executing = if let Some((selected, _)) = &admitted {
            selected
        } else {
            retained = block_on(db.register_schema_view(deep_schema(PolicyExpr::True))).unwrap();
            &retained
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

mod optional_include_nested {

    use crate::db::{
        CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, SubscriptionStream,
        block_on,
    };
    use crate::ids::{AuthorSubject, NodeUuid, RowUuid};
    use crate::query::{ArraySubquery, Query, col, eq, lit};
    use crate::records::Value;
    use crate::schema::JazzSchema;
    use jazz_model::model::public_schema::{
        ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
    };
    use std::collections::BTreeMap;

    fn schema_for(read: PolicyExpr, deep: bool) -> JazzSchema {
        JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("parents")
                        .fk_column("support", "support")
                        .fk_column("extra", "extra")
                        .policies(allow(PolicyExpr::True)),
                )
                .table(
                    TableSchemaBuilder::new("children")
                        .fk_column("parent", "parents")
                        .column("body", ColumnType::Text)
                        .policies(allow(if deep { PolicyExpr::True } else { read.clone() })),
                )
                .table(
                    TableSchemaBuilder::new("grandchildren")
                        .fk_column("child", "children")
                        .column("body", ColumnType::Text)
                        .policies(allow(if deep { read } else { PolicyExpr::True })),
                )
                .table(
                    TableSchemaBuilder::new("support")
                        .column("name", ColumnType::Text)
                        .policies(allow(PolicyExpr::True)),
                )
                .table(
                    TableSchemaBuilder::new("extra")
                        .column("name", ColumnType::Text)
                        .policies(allow(PolicyExpr::True)),
                )
                .build(),
        )
        .unwrap()
    }
    fn allow(read: PolicyExpr) -> TablePolicies {
        TablePolicies::new()
            .with_select(read)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True)
    }
    fn support_policy() -> PolicyExpr {
        PolicyExpr::Exists {
            table: "support".into(),
            condition: Box::new(PolicyExpr::True),
        }
    }
    fn id(n: u8) -> RowUuid {
        RowUuid::from_bytes([n; 16])
    }
    fn reader() -> AuthorSubject {
        super::alice()
    }
    fn setup(read: PolicyExpr, deep: bool) -> Db {
        let db = block_on(Db::open(DbConfig::new(
            schema_for(read, deep),
            DbIdentity {
                node: NodeUuid::from_bytes([0x62; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        db.set_authority();
        let seed = |table: &str, n, cells: Vec<(&str, Value)>| {
            db.seed_settled_mergeable_for_bootstrap(
                table,
                id(n),
                AuthorSubject::SYSTEM,
                cells.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
            )
            .unwrap();
        };
        seed(
            "support",
            3,
            vec![("name", Value::String("visible".into()))],
        );
        seed("extra", 4, vec![("name", Value::String("extra".into()))]);
        seed(
            "parents",
            1,
            vec![
                ("support", Value::Uuid(id(3).0)),
                ("extra", Value::Uuid(id(4).0)),
            ],
        );
        for (n, body) in [(10, "keep"), (11, "hide")] {
            seed(
                "children",
                n,
                vec![
                    ("parent", Value::Uuid(id(1).0)),
                    ("body", Value::String(body.into())),
                ],
            );
        }
        for (n, body) in [(20, "keep"), (21, "hide")] {
            seed(
                "grandchildren",
                n,
                vec![
                    ("child", Value::Uuid(id(10).0)),
                    ("body", Value::String(body.into())),
                ],
            );
        }
        db
    }
    fn query_for(filtered: bool, deep: bool) -> Query {
        let mut children = ArraySubquery::new("children", "children", "parent", "id");
        if filtered {
            children = children.filter(eq(col("body"), lit("keep")));
        }
        if deep {
            let mut grandchildren =
                ArraySubquery::new("grandchildren", "grandchildren", "child", "id");
            if filtered {
                grandchildren = grandchildren.filter(eq(col("body"), lit("keep")));
            }
            children = children.nested(grandchildren);
        }
        Query::from("parents")
            .include("support")
            .include("extra")
            .array_subquery(children)
    }
    fn drain(stream: &mut SubscriptionStream, mut last: Option<CurrentRow>) -> CurrentRow {
        while let Some(event) = stream.try_next_event() {
            match event {
                SubscriptionEvent::Delta {
                    reset,
                    added,
                    updated,
                    removed,
                    ..
                } => {
                    assert!(removed.is_empty(), "support must not remove the parent");
                    if reset {
                        last = None;
                    }
                    for output in added.into_iter().chain(updated) {
                        assert_eq!(output.row.row_uuid(), id(1), "one unchanged parent");
                        last = Some(output.row);
                    }
                }
                SubscriptionEvent::Rejected { reason, .. } => panic!("{reason}"),
                _ => {}
            }
        }
        last.expect("parent remains visible")
    }
    fn children(row: &CurrentRow) -> Value {
        row.cell("children").unwrap()
    }
    fn leaves(value: &Value, deep: bool) -> usize {
        let Value::Array(rows) = value else {
            panic!("expected array: {value:?}")
        };
        if !deep {
            return rows.len();
        }
        rows.iter()
            .map(|child| {
                let Value::Record(record) = child else {
                    panic!("expected child record")
                };
                let values = record.to_values().unwrap();
                let Some(Value::Array(grandchildren)) = values.last() else {
                    panic!("expected grandchildren")
                };
                grandchildren.len()
            })
            .sum()
    }
    fn fresh(db: &Db, query: &Query) -> CurrentRow {
        // Exactly one parent: limit2 is equivalent but has a distinct group key.
        let prepared = db.prepare_query(&query.clone().limit(2)).unwrap();
        let mut independent =
            block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), reader())).unwrap();
        drain(&mut independent, None)
    }
    fn unrelated_include_stays_output_only(
        db: &Db,
        stream: &mut SubscriptionStream,
        last: CurrentRow,
    ) -> CurrentRow {
        let before = db.fanout_stats();
        let expected = children(&last);
        block_on(db.update(
            "extra",
            id(4),
            BTreeMap::from([("name".into(), Value::String("changed".into()))]),
            Default::default(),
        ))
        .unwrap();
        let after = db.fanout_stats();
        assert_eq!(
            after.woken,
            before.woken + 1,
            "the include must really wake its group"
        );
        assert_eq!(
            after.reran, before.reran,
            "unrelated include retains the fast path"
        );
        assert_eq!(after.rechecked, before.rechecked);
        let last = drain(stream, Some(last));
        assert_eq!(children(&last), expected);
        last
    }
    fn revoke(
        db: &Db,
        executing: &Db,
        query: &Query,
        mut stream: SubscriptionStream,
        initial: CurrentRow,
        deep: bool,
    ) {
        let previous = children(&initial);
        let last = unrelated_include_stays_output_only(db, &mut stream, initial);
        block_on(db.delete("support", id(3), Default::default())).unwrap();
        let expected = fresh(executing, query);
        assert_eq!(
            leaves(&children(&expected), deep),
            0,
            "fresh same-context oracle refuses forbidden contents"
        );
        assert_ne!(
            children(&expected),
            previous,
            "the oracle must really change"
        );
        let actual = drain(&mut stream, Some(last));
        if deep {
            let Value::Array(before) = previous else {
                unreachable!()
            };
            let Value::Array(after) = children(&actual) else {
                unreachable!()
            };
            assert_eq!(
                after.len(),
                before.len(),
                "intermediate children stay visible"
            );
        }
        assert_eq!(
            children(&actual),
            children(&expected),
            "optional include must not replay forbidden nested content"
        );
    }

    fn selected_case(filtered: bool, deep: bool) {
        // The owner refuses children/grandchildren; Alice's actual authenticated
        // bundle permits them only while support exists. Keep admission alive.
        let db = setup(PolicyExpr::False, deep);
        let (selected, _transport) = super::admitted_view(&db, &schema_for(support_policy(), deep));
        let query = query_for(filtered, deep);
        let prepared = db.prepare_query(&query).unwrap();
        let mut stream =
            block_on(selected.subscribe_for_identity(&prepared, ReadOpts::default(), reader()))
                .unwrap();
        let mut last = drain(&mut stream, None);
        assert_eq!(leaves(&children(&last), deep), if filtered { 1 } else { 2 });
        assert_eq!(
            leaves(&children(&fresh(&db, &query)), deep),
            0,
            "opposite owner policy must actually differ"
        );
        // Unrelated owner policy changes cannot replace Alice's selected bundle.
        for read in [PolicyExpr::True, PolicyExpr::False] {
            block_on(db.activate_schema(schema_for(read, deep))).unwrap();
            last = drain(&mut stream, Some(last));
            assert_eq!(leaves(&children(&last), deep), if filtered { 1 } else { 2 });
        }
        // Collect the dropped oracle's group before asserting precise wake stats.
        block_on(db.tick()).unwrap();
        revoke(&db, &selected, &query, stream, last, deep);
    }
    /// Alice's admitted pin loses child access on support revocation. Lower-crate
    /// placement is needed for the real private session handle; no public pin API exists.
    #[test]
    fn optional_include_nested_selected_policy() {
        selected_case(false, false);
    }
    /// Alice's admitted pin filters children and rechecks SELECT support. This
    /// lower-crate test uses the actual private admitted Db, not a constructed pin.
    #[test]
    fn optional_include_nested_selected_filtered_policy() {
        selected_case(true, false);
    }
    /// Alice's pin retains intermediate rows but removes forbidden grandchildren.
    /// Private real-admission access requires a lower-crate test; the transport stays live.
    #[test]
    fn optional_include_nested_selected_deep_filtered_policy() {
        selected_case(true, true);
    }
}
