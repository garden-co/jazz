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
