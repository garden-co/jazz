//! Recursive nested policy dependencies use the same watch and include classification.
use jazz::ids::AuthorSubject;

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([17; 16])
}

mod optional_include_nested {

    use jazz::db::{
        CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, SubscriptionStream,
        block_on,
    };
    use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
    use jazz::query::{ArraySubquery, Query, col, eq, lit};
    use jazz::records::Value;
    use jazz::schema::JazzSchema;
    use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
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
                SubscriptionEvent::Rejected { reason } => panic!("{reason}"),
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

    fn owner_case(filtered: bool, deep: bool, activate: bool) {
        let db = setup(
            if activate {
                PolicyExpr::True
            } else {
                support_policy()
            },
            deep,
        );
        let retained = block_on(db.register_schema_view(schema_for(
            if activate {
                PolicyExpr::True
            } else {
                support_policy()
            },
            deep,
        )))
        .unwrap();
        let query = query_for(filtered, deep);
        let prepared = db.prepare_query(&query).unwrap();
        let mut stream =
            block_on(retained.subscribe_for_identity(&prepared, ReadOpts::default(), reader()))
                .unwrap();
        let mut last = drain(&mut stream, None);
        assert_eq!(leaves(&children(&last), deep), if filtered { 1 } else { 2 });
        if activate {
            block_on(db.activate_schema(schema_for(support_policy(), deep))).unwrap();
            last = drain(&mut stream, Some(last));
            assert_eq!(leaves(&children(&last), deep), if filtered { 1 } else { 2 });
        }
        revoke(&db, &retained, &query, stream, last, deep);
    }
    /// Alice loses child access when Bob revokes only an optionally included support row.
    #[test]
    fn optional_include_nested_owner_policy() {
        owner_case(false, false, false);
    }
    /// Alice's child-local filter and governing SELECT support both apply after Bob's revocation.
    #[test]
    fn optional_include_nested_owner_filtered_policy() {
        owner_case(true, false, false);
    }
    /// Alice retains intermediate children but loses forbidden grandchildren after support revocation.
    #[test]
    fn optional_include_nested_owner_deep_filtered_policy() {
        owner_case(true, true, false);
    }
    /// Bob changes an unrelated include; Alice receives the output update without a full query rerun.
    #[test]
    fn optional_include_nested_ordinary_include_control() {
        let db = setup(PolicyExpr::True, false);
        let query = query_for(false, false);
        let prepared = db.prepare_query(&query).unwrap();
        let mut stream =
            block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), reader())).unwrap();
        let initial = drain(&mut stream, None);
        let before = db.fanout_stats();
        block_on(db.delete("support", id(3), Default::default())).unwrap();
        let after = db.fanout_stats();
        assert_eq!(after.woken, before.woken + 1);
        assert_eq!(after.reran, before.reran);
        assert_eq!(after.rechecked, before.rechecked);
        let actual = drain(&mut stream, Some(initial));
        assert_eq!(leaves(&children(&actual), false), 2);
        assert_eq!(children(&actual), children(&fresh(&db, &query)));
    }
    /// Bob changes a child-local filter target that is also included; Alice's maintained array tracks it.
    #[test]
    fn optional_include_nested_child_filter_control() {
        let db = setup(PolicyExpr::True, false);
        let query = Query::from("parents").include("support").array_subquery(
            ArraySubquery::new("children", "support", "id", "support")
                .filter(eq(col("name"), lit("visible"))),
        );
        let prepared = db.prepare_query(&query).unwrap();
        let mut stream =
            block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), reader())).unwrap();
        let initial = drain(&mut stream, None);
        assert_eq!(leaves(&children(&initial), false), 1);
        block_on(db.update(
            "support",
            id(3),
            BTreeMap::from([("name".into(), Value::String("hidden".into()))]),
            Default::default(),
        ))
        .unwrap();
        let expected = fresh(&db, &query);
        assert_eq!(leaves(&children(&expected), false), 0);
        assert_eq!(
            children(&drain(&mut stream, Some(initial))),
            children(&expected)
        );
    }

    /// Bob revokes a child's transitive SELECT support that Alice did not include.
    /// Only recursive dependency watches can wake this unchanged root/child query.
    #[test]
    fn optional_include_nested_policy_support_without_include() {
        let db = setup(support_policy(), false);
        let query = Query::from("parents")
            .array_subquery(ArraySubquery::new("children", "children", "parent", "id"));
        let prepared = db.prepare_query(&query).unwrap();
        let mut stream =
            block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), reader())).unwrap();
        let initial = drain(&mut stream, None);
        assert_eq!(leaves(&children(&initial), false), 2);
        block_on(db.delete("support", id(3), Default::default())).unwrap();
        let expected = fresh(&db, &query);
        assert_eq!(leaves(&children(&expected), false), 0);
        let actual = drain(&mut stream, Some(initial));
        assert_eq!(
            children(&actual),
            children(&expected),
            "transitive SELECT support must invalidate the maintained children without an include"
        );
    }
}
