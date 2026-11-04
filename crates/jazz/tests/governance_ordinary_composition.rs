//! Ordinary reads execute under the handle's current authenticated governance.
use crate::common::{compile_schema, read_and_allow_all_writes};
use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::public_schema::Schema as PublicSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([17; 16])
}
fn schema(read: bool) -> PublicSchema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("body", ColumnType::Text)
                .policies(read_and_allow_all_writes(if read {
                    PolicyExpr::True
                } else {
                    PolicyExpr::False
                })),
        )
        .build()
}
fn open(node: u8, author: AuthorSubject, read: bool, authority: bool) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(&schema(read)),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap();
    if authority {
        db.set_authority();
    } else {
        db.become_replica();
    }
    db
}
fn seed(server: &Db) {
    server
        .seed_settled_mergeable_for_bootstrap(
            "notes",
            RowUuid::from_bytes([4; 16]),
            AuthorSubject::SYSTEM,
            BTreeMap::from([("body".into(), Value::String("secret".into()))]),
        )
        .unwrap();
}

fn observed_rows(stream: &mut jazz::db::SubscriptionStream, previous: usize) -> usize {
    let mut count = previous;
    while let Some(event) = stream.try_next_event() {
        match event {
            jazz::db::SubscriptionEvent::Delta {
                reset,
                added,
                removed,
                ..
            } => {
                if reset {
                    count = 0;
                }
                count = count
                    .checked_sub(removed.len())
                    .expect("removed visible rows")
                    + added.len();
            }
            jazz::db::SubscriptionEvent::Rejected { reason } => {
                panic!("subscription rejected: {reason}")
            }
            _ => {}
        }
    }
    count
}

/// Alice's initially public root becomes dependent on another root row.
/// Deleting that support must re-evaluate every dependent root, not only its ID.
#[test]
fn owner_policy_activation_rebuilds_self_table_recheck_state() {
    use crate::common::{exists, read_and_allow_all_writes, session_eq};
    use jazz::db::ReadOpts;
    use jazz::ids::RowUuid;
    use jazz::query::{col, eq, lit};
    use jazz::records::Value;
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
    use std::collections::BTreeMap;
    let server = open(9, AuthorSubject::SYSTEM, true, true);
    seed(&server);
    server
        .seed_settled_mergeable_for_bootstrap(
            "notes",
            RowUuid::from_bytes([5; 16]),
            AuthorSubject::SYSTEM,
            BTreeMap::from([("body".into(), Value::String("support".into()))]),
        )
        .unwrap();
    server.set_identity_claims(
        alice(),
        BTreeMap::from([("\0claims:support".into(), Value::String("support".into()))]),
    );
    let query = server
        .prepare_query(
            &Query::from("notes").filter(eq(col("body"), lit(Value::String("secret".into())))),
        )
        .unwrap();
    let mut stream =
        block_on(server.subscribe_for_identity(&query, ReadOpts::default(), alice())).unwrap();
    assert_eq!(observed_rows(&mut stream, 0), 1);
    let changed = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(read_and_allow_all_writes(exists(
                        "notes",
                        vec![session_eq("body", &["claims", "support"])],
                    ))),
            )
            .build(),
    );
    block_on(server.activate_schema(changed)).unwrap();
    assert_eq!(observed_rows(&mut stream, 1), 1);
    block_on(server.delete("notes", RowUuid::from_bytes([5; 16]), Default::default())).unwrap();
    assert_eq!(
        block_on(server.all_for_identity(&query, ReadOpts::default(), alice()))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        observed_rows(&mut stream, 1),
        0,
        "self-table support revocation must re-evaluate roots"
    );
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

    /// Alice's retained owner view rebuilds include classification after Bob activates a support policy.
    #[test]
    fn optional_include_nested_owner_activation() {
        owner_case(true, false, true);
    }
}
