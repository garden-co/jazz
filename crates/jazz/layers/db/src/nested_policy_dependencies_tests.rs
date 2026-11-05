//! Real subscriber admission is needed to obtain the private selected Db handle.
use crate::db::{Db, block_on};
use crate::ids::AuthorSubject;
use crate::schema::JazzSchema;

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([17; 16])
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
mod same_handle {

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

    fn selected_same_handle_case(filtered: bool, deep: bool) {
        let owner = setup(PolicyExpr::False, deep);
        let (selected, _keep_admitted_transport_alive) =
            super::admitted_view(&owner, &schema_for(support_policy(), deep));
        // The opposite owner policy must really deny the target relation. Use a
        // one-shot direct read so this control creates no shared subscription.
        let target = if deep { "grandchildren" } else { "children" };
        let owner_query = owner.prepare_query(&Query::from(target)).unwrap();
        assert!(
            block_on(owner.all_for_identity(&owner_query, ReadOpts::default(), reader()))
                .unwrap()
                .is_empty(),
            "owner policy differs from the admitted bundle"
        );
        let query = query_for(filtered, deep);
        let prepared = selected.prepare_query(&query).unwrap();
        let mut stream =
            block_on(selected.subscribe_for_identity(&prepared, ReadOpts::default(), reader()))
                .unwrap();
        let initial = drain(&mut stream, None);
        assert_eq!(
            leaves(&children(&initial), deep),
            if filtered { 1 } else { 2 }
        );
        revoke(&owner, &selected, &query, stream, initial, deep);
    }

    /// Alice prepares and reads under one real admitted bundle; Bob revokes support.
    /// Private placement gives the actual session Db without a public pin selector.
    #[test]
    fn optional_include_nested_selected_same_handle_policy() {
        selected_same_handle_case(false, false);
    }

    /// Alice's same admitted handle combines a child-local filter and SELECT support.
    /// Private placement preserves real subscriber admission and its live transport.
    #[test]
    fn optional_include_nested_selected_same_handle_filtered_policy() {
        selected_same_handle_case(true, false);
    }

    /// Alice retains intermediate children while her admitted bundle loses grandchildren.
    /// The private session handle prepares and executes both live and fresh queries.
    #[test]
    fn optional_include_nested_selected_same_handle_deep_filtered_policy() {
        selected_same_handle_case(true, true);
    }
}
