//! Affected-parent routing uses the same execution context and refreshed metadata
//! as the ordinary maintained query, including its conservative fallback.
use crate::common::{allow_all_policies, compile_schema, exists, read_and_allow_all_writes};
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriptionEvent, SubscriptionStream,
    block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, Query};
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;

fn schema(child_policy: PolicyExpr) -> jazz::tools::public_schema::Schema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("parents")
                .column("body", ColumnType::Text)
                .policies(allow_all_policies()),
        )
        .table(
            TableSchemaBuilder::new("children")
                .fk_column("parent", "parents")
                .column("body", ColumnType::Text)
                .policies(read_and_allow_all_writes(child_policy)),
        )
        .build()
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([17; 16])
}
fn insert(db: &Db, table: &str, n: u8, cells: &[(&str, Value)]) {
    block_on(
        db.insert(
            table,
            cells
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            InsertOptions {
                row_id: Some(id(n)),
                ..Default::default()
            },
        ),
    )
    .unwrap();
}
fn setup() -> Db {
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(&schema(PolicyExpr::True)),
        DbIdentity {
            node: NodeUuid::from_bytes([9; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    insert(
        &db,
        "parents",
        1,
        &[("body", Value::String("parent".into()))],
    );
    insert(&db, "parents", 2, &[("body", Value::String("gate".into()))]);
    insert(
        &db,
        "children",
        3,
        &[
            ("parent", Value::Uuid(id(1).0)),
            ("body", Value::String("child".into())),
        ],
    );
    db
}
fn query(db: &Db) -> jazz::db::PreparedQuery {
    db.prepare_query(
        &Query::from("parents")
            .array_subquery(ArraySubquery::new("children", "children", "parent", "id")),
    )
    .unwrap()
}
fn drain(stream: &mut SubscriptionStream, rows: &mut BTreeMap<RowUuid, usize>) {
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } => {
                if reset {
                    rows.clear();
                }
                for row in removed {
                    rows.remove(&row.row_uuid);
                }
                for row in added.into_iter().chain(updated) {
                    let Some(Value::Array(children)) = row.cell("children") else {
                        panic!("missing children: {row:?}")
                    };
                    rows.insert(row.row_uuid(), children.len());
                }
            }
            SubscriptionEvent::Rejected { reason, .. } => panic!("{reason}"),
            _ => {}
        }
    }
}
// `all_for_identity` returns roots only. Read each direct child correlation
// independently through that API rather than borrowing a maintained group.
fn fresh(db: &Db, query: &jazz::db::PreparedQuery) -> BTreeMap<RowUuid, usize> {
    use jazz::query::{col, eq, lit};
    block_on(db.all_for_identity(query, ReadOpts::default(), alice()))
        .unwrap()
        .into_iter()
        .map(|row| {
            let children = db
                .prepare_query(
                    &Query::from("children")
                        .filter(eq(col("parent"), lit(Value::Uuid(row.row_uuid().0)))),
                )
                .unwrap();
            let count = block_on(db.all_for_identity(&children, ReadOpts::default(), alice()))
                .unwrap()
                .len();
            (row.row_uuid(), count)
        })
        .collect()
}

/// Alice keeps names from the old schema. Owner activation revokes child reads
/// for that handle while an explicitly selected published bundle retains them.
#[test]
fn array_parent_context_tracks_current_owner_and_selected_bundle() {
    let db = setup();
    let initial = compile_schema(&schema(PolicyExpr::True));
    let retained = block_on(db.register_schema_view(initial.clone())).unwrap();
    let bundle = block_on(db.publish_bundle(&schema(PolicyExpr::True))).unwrap();
    let selected = db
        .pinned_view(initial.version_id(), Some(&bundle))
        .unwrap()
        .unwrap();
    let prepared = query(&retained);
    let mut owner_stream =
        block_on(retained.subscribe_for_identity(&prepared, Default::default(), alice())).unwrap();
    let mut selected_stream =
        block_on(selected.subscribe_for_identity(&prepared, Default::default(), alice())).unwrap();
    let mut owner_rows = BTreeMap::new();
    let mut selected_rows = BTreeMap::new();
    drain(&mut owner_stream, &mut owner_rows);
    drain(&mut selected_stream, &mut selected_rows);
    assert_eq!(owner_rows[&id(1)], 1);
    assert_eq!(selected_rows[&id(1)], 1);
    block_on(db.activate_schema(compile_schema(&schema(PolicyExpr::False)))).unwrap();
    drain(&mut owner_stream, &mut owner_rows);
    drain(&mut selected_stream, &mut selected_rows);
    assert_eq!(
        owner_rows[&id(1)],
        0,
        "retained names cannot retain old owner permissions"
    );
    assert_eq!(
        selected_rows[&id(1)],
        1,
        "selected bundle retains its own policy"
    );
    insert(
        &db,
        "children",
        4,
        &[
            ("parent", Value::Uuid(id(1).0)),
            ("body", Value::String("later".into())),
        ],
    );
    drain(&mut owner_stream, &mut owner_rows);
    drain(&mut selected_stream, &mut selected_rows);
    assert_eq!(owner_rows, fresh(&retained, &prepared));
    assert_eq!(selected_rows, fresh(&selected, &prepared));
    assert_eq!(owner_rows[&id(1)], 0);
    assert_eq!(selected_rows[&id(1)], 2);
}

/// Alice's child policy starts independent, then reads another parent. A write
/// to that supporting parent must refresh the first parent's children too.
#[test]
fn array_parent_activation_rebuilds_classifier_before_support_changes() {
    let db = setup();
    let prepared = query(&db);
    let mut stream =
        block_on(db.subscribe_for_identity(&prepared, Default::default(), alice())).unwrap();
    let mut rows = BTreeMap::new();
    drain(&mut stream, &mut rows);
    assert_eq!(rows[&id(1)], 1);
    let dependent = exists(
        "parents",
        vec![PolicyExpr::eq_literal(
            "body",
            jazz::tools::Value::Text("gate".into()),
        )],
    );
    block_on(db.activate_schema(compile_schema(&schema(dependent)))).unwrap();
    drain(&mut stream, &mut rows);
    assert_eq!(rows[&id(1)], 1);
    block_on(db.update(
        "parents",
        id(2),
        BTreeMap::from([("body".into(), Value::String("closed".into()))]),
        Default::default(),
    ))
    .unwrap();
    drain(&mut stream, &mut rows);
    assert_eq!(fresh(&db, &prepared)[&id(1)], 0);
    assert_eq!(
        rows,
        fresh(&db, &prepared),
        "support outside the correlation needs full refresh"
    );
}
