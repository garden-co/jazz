use std::collections::BTreeMap;

mod common;

use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::{ArraySubquery, OrderDirection, Query, col, eq, param};
use jazz::result_tree::ResultRelation;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("parents")
                    .column("title", ColumnType::Text)
                    .column("rank", ColumnType::Integer)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("children")
                    .fk_column("parent_id", "parents")
                    .column("label", ColumnType::Text)
                    .column("rank", ColumnType::Integer)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("grandchildren")
                    .fk_column("child_id", "children")
                    .column("label", ColumnType::Text)
                    .column("rank", ColumnType::Integer)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open_db() -> Db<TestStorage> {
    let schema = schema();
    let column_families = schema.column_families();
    let references = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            TestStorage::new(&references),
            DbIdentity {
                node: NodeUuid::from_bytes([0x71; 16]),
                author: AuthorSubject::for_test_bytes([0x72; 16]),
            },
        )
        .with_id_source(SeededRowIdSource::new(73)),
    ))
    .expect("open test database")
}

fn child_query(bound: ArraySubquery) -> Query {
    Query::from("parents")
        .order_by("rank", OrderDirection::Asc)
        .array_subquery(bound)
}

fn children<'a>(
    node: &'a jazz::result_tree::ResultNode,
    name: &str,
) -> &'a [jazz::result_tree::ResultNode] {
    let Some(ResultRelation::Array(children)) = node.relations.get(name) else {
        panic!("expected array relation {name}");
    };
    children
}

#[test]
fn nested_tree_preserves_projection_order_offset_and_reset() {
    let db = open_db();
    let parent = block_on(db.insert(
        "parents",
        BTreeMap::from([
            ("title".to_owned(), Value::String("parent".to_owned())),
            ("rank".to_owned(), Value::I32(0)),
        ]),
        Default::default(),
    ))
    .expect("insert parent")
    .row_uuid();
    let mut child_ids = Vec::new();
    for (rank, label) in [(0, "first"), (1, "second"), (2, "third")] {
        child_ids.push(
            block_on(db.insert(
                "children",
                BTreeMap::from([
                    ("parent_id".to_owned(), Value::Uuid(parent.0)),
                    ("label".to_owned(), Value::String(label.to_owned())),
                    ("rank".to_owned(), Value::I32(rank)),
                ]),
                Default::default(),
            ))
            .expect("insert child")
            .row_uuid(),
        );
    }
    for (rank, label) in [(0, "hidden"), (1, "visible")] {
        block_on(db.insert(
            "grandchildren",
            BTreeMap::from([
                ("child_id".to_owned(), Value::Uuid(child_ids[1].0)),
                ("label".to_owned(), Value::String(label.to_owned())),
                ("rank".to_owned(), Value::I32(rank)),
            ]),
            Default::default(),
        ))
        .expect("insert grandchild");
    }

    let query = child_query(
        ArraySubquery::new("children", "children", "parent_id", "id")
            .select(["label", "rank"])
            .order_by("rank", OrderDirection::Desc)
            .offset(1)
            .limit(2)
            .nested(
                ArraySubquery::new("grandchildren", "grandchildren", "child_id", "id")
                    .select(["label", "rank"])
                    .offset(1)
                    .limit(1),
            ),
    )
    .array_subquery(
        ArraySubquery::new("empty", "children", "parent_id", "id")
            .select(["label"])
            .limit(0),
    )
    .array_subquery(
        ArraySubquery::new("ordered_children", "children", "parent_id", "id")
            .select(["label", "rank"])
            .order_by("rank", OrderDirection::Asc),
    );
    let prepared = db.prepare_query(&query).expect("prepare finite tree");
    let tree = block_on(db.all_result_tree(&prepared, ReadOpts::default())).expect("read tree");

    assert_eq!(tree.roots.len(), 1);
    assert_eq!(tree.roots[0].row.row_uuid(), parent);
    let selected_children = children(&tree.roots[0], "children");
    assert_eq!(
        selected_children
            .iter()
            .map(|child| child.row.row_uuid())
            .collect::<Vec<_>>(),
        vec![child_ids[1], child_ids[0]]
    );
    assert!(children(&tree.roots[0], "empty").is_empty());
    assert_eq!(
        children(&tree.roots[0], "ordered_children")
            .iter()
            .map(|child| child.row.row_uuid())
            .collect::<Vec<_>>(),
        child_ids
    );
    assert_eq!(children(&selected_children[0], "grandchildren").len(), 1);
    assert_eq!(
        children(&selected_children[0], "grandchildren")[0]
            .row
            .cell_at(0),
        Some(Value::String("visible".to_owned()))
    );

    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default()))
        .expect("open local maintained subscription");
    let SubscriptionEvent::Delta {
        reset: true, added, ..
    } = block_on(subscription.next_event()).expect("maintained reset")
    else {
        panic!("expected maintained reset");
    };
    assert_eq!(added.len(), 1, "Groove emits one complete terminal parent");
    assert_eq!(added[0].row_uuid(), parent);
    let (descriptor, raw) = added[0].encoded_record();
    let children_idx = descriptor
        .field_index("children")
        .expect("terminal relation field");
    let Value::Array(children) = descriptor
        .bind(raw)
        .get_idx(children_idx)
        .expect("decode terminal relation")
    else {
        panic!("expected terminal child array");
    };
    assert_eq!(children.len(), 2);
    let child_ids_from_terminal = children
        .into_iter()
        .map(|value| match value {
            Value::Record(child) => child.get_idx(0).expect("terminal child id"),
            other => panic!("expected terminal child record, got {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        child_ids_from_terminal,
        vec![Value::Uuid(child_ids[1].0), Value::Uuid(child_ids[0].0)]
    );
}

#[test]
fn maintained_array_subscription_with_root_parameter_lowers_and_delivers() {
    let db = open_db();
    let matching_parent = block_on(db.insert(
        "parents",
        BTreeMap::from([
            ("title".to_owned(), Value::String("matching".to_owned())),
            ("rank".to_owned(), Value::I32(7)),
        ]),
        Default::default(),
    ))
    .expect("insert matching parent")
    .row_uuid();
    block_on(db.insert(
        "parents",
        BTreeMap::from([
            ("title".to_owned(), Value::String("other".to_owned())),
            ("rank".to_owned(), Value::I32(8)),
        ]),
        Default::default(),
    ))
    .expect("insert non-matching parent");
    let initial_child = block_on(db.insert(
        "children",
        BTreeMap::from([
            ("parent_id".to_owned(), Value::Uuid(matching_parent.0)),
            ("label".to_owned(), Value::String("initial".to_owned())),
            ("rank".to_owned(), Value::I32(0)),
        ]),
        Default::default(),
    ))
    .expect("insert initial child")
    .row_uuid();

    let query = Query::from("parents")
        .filter(eq(col("rank"), param("rank")))
        .array_subquery(
            ArraySubquery::new("children", "children", "parent_id", "id").select(["label"]),
        );
    let prepared = db
        .prepare_query_bound(&query, BTreeMap::from([("rank".to_owned(), Value::I32(7))]))
        .expect("prepare parameter-routed array query");
    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default()))
        .expect("open parameter-routed maintained array subscription");

    let SubscriptionEvent::Delta {
        reset: true, added, ..
    } = block_on(subscription.next_event()).expect("initial maintained reset")
    else {
        panic!("expected initial maintained reset");
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row_uuid(), matching_parent);
    let (descriptor, raw) = added[0].encoded_record();
    let Value::Array(children) = descriptor
        .bind(raw)
        .get("children")
        .expect("decode terminal children")
    else {
        panic!("expected terminal child array");
    };
    assert_eq!(children.len(), 1);
    let Value::Record(child) = &children[0] else {
        panic!("expected terminal child record");
    };
    assert_eq!(child.get_idx(0), Ok(Value::Uuid(initial_child.0)));

    let _added_child = block_on(db.insert(
        "children",
        BTreeMap::from([
            ("parent_id".to_owned(), Value::Uuid(matching_parent.0)),
            ("label".to_owned(), Value::String("later".to_owned())),
            ("rank".to_owned(), Value::I32(1)),
        ]),
        Default::default(),
    ))
    .expect("insert later child")
    .row_uuid();
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        terminal_operations,
        ..
    } = block_on(subscription.next_event()).expect("maintained terminal delivery")
    else {
        panic!("expected incremental maintained delta");
    };
    assert!(!reset);
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert!(matches!(
        terminal_operations.as_slice(),
        [jazz::groove::ivm::TerminalOperation {
            path,
            edit: jazz::groove::ivm::TerminalEdit::Insert { index: 1, .. },
            ..
        }] if path == &[jazz::groove::ivm::TerminalPathSegment::Collection(
            "children".to_owned()
        )]
    ));
}

#[test]
fn omitted_array_limit_is_unbounded_for_prepare_read_and_subscribe() {
    let db = open_db();
    let parent = block_on(db.insert(
        "parents",
        BTreeMap::from([
            ("title".to_owned(), Value::String("parent".to_owned())),
            ("rank".to_owned(), Value::I32(0)),
        ]),
        Default::default(),
    ))
    .expect("insert parent")
    .row_uuid();
    block_on(db.insert(
        "children",
        BTreeMap::from([
            ("parent_id".to_owned(), Value::Uuid(parent.0)),
            ("label".to_owned(), Value::String("child".to_owned())),
            ("rank".to_owned(), Value::I32(0)),
        ]),
        Default::default(),
    ))
    .expect("insert child");

    let unbounded_query = child_query(ArraySubquery::new(
        "children",
        "children",
        "parent_id",
        "id",
    ));
    let nested_unbounded = child_query(
        ArraySubquery::new("children", "children", "parent_id", "id")
            .limit(1)
            .nested(ArraySubquery::new(
                "grandchildren",
                "grandchildren",
                "child_id",
                "id",
            )),
    );
    db.prepare_query(&nested_unbounded)
        .expect("prepare nested omitted limit");

    let zero = db
        .prepare_query(&child_query(
            ArraySubquery::new("children", "children", "parent_id", "id").limit(0),
        ))
        .expect("prepare limit zero");
    let zero_tree =
        block_on(db.all_result_tree(&zero, ReadOpts::default())).expect("read limit zero");
    assert!(children(&zero_tree.roots[0], "children").is_empty());

    let unbounded = db
        .prepare_query(&unbounded_query)
        .expect("prepare unbounded");
    assert_eq!(
        children(
            &block_on(db.all_result_tree(&unbounded, ReadOpts::default()))
                .expect("read unbounded")
                .roots[0],
            "children"
        )
        .len(),
        1
    );
    let _subscription =
        block_on(db.subscribe(&unbounded, ReadOpts::default())).expect("subscribe unbounded");
}

#[test]
fn large_parent_is_materialized_atomically_without_a_frame_bound() {
    let db = open_db();
    let parent = block_on(db.insert(
        "parents",
        BTreeMap::from([
            ("title".to_owned(), Value::String("parent".to_owned())),
            ("rank".to_owned(), Value::I32(0)),
        ]),
        Default::default(),
    ))
    .expect("insert parent")
    .row_uuid();
    let payload = "x".repeat(jazz::protocol_limits::MAX_WIRE_FRAME_BYTES + 1024);
    for rank in 0..2 {
        block_on(db.insert(
            "children",
            BTreeMap::from([
                ("parent_id".to_owned(), Value::Uuid(parent.0)),
                ("label".to_owned(), Value::String(payload.clone())),
                ("rank".to_owned(), Value::I32(rank)),
            ]),
            Default::default(),
        ))
        .expect("insert individually valid child");
    }
    let prepared = db
        .prepare_query(&child_query(
            ArraySubquery::new("children", "children", "parent_id", "id")
                .select(["label", "rank"])
                .limit(2),
        ))
        .expect("prepare finite children");

    let tree = block_on(db.all_result_tree(&prepared, ReadOpts::default()))
        .expect("large logical parent is not constrained by a physical frame");
    assert_eq!(tree.roots.len(), 1);
    assert_eq!(children(&tree.roots[0], "children").len(), 2);
    block_on(db.subscribe(&prepared, ReadOpts::default()))
        .expect("large reset remains one atomic logical result");
}
