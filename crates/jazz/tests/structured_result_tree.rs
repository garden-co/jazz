use std::collections::BTreeMap;

use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::query::{ArraySubquery, OrderDirection, Query};
use jazz::result_tree::MAX_RESULT_TREE_PARENT_BYTES;
use jazz::result_tree::ResultRelation;
use jazz::schema::{JazzSchema, Policy, TableSchema};

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            "parents",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("rank", ColumnType::U32),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "children",
            [
                ColumnSchema::new("parent_id", ColumnType::Uuid),
                ColumnSchema::new("label", ColumnType::String),
                ColumnSchema::new("rank", ColumnType::U32),
            ],
        )
        .with_reference("parent_id", "parents")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "grandchildren",
            [
                ColumnSchema::new("child_id", ColumnType::Uuid),
                ColumnSchema::new("label", ColumnType::String),
                ColumnSchema::new("rank", ColumnType::U32),
            ],
        )
        .with_reference("child_id", "children")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn open_db() -> Db<MemoryStorage> {
    let schema = schema();
    let column_families = schema.column_families();
    let references = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&references),
            DbIdentity {
                node: NodeUuid::from_bytes([0x71; 16]),
                author: AuthorId::from_bytes([0x72; 16]),
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
    let parent = db
        .insert(
            "parents",
            BTreeMap::from([
                ("title".to_owned(), Value::String("parent".to_owned())),
                ("rank".to_owned(), Value::U32(0)),
            ]),
        )
        .expect("insert parent")
        .row_uuid();
    let mut child_ids = Vec::new();
    for (rank, label) in [(0, "first"), (1, "second"), (2, "third")] {
        child_ids.push(
            db.insert(
                "children",
                BTreeMap::from([
                    ("parent_id".to_owned(), Value::Uuid(parent.0)),
                    ("label".to_owned(), Value::String(label.to_owned())),
                    ("rank".to_owned(), Value::U32(rank)),
                ]),
            )
            .expect("insert child")
            .row_uuid(),
        );
    }
    for (rank, label) in [(0, "hidden"), (1, "visible")] {
        db.insert(
            "grandchildren",
            BTreeMap::from([
                ("child_id".to_owned(), Value::Uuid(child_ids[1].0)),
                ("label".to_owned(), Value::String(label.to_owned())),
                ("rank".to_owned(), Value::U32(rank)),
            ]),
        )
        .expect("insert grandchild");
    }

    let query = child_query(
        ArraySubquery::new("children", "children", "parent_id", "id")
            .select(["label", "rank"])
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
            .order_by("rank", OrderDirection::Asc)
            .unbounded(),
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
        child_ids[1..]
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
            .cell_at(1),
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
    assert_eq!(added.first().map(|row| row.row_uuid()), Some(parent));
    assert!(added.iter().any(|row| row.table() == "children"));
}

#[test]
fn array_bounds_must_be_declared_for_prepare_read_and_subscribe() {
    let db = open_db();
    let parent = db
        .insert(
            "parents",
            BTreeMap::from([
                ("title".to_owned(), Value::String("parent".to_owned())),
                ("rank".to_owned(), Value::U32(0)),
            ]),
        )
        .expect("insert parent")
        .row_uuid();
    db.insert(
        "children",
        BTreeMap::from([
            ("parent_id".to_owned(), Value::Uuid(parent.0)),
            ("label".to_owned(), Value::String("child".to_owned())),
            ("rank".to_owned(), Value::U32(0)),
        ]),
    )
    .expect("insert child");

    let undeclared = child_query(ArraySubquery::new(
        "children",
        "children",
        "parent_id",
        "id",
    ));
    for operation in ["prepare", "read", "subscribe"] {
        let error = db.prepare_query(&undeclared).expect_err(operation);
        assert!(
            error
                .to_string()
                .contains("array subquery children must specify limit(...) or unbounded()"),
            "{operation}: {error}"
        );
    }
    let nested_undeclared = child_query(
        ArraySubquery::new("children", "children", "parent_id", "id")
            .limit(1)
            .nested(ArraySubquery::new(
                "grandchildren",
                "grandchildren",
                "child_id",
                "id",
            )),
    );
    assert!(db.prepare_query(&nested_undeclared).is_err());

    let zero = db
        .prepare_query(&child_query(
            ArraySubquery::new("children", "children", "parent_id", "id").limit(0),
        ))
        .expect("prepare limit zero");
    let zero_tree =
        block_on(db.all_result_tree(&zero, ReadOpts::default())).expect("read limit zero");
    assert!(children(&zero_tree.roots[0], "children").is_empty());

    let unbounded = db
        .prepare_query(&child_query(
            ArraySubquery::new("children", "children", "parent_id", "id").unbounded(),
        ))
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
fn parent_too_large_is_atomic() {
    let db = open_db();
    let parent = db
        .insert(
            "parents",
            BTreeMap::from([
                ("title".to_owned(), Value::String("parent".to_owned())),
                ("rank".to_owned(), Value::U32(0)),
            ]),
        )
        .expect("insert parent")
        .row_uuid();
    let payload = "x".repeat(MAX_RESULT_TREE_PARENT_BYTES / 2 + 1024);
    for rank in 0..2 {
        db.insert(
            "children",
            BTreeMap::from([
                ("parent_id".to_owned(), Value::Uuid(parent.0)),
                ("label".to_owned(), Value::String(payload.clone())),
                ("rank".to_owned(), Value::U32(rank)),
            ]),
        )
        .expect("insert individually valid child");
    }
    let prepared = db
        .prepare_query(&child_query(
            ArraySubquery::new("children", "children", "parent_id", "id")
                .select(["label", "rank"])
                .limit(2),
        ))
        .expect("prepare finite children");

    let read_error = block_on(db.all_result_tree(&prepared, ReadOpts::default()))
        .expect_err("whole parent exceeds terminal byte limit");
    let message = read_error.to_string();
    assert!(message.contains("parent-too-large"), "{message}");
    assert!(message.contains(&parent.0.to_string()), "{message}");
    assert!(message.contains("relation=children"), "{message}");
    assert!(
        message.contains(&format!("limit={MAX_RESULT_TREE_PARENT_BYTES}")),
        "{message}"
    );

    assert!(
        block_on(db.subscribe(&prepared, ReadOpts::default())).is_err(),
        "the oversized reset must be rejected before any replacement is admitted"
    );
}
