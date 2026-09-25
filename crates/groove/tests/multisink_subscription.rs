//! Integration tests for canonical multisink graph subscriptions.
//!
//! This is a lower-level Groove test because multisink delivery is the
//! executor primitive that Jazz one-sink subscriptions wrap.

use std::sync::mpsc::TryRecvError;

use groove::db::{Database, GraphBuilder, PrimaryKeyValue, RoutedMultisinkTerminal};
use groove::ivm::runtime::TerminalEdit;
use groove::ivm::{CollectByField, ProjectField, TopByLimit, TopByOrder};
use groove::records::{RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

fn albums_schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("year", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

async fn database() -> Database {
    Database::new(
        albums_schema(),
        MemoryStorage::new(&["albums"]).expect("valid memory storage families"),
    )
    .await
    .unwrap()
}

async fn insert_album(db: &mut Database, id: u64, title: &str, year: u64) {
    let mut batch = db.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(id),
            Value::String(title.to_owned()),
            Value::U64(year),
        ],
    );
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
}

fn project_schema() -> DatabaseSchema {
    DatabaseSchema::new([
        TableSchema::new(
            "docs",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("org_id", ColumnType::U64),
                ColumnSchema::new("project_id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
        TableSchema::new(
            "comments",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("org_id", ColumnType::U64),
                ColumnSchema::new("project_id", ColumnType::U64),
                ColumnSchema::new("doc_id", ColumnType::U64),
                ColumnSchema::new("body", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64)),
    ])
}

fn route_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("org_id", ColumnType::U64.clone()),
        ("project_id", ColumnType::U64.clone()),
    ])
}

async fn project_database() -> Database {
    Database::new(
        project_schema(),
        MemoryStorage::new(&["docs", "comments"]).expect("valid memory storage families"),
    )
    .await
    .unwrap()
}

fn project_bindings() -> GraphBuilder {
    GraphBuilder::binding_source("project_route", route_descriptor())
}

fn docs_terminal_graph() -> GraphBuilder {
    GraphBuilder::join(
        project_bindings(),
        GraphBuilder::table("docs"),
        ["org_id", "project_id"],
        ["org_id", "project_id"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.title", "title"),
        ProjectField::renamed("left.org_id", "__route_org_id"),
        ProjectField::renamed("left.project_id", "__route_project_id"),
    ])
}

fn comments_terminal_graph() -> GraphBuilder {
    GraphBuilder::join(
        project_bindings(),
        GraphBuilder::table("comments"),
        ["org_id", "project_id"],
        ["org_id", "project_id"],
    )
    .project_fields([
        ProjectField::renamed("right.id", "id"),
        ProjectField::renamed("right.doc_id", "doc_id"),
        ProjectField::renamed("right.body", "body"),
        ProjectField::renamed("left.org_id", "__route_org_id"),
        ProjectField::renamed("left.project_id", "__route_project_id"),
    ])
}

fn routed_terminals() -> [RoutedMultisinkTerminal; 2] {
    [
        RoutedMultisinkTerminal::new(
            "docs",
            docs_terminal_graph(),
            ["__route_org_id", "__route_project_id"],
            ["id", "title"],
        ),
        RoutedMultisinkTerminal::new(
            "comments",
            comments_terminal_graph(),
            ["__route_org_id", "__route_project_id"],
            ["id", "doc_id", "body"],
        ),
    ]
}

fn routed_doc_output_terminals() -> [RoutedMultisinkTerminal; 2] {
    [
        RoutedMultisinkTerminal::new(
            "ids",
            docs_terminal_graph(),
            ["__route_org_id", "__route_project_id"],
            ["id"],
        ),
        RoutedMultisinkTerminal::new(
            "rows",
            docs_terminal_graph(),
            ["__route_org_id", "__route_project_id"],
            ["id", "title"],
        ),
    ]
}

fn project_only_route_terminal() -> RoutedMultisinkTerminal {
    RoutedMultisinkTerminal::new(
        "docs",
        docs_terminal_graph(),
        ["__route_project_id"],
        ["id", "title"],
    )
    .with_route_value_indices([1])
}

fn insert_doc(
    batch: &mut groove::db::DatabaseBatch,
    id: u64,
    org_id: u64,
    project_id: u64,
    title: &str,
) {
    batch.insert(
        "docs",
        vec![
            Value::U64(id),
            Value::U64(org_id),
            Value::U64(project_id),
            Value::String(title.to_owned()),
        ],
    );
}

fn insert_comment(
    batch: &mut groove::db::DatabaseBatch,
    id: u64,
    org_id: u64,
    project_id: u64,
    doc_id: u64,
    body: &str,
) {
    batch.insert(
        "comments",
        vec![
            Value::U64(id),
            Value::U64(org_id),
            Value::U64(project_id),
            Value::U64(doc_id),
            Value::String(body.to_owned()),
        ],
    );
}

#[futures_test::test]
async fn routed_terminal_can_select_a_nonprefix_binding_value() {
    let mut db = project_database().await;
    let mut batch = db.open_batch();
    insert_doc(&mut batch, 1, 10, 20, "Spec");
    insert_doc(&mut batch, 2, 11, 21, "Roadmap");
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    let shape = db
        .prepare(
            [project_only_route_terminal()],
            "project_route",
            route_descriptor(),
        )
        .await
        .unwrap();
    let project_21 = db
        .bind_shape(shape.id(), &[Value::U64(11), Value::U64(21)])
        .await
        .unwrap();

    assert_eq!(
        project_21
            .recv()
            .unwrap()
            .get("docs")
            .unwrap()
            .to_values()
            .unwrap(),
        [(vec![Value::U64(2), Value::String("Roadmap".to_owned())], 1)]
    );
}

#[futures_test::test]
async fn prepared_routed_multisink_combines_binding_sets_with_user_output_routings() {
    let mut db = project_database().await;
    let mut batch = db.open_batch();
    insert_doc(&mut batch, 1, 10, 20, "Spec");
    insert_doc(&mut batch, 2, 10, 21, "Roadmap");
    insert_doc(&mut batch, 3, 11, 20, "Other org");
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    let shape = db
        .prepare(
            routed_doc_output_terminals(),
            "project_route",
            route_descriptor(),
        )
        .await
        .unwrap();

    let project_20 = db
        .bind_shape(shape.id(), &[Value::U64(10), Value::U64(20)])
        .await
        .unwrap();
    let initial_20 = project_20.recv().unwrap();
    assert_eq!(initial_20.sinks.len(), 2);
    assert_eq!(
        initial_20.get("ids").unwrap().to_values().unwrap(),
        [(vec![Value::U64(1)], 1,)]
    );
    assert_eq!(
        initial_20.get("rows").unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::String("Spec".to_owned())], 1,)]
    );

    let project_21 = db
        .bind_shape(shape.id(), &[Value::U64(10), Value::U64(21)])
        .await
        .unwrap();
    assert!(
        matches!(project_20.try_recv(), Err(TryRecvError::Empty)),
        "binding a second tuple should not notify existing bindings"
    );
    let initial_21 = project_21.recv().unwrap();
    assert_eq!(
        initial_21.get("ids").unwrap().to_values().unwrap(),
        [(vec![Value::U64(2)], 1,)]
    );
    assert_eq!(
        initial_21.get("rows").unwrap().to_values().unwrap(),
        [(vec![Value::U64(2), Value::String("Roadmap".to_owned())], 1,)]
    );

    let project_20_again = db
        .bind_shape(shape.id(), &[Value::U64(10), Value::U64(20)])
        .await
        .unwrap();
    assert!(
        matches!(project_20.try_recv(), Err(TryRecvError::Empty)),
        "refcounting the same binding should not notify existing subscribers"
    );
    let duplicate_initial_20 = project_20_again.recv().unwrap();
    assert_eq!(
        duplicate_initial_20
            .get("ids")
            .unwrap()
            .to_values()
            .unwrap(),
        [(vec![Value::U64(1)], 1,)]
    );
    assert_eq!(
        duplicate_initial_20
            .get("rows")
            .unwrap()
            .to_values()
            .unwrap(),
        [(vec![Value::U64(1), Value::String("Spec".to_owned())], 1,)]
    );

    let mut batch = db.open_batch();
    insert_doc(&mut batch, 4, 10, 20, "Design");
    insert_doc(&mut batch, 5, 10, 21, "Launch");
    insert_doc(&mut batch, 6, 10, 22, "Wrong project");
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    let tick_20 = project_20.recv().unwrap();
    assert_eq!(
        tick_20.get("ids").unwrap().to_values().unwrap(),
        [(vec![Value::U64(4)], 1,)]
    );
    assert_eq!(
        tick_20.get("rows").unwrap().to_values().unwrap(),
        [(vec![Value::U64(4), Value::String("Design".to_owned())], 1,)]
    );

    let duplicate_tick_20 = project_20_again.recv().unwrap();
    assert_eq!(
        duplicate_tick_20.get("ids").unwrap().to_values().unwrap(),
        [(vec![Value::U64(4)], 1,)]
    );
    assert_eq!(
        duplicate_tick_20.get("rows").unwrap().to_values().unwrap(),
        [(vec![Value::U64(4), Value::String("Design".to_owned())], 1,)]
    );

    let tick_21 = project_21.recv().unwrap();
    assert_eq!(
        tick_21.get("ids").unwrap().to_values().unwrap(),
        [(vec![Value::U64(5)], 1,)]
    );
    assert_eq!(
        tick_21.get("rows").unwrap().to_values().unwrap(),
        [(vec![Value::U64(5), Value::String("Launch".to_owned())], 1,)]
    );
}

#[futures_test::test]
async fn multisink_subscription_delivers_initial_and_tick_deltas_for_all_sinks() {
    let mut db = database().await;
    insert_album(&mut db, 1, "Kind of Blue", 1959).await;

    let subscription = db
        .subscribe([
            ("rows", GraphBuilder::table("albums")),
            (
                "years",
                GraphBuilder::table("albums").project(["id", "year"]),
            ),
        ])
        .unwrap();

    let initial = subscription.recv().unwrap();
    assert_eq!(
        initial.get("rows").unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(1),
                Value::String("Kind of Blue".to_owned()),
                Value::U64(1959),
            ],
            1,
        )]
    );
    assert_eq!(
        initial.get("years").unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::U64(1959)], 1)]
    );

    insert_album(&mut db, 2, "Blue Train", 1957).await;

    let tick = subscription.recv().unwrap();
    assert_eq!(tick.sinks.len(), 2);
    assert_eq!(
        tick.get("rows").unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(2),
                Value::String("Blue Train".to_owned()),
                Value::U64(1957),
            ],
            1,
        )]
    );
    assert_eq!(
        tick.get("years").unwrap().to_values().unwrap(),
        [(vec![Value::U64(2), Value::U64(1957)], 1)]
    );
}

#[futures_test::test]
async fn shared_structured_output_delivers_terminal_deltas_to_every_subscription() {
    let mut db = database().await;
    let graph = GraphBuilder::collect_root_ordered(
        GraphBuilder::table("albums"),
        ["id"],
        [
            CollectByField::named("id"),
            CollectByField::named("title"),
            CollectByField::named("year"),
        ],
        Vec::new(),
        Vec::<String>::new(),
        0,
        TopByLimit::Unbounded,
    );
    let first = db.subscribe([("rows", graph.clone())]).unwrap();
    let second = db.subscribe([("rows", graph)]).unwrap();
    first.try_recv().expect("first initial snapshot");
    second.try_recv().expect("second initial snapshot");

    insert_album(&mut db, 1, "Kind of Blue", 1959).await;

    let first_tick = first.try_recv().expect("first subscription tick");
    let second_tick = second.try_recv().expect("second subscription tick");
    let first_terminal = first_tick
        .terminal_sinks
        .get("rows")
        .expect("first subscription receives structured terminal deltas");
    let second_terminal = second_tick
        .terminal_sinks
        .get("rows")
        .expect("second subscription receives structured terminal deltas");
    assert!(!first_terminal.is_empty());
    assert_eq!(second_terminal, first_terminal);
}

#[futures_test::test]
async fn root_position_maps_follow_plain_consumer_demand_for_shared_ordering() {
    let mut db = database().await;
    let ordered = GraphBuilder::top_by(
        GraphBuilder::table("albums"),
        Vec::<String>::new(),
        [TopByOrder::asc("year")],
        ["id"],
        0,
        TopByLimit::Unbounded,
    );
    let structured = db
        .subscribe([(
            "rows",
            GraphBuilder::collect_root_ordered(
                ordered.clone(),
                ["id"],
                [
                    CollectByField::named("id"),
                    CollectByField::named("title"),
                    CollectByField::named("year"),
                ],
                [TopByOrder::asc("year")],
                ["id"],
                0,
                TopByLimit::Unbounded,
            ),
        )])
        .unwrap();
    structured.try_recv().unwrap();
    insert_album(&mut db, 1, "Blue Train", 1957).await;
    assert!(!structured.try_recv().unwrap().terminal_sinks["rows"].is_empty());
    let metrics = db.last_tick_metrics().unwrap();
    assert_eq!(metrics.root_ordering_position_records, 0);
    assert_eq!(metrics.root_ordering_position_records_skipped, 0);
    assert_eq!(metrics.top_by_delta_membership_records, 1);

    let plain = db.subscribe([("rows", ordered.clone())]).unwrap();
    assert_eq!(
        plain.try_recv().unwrap().get("rows").unwrap().deltas.len(),
        1
    );
    for (id, year, expected_index) in [(3, 1965, 1), (2, 1959, 1)] {
        insert_album(&mut db, id, "Album", year).await;
        let tick = plain.try_recv().unwrap();
        assert!(tick.terminal_sinks["rows"].operations.iter().any(|operation| {
            matches!(operation.edit, TerminalEdit::Insert { index, .. } if index == expected_index)
        }));
        assert!(!structured.try_recv().unwrap().terminal_sinks["rows"].is_empty());
        let metrics = db.last_tick_metrics().unwrap();
        // One shared TopBy: both consumers must not duplicate position work,
        // and only the inserted row is ranked, not the whole window.
        assert_eq!(metrics.root_ordering_position_records, 1);
        assert_eq!(metrics.root_ordering_position_records_skipped, 0);
        assert_eq!(metrics.top_by_delta_membership_records, 1);
    }

    let mut batch = db.open_batch();
    batch.update(
        "albums",
        vec![
            Value::U64(3),
            Value::String("Moved".into()),
            Value::U64(1950),
        ],
    );
    let persisted = db.apply_batch(batch).await.unwrap().persist().await;
    db.finish_persistence(persisted).unwrap();
    let tick = plain.try_recv().unwrap();
    assert!(
        tick.terminal_sinks["rows"]
            .operations
            .iter()
            .any(|operation| { matches!(operation.edit, TerminalEdit::Move { index: 0, .. }) })
    );
    structured.try_recv().unwrap();

    assert!(db.unsubscribe(plain.id()));
    insert_album(&mut db, 4, "Later", 1970).await;
    structured.try_recv().unwrap();
    let metrics = db.last_tick_metrics().unwrap();
    assert_eq!(metrics.root_ordering_position_records, 0);
    // With only the structured consumer left, even window enumeration is
    // unnecessary: the fourth insert visits one input delta, not seven rows.
    assert_eq!(metrics.root_ordering_position_records_skipped, 0);
    assert_eq!(metrics.top_by_delta_membership_records, 1);

    let rebound = db.subscribe([("rows", ordered)]).unwrap();
    let initial = rebound.try_recv().unwrap();
    let ids = initial
        .get("rows")
        .unwrap()
        .to_values()
        .unwrap()
        .into_iter()
        .map(|(values, _)| values[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        [Value::U64(3), Value::U64(1), Value::U64(2), Value::U64(4)]
    );
    insert_album(&mut db, 5, "Between", 1952).await;
    assert!(
        rebound.try_recv().unwrap().terminal_sinks["rows"]
            .operations
            .iter()
            .any(|operation| { matches!(operation.edit, TerminalEdit::Insert { index: 1, .. }) })
    );
    assert_eq!(
        db.last_tick_metrics()
            .unwrap()
            .root_ordering_position_records,
        1
    );
}

#[futures_test::test]
async fn root_payload_updates_preserve_order_and_mixed_positional_edits() {
    use groove::ivm::runtime::TerminalOperation;
    use groove::records::BorrowedRecord;

    fn apply(rows: &mut Vec<(Vec<u8>, Vec<Value>)>, operations: &[TerminalOperation]) {
        for operation in operations {
            let decode = |bytes: &[u8]| {
                let record = BorrowedRecord::new(bytes, &operation.root_descriptor);
                (0..3)
                    .map(|index| record.get_idx(index).unwrap())
                    .collect::<Vec<_>>()
            };
            match &operation.edit {
                TerminalEdit::Insert { key, index, value } => {
                    rows.insert(*index, (key.clone(), decode(value)))
                }
                TerminalEdit::Remove { key } => {
                    rows.remove(rows.iter().position(|(k, _)| k == key).unwrap());
                }
                TerminalEdit::Move { key, index } => {
                    let row = rows.remove(rows.iter().position(|(k, _)| k == key).unwrap());
                    rows.insert(*index, row);
                }
                TerminalEdit::Update { key, value } => {
                    rows.iter_mut().find(|(k, _)| k == key).unwrap().1 = decode(value)
                }
            }
        }
    }

    let mut db = database().await;
    let subscription = db
        .subscribe([(
            "rows",
            GraphBuilder::collect_root_ordered(
                GraphBuilder::table("albums"),
                ["id"],
                [
                    CollectByField::named("id"),
                    CollectByField::named("title"),
                    CollectByField::named("year"),
                ],
                [TopByOrder::asc("year")],
                ["id"],
                0,
                TopByLimit::Unbounded,
            ),
        )])
        .unwrap();
    subscription.try_recv().unwrap();
    let mut rows = Vec::new();
    for id in 1..=64 {
        insert_album(&mut db, id, "Original", 2000 + id).await;
        apply(
            &mut rows,
            &subscription.try_recv().unwrap().terminal_sinks["rows"].operations,
        );
    }
    let initial_ids = rows
        .iter()
        .map(|(_, values)| values[0].clone())
        .collect::<Vec<_>>();
    for title in ["Changed", "Changed again"] {
        let mut batch = db.open_batch();
        batch.update(
            "albums",
            vec![
                Value::U64(32),
                Value::String(title.into()),
                Value::U64(2032),
            ],
        );
        let persisted = db.apply_batch(batch).await.unwrap().persist().await;
        db.finish_persistence(persisted).unwrap();
        let tick = subscription.try_recv().unwrap();
        let operations = &tick.terminal_sinks["rows"].operations;
        assert!(matches!(
            operations.as_slice(),
            [TerminalOperation {
                edit: TerminalEdit::Update { .. },
                ..
            }]
        ));
        apply(&mut rows, operations);
        assert_eq!(rows[31].1[1], Value::String(title.into()));
        assert_eq!(
            rows.iter()
                .map(|(_, values)| values[0].clone())
                .collect::<Vec<_>>(),
            initial_ids
        );
    }
    // Two moves cross each other while a new row joins between them. Terminal
    // positions are sequential edits, not independently applied final ranks.
    let mut batch = db.open_batch();
    batch.update(
        "albums",
        vec![
            Value::U64(64),
            Value::String("First".into()),
            Value::U64(1999),
        ],
    );
    batch.update(
        "albums",
        vec![
            Value::U64(1),
            Value::String("Last".into()),
            Value::U64(2100),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(65),
            Value::String("Middle".into()),
            Value::U64(2032),
        ],
    );
    let persisted = db.apply_batch(batch).await.unwrap().persist().await;
    db.finish_persistence(persisted).unwrap();
    apply(
        &mut rows,
        &subscription.try_recv().unwrap().terminal_sinks["rows"].operations,
    );
    let mut expected = (2..=63).collect::<Vec<u64>>();
    expected.insert(0, 64);
    expected.insert(expected.iter().position(|id| *id == 32).unwrap() + 1, 65);
    expected.push(1);
    assert_eq!(
        rows.iter()
            .map(|(_, values)| values[0].clone())
            .collect::<Vec<_>>(),
        expected.into_iter().map(Value::U64).collect::<Vec<_>>()
    );
    assert_eq!(rows.first().unwrap().1[1], Value::String("First".into()));
    assert_eq!(rows.last().unwrap().1[1], Value::String("Last".into()));
}

#[futures_test::test]
async fn structured_unbounded_membership_visits_only_changed_rows_at_scale() {
    for count in [10, 1000] {
        let mut db = database().await;
        let mut batch = db.open_batch();
        for id in 1..=count {
            batch.insert(
                "albums",
                vec![
                    Value::U64(id),
                    Value::String("Album".into()),
                    Value::U64(id),
                ],
            );
        }
        let persisted = db.apply_batch(batch).await.unwrap().persist().await;
        db.finish_persistence(persisted).unwrap();
        let ordered = GraphBuilder::top_by(
            GraphBuilder::table("albums"),
            Vec::<String>::new(),
            [TopByOrder::asc("year")],
            ["id"],
            0,
            TopByLimit::Unbounded,
        );
        let graph = GraphBuilder::collect_root_ordered(
            ordered,
            ["id"],
            [
                CollectByField::named("id"),
                CollectByField::named("title"),
                CollectByField::named("year"),
            ],
            [TopByOrder::asc("year")],
            ["id"],
            0,
            TopByLimit::Unbounded,
        );
        let subscription = db.subscribe([("rows", graph.clone())]).unwrap();
        let initial = subscription.try_recv().unwrap();
        let mut keys = Vec::new();
        let apply = |keys: &mut Vec<Vec<u8>>, edits: &groove::ivm::runtime::TerminalDeltas| {
            for operation in &edits.operations {
                assert!(operation.path.is_empty());
                match &operation.edit {
                    TerminalEdit::Insert { index, key, .. } => keys.insert(*index, key.clone()),
                    TerminalEdit::Remove { key } => {
                        let i = keys.iter().position(|k| k == key).unwrap();
                        keys.remove(i);
                    }
                    TerminalEdit::Move { index, key } => {
                        let i = keys.iter().position(|k| k == key).unwrap();
                        keys.remove(i);
                        keys.insert(*index, key.clone());
                    }
                    TerminalEdit::Update { key, .. } => assert!(keys.contains(key)),
                }
            }
        };
        apply(&mut keys, &initial.terminal_sinks["rows"]);
        assert_eq!(keys.len(), count as usize);
        let mut batch = db.open_batch();
        batch.update(
            "albums",
            vec![
                Value::U64(count),
                Value::String("Moved".into()),
                Value::U64(0),
            ],
        );
        batch.delete("albums", PrimaryKeyValue::U64(2));
        let persisted = db.apply_batch(batch).await.unwrap().persist().await;
        db.finish_persistence(persisted).unwrap();
        let metrics = db.last_tick_metrics().unwrap();
        assert_eq!(
            metrics.top_by_delta_membership_records, 3,
            "retained rows={count}"
        );
        assert_eq!(metrics.root_ordering_position_records, 0);
        assert_eq!(metrics.root_ordering_position_records_skipped, 0);
        apply(
            &mut keys,
            &subscription.try_recv().unwrap().terminal_sinks["rows"],
        );
        let fresh = db.subscribe([("rows", graph)]).unwrap().try_recv().unwrap();
        let mut expected = Vec::new();
        apply(&mut expected, &fresh.terminal_sinks["rows"]);
        assert_eq!(
            keys, expected,
            "incremental order matches fresh hydration at {count} rows"
        );
        let rows = fresh.get("rows").unwrap().to_values().unwrap();
        assert_eq!(rows.len(), count as usize - 1);
        assert_eq!(
            rows[0].0,
            [
                Value::U64(count),
                Value::String("Moved".into()),
                Value::U64(0)
            ]
        );
    }
}

#[futures_test::test]
async fn unsubscribing_multisink_subscription_closes_the_whole_stream() {
    let mut db = database().await;
    let subscription = db
        .subscribe([
            ("rows", GraphBuilder::table("albums")),
            (
                "years",
                GraphBuilder::table("albums").project(["id", "year"]),
            ),
        ])
        .unwrap();
    let initial = subscription.recv().unwrap();
    assert!(initial.get("rows").unwrap().is_empty());
    assert!(initial.get("years").unwrap().is_empty());

    assert!(db.unsubscribe(subscription.id()));
    insert_album(&mut db, 1, "Kind of Blue", 1959).await;

    assert!(matches!(
        subscription.try_recv(),
        Err(TryRecvError::Disconnected)
    ));
}

#[futures_test::test]
async fn routed_multisink_binding_sets_filter_in_graph_and_project_public_sinks() {
    let mut db = project_database().await;
    let mut batch = db.open_batch();
    insert_doc(&mut batch, 1, 10, 20, "Spec");
    insert_doc(&mut batch, 2, 10, 21, "Roadmap");
    insert_doc(&mut batch, 3, 11, 20, "Other org");
    insert_comment(&mut batch, 11, 10, 20, 1, "looks good");
    insert_comment(&mut batch, 12, 10, 21, 2, "ship it");
    insert_comment(&mut batch, 13, 10, 22, 1, "wrong project");
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    let shape = db
        .prepare(routed_terminals(), "project_route", route_descriptor())
        .await
        .unwrap();

    let project_20 = db
        .bind_shape(shape.id(), &[Value::U64(10), Value::U64(20)])
        .await
        .unwrap();
    let initial_20 = project_20.recv().unwrap();
    assert_eq!(initial_20.sinks.len(), 2);
    assert_eq!(
        initial_20.get("docs").unwrap().to_values().unwrap(),
        [(vec![Value::U64(1), Value::String("Spec".to_owned())], 1,)]
    );
    assert_eq!(
        initial_20.get("comments").unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(11),
                Value::U64(1),
                Value::String("looks good".to_owned()),
            ],
            1,
        )]
    );

    let project_21 = db
        .bind_shape(shape.id(), &[Value::U64(10), Value::U64(21)])
        .await
        .unwrap();
    assert!(project_20.try_recv().is_err());
    let initial_21 = project_21.recv().unwrap();
    assert_eq!(
        initial_21.get("docs").unwrap().to_values().unwrap(),
        [(vec![Value::U64(2), Value::String("Roadmap".to_owned())], 1,)]
    );
    assert_eq!(
        initial_21.get("comments").unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(12),
                Value::U64(2),
                Value::String("ship it".to_owned()),
            ],
            1,
        )]
    );

    let mut batch = db.open_batch();
    insert_doc(&mut batch, 4, 10, 20, "Design");
    insert_comment(&mut batch, 14, 10, 20, 4, "needs review");
    insert_doc(&mut batch, 5, 10, 21, "Launch");
    insert_comment(&mut batch, 15, 10, 21, 5, "approved");
    insert_doc(&mut batch, 6, 11, 20, "Ignored");
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    let tick_20 = project_20.recv().unwrap();
    assert_eq!(
        tick_20.get("docs").unwrap().to_values().unwrap(),
        [(vec![Value::U64(4), Value::String("Design".to_owned())], 1,)]
    );
    assert_eq!(
        tick_20.get("comments").unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(14),
                Value::U64(4),
                Value::String("needs review".to_owned()),
            ],
            1,
        )]
    );

    let tick_21 = project_21.recv().unwrap();
    assert_eq!(
        tick_21.get("docs").unwrap().to_values().unwrap(),
        [(vec![Value::U64(5), Value::String("Launch".to_owned())], 1,)]
    );
    assert_eq!(
        tick_21.get("comments").unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(15),
                Value::U64(5),
                Value::String("approved".to_owned()),
            ],
            1,
        )]
    );
}

#[futures_test::test]
async fn dropped_routed_multisink_receiver_retracts_binding_before_rebind() {
    let mut db = project_database().await;
    let shape = db
        .prepare(routed_terminals(), "project_route", route_descriptor())
        .await
        .unwrap();

    let dropped = db
        .bind_shape(shape.id(), &[Value::U64(10), Value::U64(20)])
        .await
        .unwrap();
    let initial = dropped.recv().unwrap();
    assert!(initial.get("docs").unwrap().is_empty());
    drop(dropped);

    let mut batch = db.open_batch();
    insert_doc(&mut batch, 1, 10, 20, "Spec");
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();

    let rebound = db
        .bind_shape(shape.id(), &[Value::U64(10), Value::U64(20)])
        .await
        .unwrap();
    assert_eq!(
        rebound
            .recv()
            .unwrap()
            .get("docs")
            .unwrap()
            .to_values()
            .unwrap(),
        [(vec![Value::U64(1), Value::String("Spec".to_owned())], 1,)]
    );

    let mut batch = db.open_batch();
    insert_doc(&mut batch, 2, 10, 20, "Design");
    let applied = db.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    db.finish_persistence(persisted).unwrap();
    assert_eq!(
        rebound
            .recv()
            .unwrap()
            .get("docs")
            .unwrap()
            .to_values()
            .unwrap(),
        [(vec![Value::U64(2), Value::String("Design".to_owned())], 1,)],
        "rebound binding should receive later rows exactly once"
    );
}
