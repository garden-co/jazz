//! Integration tests for canonical multisink graph subscriptions.
//!
//! This is a lower-level Groove test because multisink delivery is the
//! executor primitive that Jazz one-sink subscriptions wrap.

use std::sync::mpsc::TryRecvError;

use groove::db::{Database, GraphBuilder, RoutedMultisinkTerminal};
use groove::ivm::{CollectByField, ProjectField, TopByLimit};
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
