//! INV-INC-1 for prepared shapes: a write that changes one binding's result
//! must cost the same whether the shape has 10 or 100 other live bindings
//! (#3288). The counters are deterministic work proxies for tick time.

use groove::db::{Database, GraphBuilder};
use groove::ivm::runtime::{MultisinkSubscription, RoutedMultisinkTerminal, TerminalEdit};
use groove::ivm::{CollectByField, ProjectField, TopByLimit, TopByOrder};
use groove::records::{BorrowedRecord, RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, PrimaryKeyColumn,
    TableSchema,
};
use groove::storage::MemoryStorage;

const TASKS_PER_OWNER: u64 = 5;

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "tasks",
        [
            ColumnSchema::new("owner", ColumnType::U64),
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("rev", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("owner", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("id", IntegerKeyType::U64),
    ]))])
}

fn task(owner: u64, id: u64, rev: u64) -> Vec<Value> {
    vec![Value::U64(owner), Value::U64(id), Value::U64(rev)]
}

async fn commit(database: &mut Database, batch: groove::db::DatabaseBatch) {
    let applied = database.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();
}

/// (nodes evaluated, subscriptions considered, notified subscribers) for one
/// write to owner 0's first task with `bindings` live owners bound.
async fn one_write_cost(bindings: u64) -> (usize, usize, usize) {
    let mut database = Database::new(schema(), MemoryStorage::new(&["tasks"]).unwrap())
        .await
        .unwrap();
    let mut seed = database.open_batch();
    for owner in 0..bindings {
        for id in 0..TASKS_PER_OWNER {
            seed.insert("tasks", task(owner, id, 0));
        }
    }
    commit(&mut database, seed).await;

    let user =
        GraphBuilder::binding_source("user", RecordDescriptor::new([("user", ColumnType::U64)]));
    let graph = GraphBuilder::join(user, GraphBuilder::table("tasks"), ["user"], ["owner"])
        .project_fields([
            ProjectField::renamed("left.user", "user"),
            ProjectField::renamed("right.id", "id"),
            ProjectField::renamed("right.rev", "rev"),
        ]);
    let shape = database
        .prepare_one_sink(
            graph,
            "user",
            RecordDescriptor::new([("user", ColumnType::U64)]),
            ["user"],
        )
        .await
        .unwrap();
    let mut subscriptions = Vec::new();
    for owner in 0..bindings {
        subscriptions.push(
            database
                .bind_shape_one_sink(shape.id(), &[Value::U64(owner)])
                .await
                .unwrap(),
        );
    }
    database.drive_progress().await.unwrap();
    for subscription in &subscriptions {
        while subscription.try_recv().is_ok() {}
    }

    let mut write = database.open_batch();
    write.update("tasks", task(0, 0, 1));
    commit(&mut database, write).await;
    database.drive_progress().await.unwrap();
    let metrics = database.last_tick_metrics().unwrap().clone();
    let mut notified = 0;
    for (owner, subscription) in subscriptions.iter().enumerate() {
        let mut received = false;
        while let Ok(deltas) = subscription.try_recv() {
            received |= !deltas.to_values().unwrap().is_empty();
        }
        if received {
            assert_eq!(owner, 0, "only the written owner's binding may change");
            notified += 1;
        }
    }
    assert_eq!(
        notified, 1,
        "the written owner's binding must see its update"
    );
    (
        metrics.nodes_evaluated,
        metrics.subscriptions_considered,
        notified,
    )
}

#[futures_test::test]
async fn one_binding_write_cost_is_independent_of_live_binding_count() {
    let small = one_write_cost(10).await;
    let large = one_write_cost(100).await;
    assert_eq!(
        small, large,
        "(nodes evaluated, subscriptions considered, notified) must not grow with bindings"
    );
}

type Rows = std::collections::BTreeMap<(u64, u64), u64>;

fn apply_deltas(rows: &mut Rows, subscription: &groove::ivm::runtime::Subscription) {
    while let Ok(deltas) = subscription.try_recv() {
        for (values, weight) in deltas.to_values().unwrap() {
            let [Value::U64(owner), Value::U64(id), Value::U64(rev)] = values[..] else {
                panic!("unexpected row {values:?}");
            };
            match weight {
                1 => assert!(rows.insert((owner, id), rev).is_none_or(|old| old != rev)),
                -1 => assert_eq!(rows.remove(&(owner, id)), Some(rev)),
                other => panic!("unexpected weight {other}"),
            }
        }
    }
}

/// Routed activation must deliver exactly what full activation would: rows
/// that move between owners, owners bound twice, and bindings that come and
/// go mid-stream all converge to each owner's exact task set.
#[futures_test::test]
async fn routed_bindings_match_an_exact_oracle_across_moves_and_rebinding() {
    const OWNERS: u64 = 6;
    let mut database = Database::new(schema(), MemoryStorage::new(&["tasks"]).unwrap())
        .await
        .unwrap();
    let user =
        GraphBuilder::binding_source("user", RecordDescriptor::new([("user", ColumnType::U64)]));
    let graph = GraphBuilder::join(user, GraphBuilder::table("tasks"), ["user"], ["owner"])
        .project_fields([
            ProjectField::renamed("left.user", "user"),
            ProjectField::renamed("right.id", "id"),
            ProjectField::renamed("right.rev", "rev"),
        ]);
    let shape = database
        .prepare_one_sink(
            graph,
            "user",
            RecordDescriptor::new([("user", ColumnType::U64)]),
            ["user"],
        )
        .await
        .unwrap();
    // (owner, subscription, rows seen so far)
    let mut live = Vec::new();
    for owner in [0, 1, 1, 2, 3] {
        let subscription = database
            .bind_shape_one_sink(shape.id(), &[Value::U64(owner)])
            .await
            .unwrap();
        live.push((owner, subscription, Rows::new()));
    }
    database.drive_progress().await.unwrap();
    // task id -> (owner, rev); ids are the primary key within an owner, so a
    // move is a delete under the old owner plus an insert under the new one.
    let mut oracle = std::collections::BTreeMap::<(u64, u64), u64>::new();
    let mut random = 0x3288_u64;
    for step in 0..120_u64 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let mut batch = database.open_batch();
        for edit in 0..3 {
            let pick = random.rotate_left(edit * 11);
            let owner = pick % OWNERS;
            let id = (pick >> 8) % 4;
            if pick % 7 == 0 {
                if oracle.remove(&(owner, id)).is_some() {
                    batch.delete(
                        "tasks",
                        groove::db::PrimaryKeyValue::Composite(vec![
                            groove::db::PrimaryKeyValue::U64(owner),
                            groove::db::PrimaryKeyValue::U64(id),
                        ]),
                    );
                }
            } else if let std::collections::btree_map::Entry::Occupied(mut row) =
                oracle.entry((owner, id))
            {
                *row.get_mut() = step;
                batch.update("tasks", task(owner, id, step));
            } else {
                oracle.insert((owner, id), step);
                batch.insert("tasks", task(owner, id, step));
            }
        }
        commit(&mut database, batch).await;
        if step % 17 == 5 {
            let (_, subscription, _) = live.remove((random % live.len() as u64) as usize);
            database.unsubscribe(subscription.id());
        }
        if step % 13 == 3 {
            let owner = random % OWNERS;
            let subscription = database
                .bind_shape_one_sink(shape.id(), &[Value::U64(owner)])
                .await
                .unwrap();
            live.push((owner, subscription, Rows::new()));
        }
        database.drive_progress().await.unwrap();
        for (owner, subscription, rows) in &mut live {
            apply_deltas(rows, subscription);
            let expected = oracle
                .iter()
                .filter(|((task_owner, _), _)| task_owner == owner)
                .map(|((_, id), rev)| ((*owner, *id), *rev))
                .collect::<Rows>();
            assert_eq!(*rows, expected, "owner {owner} at step {step}");
        }
    }
}

// Root collectors (#3308). Jazz lowers app-row queries to a root collector,
// and a bound collector owns private state over `Filter(route == value)` of
// the shape's shared input. The same law must hold there.

const COLLECTOR_SINK: &str = "rows";

fn collector_shape_graph() -> GraphBuilder {
    let user =
        GraphBuilder::binding_source("user", RecordDescriptor::new([("user", ColumnType::U64)]));
    GraphBuilder::collect_root_ordered(
        GraphBuilder::join(user, GraphBuilder::table("tasks"), ["user"], ["owner"]).project_fields(
            [
                ProjectField::renamed("left.user", "user"),
                ProjectField::renamed("right.id", "id"),
                ProjectField::renamed("right.rev", "rev"),
            ],
        ),
        ["user", "id"],
        [
            CollectByField::named("user"),
            CollectByField::named("id"),
            CollectByField::named("rev"),
        ],
        [TopByOrder::asc("id")],
        ["user"],
        0,
        TopByLimit::Unbounded,
    )
}

async fn prepare_collector_shape(database: &mut Database) -> groove::ivm::PreparedShape {
    database
        .prepare(
            [RoutedMultisinkTerminal::new(
                COLLECTOR_SINK,
                collector_shape_graph(),
                ["user"],
                ["user", "id", "rev"],
            )],
            "user",
            RecordDescriptor::new([("user", ColumnType::U64)]),
        )
        .await
        .unwrap()
}

/// Visible collector rows by terminal key: (owner, id) -> rev.
#[derive(Default)]
struct CollectorRows {
    by_key: std::collections::BTreeMap<Vec<u8>, (u64, u64, u64)>,
}

impl CollectorRows {
    /// Apply every queued tick; returns whether any tick carried an edit.
    fn drain(&mut self, subscription: &MultisinkSubscription) -> bool {
        let mut changed = false;
        while let Ok(tick) = subscription.try_recv() {
            let Some(terminal) = tick.terminal_sinks.get(COLLECTOR_SINK) else {
                continue;
            };
            changed |= !terminal.operations.is_empty();
            for operation in &terminal.operations {
                assert!(operation.path.is_empty());
                let decode = |bytes: &[u8]| {
                    let record = BorrowedRecord::new(bytes, &operation.root_descriptor);
                    let field = |index| match record.get_idx(index).unwrap() {
                        Value::U64(value) => value,
                        other => panic!("unexpected field {other:?}"),
                    };
                    (field(0), field(1), field(2))
                };
                match &operation.edit {
                    TerminalEdit::Insert { key, value, .. } => {
                        assert!(self.by_key.insert(key.clone(), decode(value)).is_none());
                    }
                    TerminalEdit::Update { key, value } => {
                        assert!(self.by_key.insert(key.clone(), decode(value)).is_some());
                    }
                    TerminalEdit::Remove { key } => {
                        assert!(self.by_key.remove(key).is_some());
                    }
                    TerminalEdit::Move { key, .. } => {
                        assert!(self.by_key.contains_key(key));
                    }
                }
            }
        }
        changed
    }

    fn rows(&self) -> Rows {
        self.by_key
            .values()
            .map(|(owner, id, rev)| ((*owner, *id), *rev))
            .collect()
    }
}

/// (nodes evaluated, subscriptions considered, notified subscribers) for one
/// write to owner 0's first task with `bindings` live owners bound to a root
/// collector shape.
async fn one_collector_write_cost(bindings: u64) -> (usize, usize, usize) {
    let mut database = Database::new(schema(), MemoryStorage::new(&["tasks"]).unwrap())
        .await
        .unwrap();
    let mut seed = database.open_batch();
    for owner in 0..bindings {
        for id in 0..TASKS_PER_OWNER {
            seed.insert("tasks", task(owner, id, 0));
        }
    }
    commit(&mut database, seed).await;
    let shape = prepare_collector_shape(&mut database).await;
    let mut subscriptions = Vec::new();
    for owner in 0..bindings {
        subscriptions.push(
            database
                .bind_shape(shape.id(), &[Value::U64(owner)])
                .await
                .unwrap(),
        );
    }
    database.drive_progress().await.unwrap();
    let mut visible = subscriptions
        .iter()
        .map(|subscription| {
            let mut rows = CollectorRows::default();
            rows.drain(subscription);
            rows
        })
        .collect::<Vec<_>>();

    let mut write = database.open_batch();
    write.update("tasks", task(0, 0, 1));
    commit(&mut database, write).await;
    database.drive_progress().await.unwrap();
    let metrics = database.last_tick_metrics().unwrap().clone();
    let mut notified = 0;
    for (owner, (subscription, rows)) in subscriptions.iter().zip(&mut visible).enumerate() {
        if rows.drain(subscription) {
            assert_eq!(owner, 0, "only the written owner's binding may change");
            notified += 1;
        }
    }
    assert_eq!(
        notified, 1,
        "the written owner's binding must see its update"
    );
    assert_eq!(visible[0].rows().get(&(0, 0)), Some(&1));
    (
        metrics.nodes_evaluated,
        metrics.subscriptions_considered,
        notified,
    )
}

#[futures_test::test]
async fn one_collector_binding_write_cost_is_independent_of_live_binding_count() {
    let small = one_collector_write_cost(10).await;
    let large = one_collector_write_cost(100).await;
    assert_eq!(
        small, large,
        "(nodes evaluated, subscriptions considered, notified) must not grow with bindings"
    );
}

/// The collector counterpart of the flat oracle above: routed activation of
/// collector bindings delivers exactly what full activation would.
#[futures_test::test]
async fn routed_collector_bindings_match_an_exact_oracle_across_moves_and_rebinding() {
    const OWNERS: u64 = 6;
    let mut database = Database::new(schema(), MemoryStorage::new(&["tasks"]).unwrap())
        .await
        .unwrap();
    let shape = prepare_collector_shape(&mut database).await;
    let mut live = Vec::new();
    for owner in [0, 1, 1, 2, 3] {
        let subscription = database
            .bind_shape(shape.id(), &[Value::U64(owner)])
            .await
            .unwrap();
        live.push((owner, subscription, CollectorRows::default()));
    }
    database.drive_progress().await.unwrap();
    let mut oracle = std::collections::BTreeMap::<(u64, u64), u64>::new();
    let mut random = 0x3308_u64;
    for step in 0..120_u64 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let mut batch = database.open_batch();
        for edit in 0..3 {
            let pick = random.rotate_left(edit * 11);
            let owner = pick % OWNERS;
            let id = (pick >> 8) % 4;
            if pick % 7 == 0 {
                if oracle.remove(&(owner, id)).is_some() {
                    batch.delete(
                        "tasks",
                        groove::db::PrimaryKeyValue::Composite(vec![
                            groove::db::PrimaryKeyValue::U64(owner),
                            groove::db::PrimaryKeyValue::U64(id),
                        ]),
                    );
                }
            } else if let std::collections::btree_map::Entry::Occupied(mut row) =
                oracle.entry((owner, id))
            {
                *row.get_mut() = step;
                batch.update("tasks", task(owner, id, step));
            } else {
                oracle.insert((owner, id), step);
                batch.insert("tasks", task(owner, id, step));
            }
        }
        commit(&mut database, batch).await;
        if step % 17 == 5 {
            let (_, subscription, _) = live.remove((random % live.len() as u64) as usize);
            database.unsubscribe(subscription.id());
        }
        if step % 13 == 3 {
            let owner = random % OWNERS;
            let subscription = database
                .bind_shape(shape.id(), &[Value::U64(owner)])
                .await
                .unwrap();
            live.push((owner, subscription, CollectorRows::default()));
        }
        database.drive_progress().await.unwrap();
        for (owner, subscription, rows) in &mut live {
            rows.drain(subscription);
            let expected = oracle
                .iter()
                .filter(|((task_owner, _), _)| task_owner == owner)
                .map(|((_, id), rev)| ((*owner, *id), *rev))
                .collect::<Rows>();
            assert_eq!(rows.rows(), expected, "owner {owner} at step {step}");
        }
    }
}
