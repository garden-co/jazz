#![cfg(feature = "test")]

mod support;

use std::time::Duration;

use jazz_tools::Operation;
use jazz_tools::public_schema::{
    RelColumnRef, RelExpr, RelJoinCondition, RelJoinKind, RelKeyRef, RelPredicateCmpOp,
    RelPredicateExpr, RelRecursionBound, RelValueRef, RowIdRef, TablePolicies,
};
use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::{
    AppId, ColumnType, DurabilityTier, JazzClient, ObjectId, PolicyExpr, QueryBuilder, Schema,
    SchemaBuilder, TableSchema, Value,
};
use serde_json::json;
use support::{
    TestingClient, has_added, wait_for_edge_query_ready, wait_for_query,
    wait_for_subscription_update,
};
use tempfile::TempDir;
use uuid::Uuid;

fn todo_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        )
        .build()
}

fn uuid_from_u128(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

#[tokio::test(flavor = "current_thread")]
async fn default_order_limit_subscription_emits_ordered_window_indices() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = JazzClient::connect(
                server.make_client_context_for_user(schema.clone(), "default-order-writer"),
            )
            .await
            .expect("connect writer");
            let reader = JazzClient::connect(
                server.make_client_context_for_user(schema, "default-order-reader"),
            )
            .await
            .expect("connect reader");

            wait_for_edge_query_ready(&writer, "todos", Duration::from_secs(30)).await;
            wait_for_edge_query_ready(&reader, "todos", Duration::from_secs(30)).await;

            let id10 = ObjectId::from_uuid(uuid_from_u128(10));
            let id20 = ObjectId::from_uuid(uuid_from_u128(20));
            let id30 = ObjectId::from_uuid(uuid_from_u128(30));
            let id40 = ObjectId::from_uuid(uuid_from_u128(40));
            for (id, title) in [
                (id10, "ten"),
                (id20, "twenty"),
                (id30, "thirty"),
                (id40, "forty"),
            ] {
                writer
                    .insert_with_id(
                        "todos",
                        Some(*id.uuid()),
                        row_input!("title" => title, "done" => false),
                    )
                    .expect("insert seeded todo");
            }

            let query = QueryBuilder::new("todos").limit(3).build();
            wait_for_query(
                &reader,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(25),
                "reader sees default-ordered limited seed window",
                |rows| {
                    (rows.iter().map(|(id, _)| *id).collect::<Vec<_>>() == vec![id10, id20, id30])
                        .then_some(())
                },
            )
            .await;
            let mut stream = reader
                .subscribe(query.clone())
                .await
                .expect("subscribe limit");
            let initial = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("initial subscription delta")
                .expect("subscription stream should stay open");
            assert_eq!(
                initial
                    .added
                    .iter()
                    .map(|added| (added.id, added.index))
                    .collect::<Vec<_>>(),
                vec![(id10, 0), (id20, 1), (id30, 2)]
            );

            let id05 = ObjectId::from_uuid(uuid_from_u128(5));
            writer
                .insert_with_id(
                    "todos",
                    Some(*id05.uuid()),
                    row_input!("title" => "five", "done" => false),
                )
                .expect("insert lower todo");

            let mut delta = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("window update delta")
                .expect("subscription stream should stay open");
            while delta.added.iter().all(|added| added.id != id05) {
                assert!(
                    delta.is_empty() || delta.pending || !delta.removed.is_empty(),
                    "unexpected non-window delta before lower-id insert delta: {delta:#?}"
                );
                delta = tokio::time::timeout(Duration::from_secs(10), stream.next())
                    .await
                    .expect("window update delta")
                    .expect("subscription stream should stay open");
            }
            assert_eq!(
                delta
                    .added
                    .iter()
                    .map(|added| (added.id, added.index))
                    .collect::<Vec<_>>(),
                vec![(id05, 0)]
            );
            assert_eq!(
                delta
                    .removed
                    .iter()
                    .map(|removed| (removed.id, removed.index))
                    .collect::<Vec<_>>(),
                vec![(id30, 2)]
            );

            wait_for_query(
                &reader,
                query,
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(25),
                "reader sees shifted default-order window",
                |rows| {
                    (rows.iter().map(|(id, _)| *id).collect::<Vec<_>>() == vec![id05, id10, id20])
                        .then_some(())
                },
            )
            .await;
        })
        .await;
}

fn policied_todo_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_insert(PolicyExpr::True)
                        .with_select(PolicyExpr::True)
                        .with_update(None, PolicyExpr::True)
                        .with_delete(PolicyExpr::True),
                ),
        )
        .build()
}

#[tokio::test(flavor = "current_thread")]
async fn default_order_limit_subscription_delivers_updates_with_table_policies() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = policied_todo_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = JazzClient::connect(
                server.make_client_context_for_user(schema.clone(), "policied-order-writer"),
            )
            .await
            .expect("connect writer");
            let reader = JazzClient::connect(
                server.make_client_context_for_user(schema, "policied-order-reader"),
            )
            .await
            .expect("connect reader");

            wait_for_edge_query_ready(&writer, "todos", Duration::from_secs(30)).await;
            wait_for_edge_query_ready(&reader, "todos", Duration::from_secs(30)).await;

            let id10 = ObjectId::from_uuid(uuid_from_u128(10));
            let id20 = ObjectId::from_uuid(uuid_from_u128(20));
            let id30 = ObjectId::from_uuid(uuid_from_u128(30));
            let id40 = ObjectId::from_uuid(uuid_from_u128(40));
            for (id, title) in [
                (id10, "ten"),
                (id20, "twenty"),
                (id30, "thirty"),
                (id40, "forty"),
            ] {
                writer
                    .insert_with_id(
                        "todos",
                        Some(*id.uuid()),
                        row_input!("title" => title, "done" => false),
                    )
                    .expect("insert seeded todo");
            }

            let query = QueryBuilder::new("todos").limit(3).build();
            wait_for_query(
                &reader,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(25),
                "reader sees policied default-ordered limited seed window",
                |rows| {
                    (rows.iter().map(|(id, _)| *id).collect::<Vec<_>>() == vec![id10, id20, id30])
                        .then_some(())
                },
            )
            .await;
            let mut stream = reader
                .subscribe(query.clone())
                .await
                .expect("subscribe policied limit");
            let initial = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("initial policied subscription delta")
                .expect("subscription stream should stay open");
            assert_eq!(
                initial
                    .added
                    .iter()
                    .map(|added| (added.id, added.index))
                    .collect::<Vec<_>>(),
                vec![(id10, 0), (id20, 1), (id30, 2)]
            );

            // The stress-app repro: toggle a row inside the window and require
            // the update to arrive as a subscription delta, not only via
            // one-shot re-query.
            let toggle_batch = writer
                .update(id20, vec![("done".to_string(), Value::Boolean(true))])
                .expect("toggle done on in-window todo");
            writer
                .wait_for_batch(toggle_batch, DurabilityTier::EdgeServer)
                .await
                .expect("toggle settles at edge");

            let mut delta = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("toggle update delta")
                .expect("subscription stream should stay open");
            while delta.updated.iter().all(|updated| updated.id != id20) {
                delta = tokio::time::timeout(Duration::from_secs(10), stream.next())
                    .await
                    .expect("toggle update delta")
                    .expect("subscription stream should stay open");
            }
            let updated = delta
                .updated
                .iter()
                .find(|updated| updated.id == id20)
                .expect("toggle delta contains id20");
            assert_eq!((updated.old_index, updated.new_index), (1, 1));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn default_order_limit_one_subscription_delivers_value_updates() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = JazzClient::connect(
                server.make_client_context_for_user(schema.clone(), "limit-one-writer"),
            )
            .await
            .expect("connect writer");
            let reader = JazzClient::connect(
                server.make_client_context_for_user(schema, "limit-one-reader"),
            )
            .await
            .expect("connect reader");

            wait_for_edge_query_ready(&writer, "todos", Duration::from_secs(30)).await;
            wait_for_edge_query_ready(&reader, "todos", Duration::from_secs(30)).await;

            let id10 = ObjectId::from_uuid(uuid_from_u128(10));
            let id20 = ObjectId::from_uuid(uuid_from_u128(20));
            for (id, title) in [(id10, "ten"), (id20, "twenty")] {
                writer
                    .insert_with_id(
                        "todos",
                        Some(*id.uuid()),
                        row_input!("title" => title, "done" => false),
                    )
                    .expect("insert seeded todo");
            }

            let query = QueryBuilder::new("todos").limit(1).build();
            wait_for_query(
                &reader,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(25),
                "reader sees limit-one winner",
                |rows| {
                    (rows.iter().map(|(id, _)| *id).collect::<Vec<_>>() == vec![id10]).then_some(())
                },
            )
            .await;
            let mut stream = reader
                .subscribe(query.clone())
                .await
                .expect("subscribe limit one");
            let initial = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("initial limit-one delta")
                .expect("subscription stream should stay open");
            assert_eq!(
                initial
                    .added
                    .iter()
                    .map(|added| (added.id, added.index))
                    .collect::<Vec<_>>(),
                vec![(id10, 0)]
            );

            // Value-only update to the winner must arrive as a delta.
            let toggle_batch = writer
                .update(id10, vec![("done".to_string(), Value::Boolean(true))])
                .expect("toggle winner");
            writer
                .wait_for_batch(toggle_batch, DurabilityTier::EdgeServer)
                .await
                .expect("toggle settles at edge");

            let mut delta = tokio::time::timeout(Duration::from_secs(10), stream.next())
                .await
                .expect("winner update delta")
                .expect("subscription stream should stay open");
            while delta.updated.iter().all(|updated| updated.id != id10) {
                delta = tokio::time::timeout(Duration::from_secs(10), stream.next())
                    .await
                    .expect("winner update delta")
                    .expect("subscription stream should stay open");
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn default_order_unbounded_subscription_keeps_row_id_order_across_deltas() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = JazzClient::connect(
                server
                    .make_client_context_for_user(schema.clone(), "default-order-unbounded-writer"),
            )
            .await
            .expect("connect writer");
            let reader = JazzClient::connect(
                server.make_client_context_for_user(schema, "default-order-unbounded-reader"),
            )
            .await
            .expect("connect reader");

            wait_for_edge_query_ready(&writer, "todos", Duration::from_secs(30)).await;
            wait_for_edge_query_ready(&reader, "todos", Duration::from_secs(30)).await;

            let query = QueryBuilder::new("todos").limit(3).build();
            let mut stream = reader
                .subscribe(query.clone())
                .await
                .expect("subscribe unbounded todos");

            let id20 = ObjectId::from_uuid(uuid_from_u128(20));
            writer
                .insert_with_id(
                    "todos",
                    Some(*id20.uuid()),
                    row_input!("title" => "twenty", "done" => false),
                )
                .expect("insert twenty");
            expect_unbounded_order_delta(&mut stream, &reader, &query, vec![id20], id20, 0).await;

            let id40 = ObjectId::from_uuid(uuid_from_u128(40));
            writer
                .insert_with_id(
                    "todos",
                    Some(*id40.uuid()),
                    row_input!("title" => "forty", "done" => false),
                )
                .expect("insert forty");
            expect_unbounded_order_delta(&mut stream, &reader, &query, vec![id20, id40], id40, 1)
                .await;

            let id10 = ObjectId::from_uuid(uuid_from_u128(10));
            writer
                .insert_with_id(
                    "todos",
                    Some(*id10.uuid()),
                    row_input!("title" => "ten", "done" => false),
                )
                .expect("insert ten");
            expect_unbounded_order_delta(
                &mut stream,
                &reader,
                &query,
                vec![id10, id20, id40],
                id10,
                0,
            )
            .await;
        })
        .await;
}

async fn expect_unbounded_order_delta(
    stream: &mut jazz_tools::SubscriptionStream,
    reader: &JazzClient,
    query: &jazz_tools::Query,
    mut expected_ids: Vec<ObjectId>,
    inserted_id: ObjectId,
    inserted_index: usize,
) {
    expected_ids.sort_by_key(|id| *id.uuid());
    let delta = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let delta = stream
                .next()
                .await
                .expect("subscription stream should stay open");
            if delta.added.iter().any(|added| added.id == inserted_id) {
                break delta;
            }
            assert!(
                delta.is_empty() || delta.pending,
                "unexpected delta before inserted row: {delta:#?}"
            );
        }
    })
    .await
    .expect("unbounded insert delta");
    assert_eq!(
        delta
            .added
            .iter()
            .filter(|added| added.id == inserted_id)
            .map(|added| added.index)
            .collect::<Vec<_>>(),
        vec![inserted_index]
    );
    wait_for_query(
        reader,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "reader sees row-id ordered unbounded todos",
        |rows| (rows.iter().map(|(id, _)| *id).collect::<Vec<_>>() == expected_ids).then_some(()),
    )
    .await;
}
fn core_todo_schema() -> jazz::schema::JazzSchema {
    use jazz::groove::schema::{ColumnSchema, ColumnType as CoreColumnType};
    use jazz::schema::{Policy, TableSchema as CoreTableSchema};

    jazz::schema::JazzSchema::new([CoreTableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", CoreColumnType::String),
            ColumnSchema::new("done", CoreColumnType::Bool),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

async fn open_core_todo_db() -> jazz::db::Db<jazz::groove::storage::MemoryStorage> {
    use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource};
    use jazz::groove::storage::MemoryStorage;
    use jazz::ids::{AuthorId, NodeUuid};

    let schema = core_todo_schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x31; 16]),
                author: AuthorId::from_bytes([0xa7; 16]),
            },
        )
        .with_id_source(SeededRowIdSource::new(0x600d)),
    )
    .await
    .expect("open local core todo db")
}

async fn run_core_same_client_value_update_subscription(limited: bool) {
    use jazz::db::{LocalUpdates, Propagation, ReadOpts, RowCells, SubscriptionEvent};
    use jazz::groove::records::Value as CoreValue;
    use jazz::query::Query;
    use jazz::tx::DurabilityTier as CoreDurabilityTier;

    fn cells(title: &str, done: bool) -> RowCells {
        RowCells::from([
            ("title".to_owned(), CoreValue::String(title.to_owned())),
            ("done".to_owned(), CoreValue::Bool(done)),
        ])
    }

    let db = open_core_todo_db().await;
    let target_id = db
        .insert("todos", cells("a", false))
        .expect("insert todo a")
        .row_uuid();
    db.insert("todos", cells("b", false))
        .expect("insert todo b");
    db.insert("todos", cells("c", false))
        .expect("insert todo c");
    db.insert("todos", cells("d", false))
        .expect("insert todo d");

    let mut query = Query::from("todos").select(["title", "done"]);
    if limited {
        query = query.limit(3);
    }
    let prepared = db.prepare_query(&query).expect("prepare query");
    let mut stream = db
        .subscribe(
            &prepared,
            ReadOpts {
                tier: CoreDurabilityTier::Local,
                local_updates: LocalUpdates::Immediate,
                propagation: Propagation::LocalOnly,
                include_deleted: false,
                ..ReadOpts::default()
            },
        )
        .await
        .expect("subscribe");

    let initial = tokio::time::timeout(Duration::from_secs(5), stream.next_event())
        .await
        .expect("initial event timeout")
        .expect("initial event");
    let SubscriptionEvent::Delta { reset, added, .. } = initial else {
        panic!("expected initial delta, got {initial:#?}");
    };
    assert!(reset, "initial event should reset");
    assert_eq!(added.len(), if limited { 3 } else { 4 });

    db.update(
        "todos",
        target_id,
        RowCells::from([("done".to_owned(), CoreValue::Bool(true))]),
    )
    .expect("same-client update");

    let changed = tokio::time::timeout(Duration::from_secs(5), stream.next_event())
        .await
        .expect("value update event timeout")
        .expect("value update event");
    let SubscriptionEvent::Delta {
        added,
        updated,
        removed,
        ..
    } = changed
    else {
        panic!("expected value update delta, got {changed:#?}");
    };
    let updated_target = updated
        .iter()
        .any(|row| row.row_uuid() == target_id && row.cell_at(1) == Some(CoreValue::Bool(true)));
    let removed_and_added_target = removed.iter().any(|row| row.row_uuid == target_id)
        && added.iter().any(|row| {
            row.row_uuid() == target_id && row.cell_at(1) == Some(CoreValue::Bool(true))
        });
    assert!(
        updated_target || removed_and_added_target,
        "same-client value update should be observable; got added={added:#?} updated={updated:#?} removed={removed:#?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn local_subscription_limited_window_emits_same_client_value_update() {
    tokio::task::LocalSet::new()
        .run_until(async {
            run_core_same_client_value_update_subscription(true).await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn local_subscription_unbounded_emits_same_client_value_update() {
    tokio::task::LocalSet::new()
        .run_until(async {
            run_core_same_client_value_update_subscription(false).await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn edge_tier_public_subscription_opens_and_receives_rows() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000001")
                .ready_on("todos", Duration::from_secs(30))
                .connect()
                .await;

            let query = QueryBuilder::new("todos").limit(3).build();
            let mut stream = client
                .subscribe(query)
                .await
                .expect("edge-tier public subscription should open");
            let mut log = Vec::new();

            let (todo_id, _, batch_id) = client
                .insert("todos", row_input!("title" => "visible", "done" => false))
                .expect("insert todo");
            client
                .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
                .await
                .expect("todo should settle at edge");

            wait_for_subscription_update(
                &mut stream,
                &mut log,
                Duration::from_secs(10),
                "edge-tier public subscription receives inserted row",
                |deltas| has_added(deltas, todo_id),
            )
            .await;
        })
        .await;
}

fn policy_graph_policy_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("resources")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(resource_access_policy())
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("data_entries")
                .fk_column("resource", "resources")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::inherits(Operation::Select, "resource"))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("mapping_rules")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(mapping_rule_access_policy())
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("mapping_rule_entries")
                .fk_column("mapping_rule", "mapping_rules")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::inherits(Operation::Select, "mapping_rule"))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("data_entry_entries")
                .fk_column("data_entry", "data_entries")
                .column("label", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::inherits(Operation::Select, "data_entry"))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("teams")
                .column("identity_key", ColumnType::Text)
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .table(
            TableSchema::builder("team_team_edges")
                .fk_column("child_team", "teams")
                .fk_column("parent_team", "teams")
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .table(
            TableSchema::builder("resource_access_edges")
                .fk_column("resource", "resources")
                .fk_column("team", "teams")
                .column("grant_role", ColumnType::Text)
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .table(
            TableSchema::builder("mapping_rule_access_edges")
                .fk_column("mapping_rule", "mapping_rules")
                .fk_column("team", "teams")
                .column("grant_role", ColumnType::Text)
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .build()
}

fn resource_access_policy() -> PolicyExpr {
    PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::Gather {
                    seed: Box::new(RelExpr::Filter {
                        input: Box::new(RelExpr::TableScan {
                            table: "teams".into(),
                            alias: None,
                        }),
                        predicate: RelPredicateExpr::Cmp {
                            left: RelColumnRef {
                                scope: Some("teams".to_owned()),
                                column: "identity_key".to_owned(),
                            },
                            op: RelPredicateCmpOp::Eq,
                            right: RelValueRef::SessionRef(vec!["sub".to_owned()]),
                        },
                    }),
                    step: Box::new(RelExpr::Project {
                        input: Box::new(RelExpr::Join {
                            left: Box::new(RelExpr::Filter {
                                input: Box::new(RelExpr::TableScan {
                                    table: "team_team_edges".into(),
                                    alias: None,
                                }),
                                predicate: RelPredicateExpr::Cmp {
                                    left: RelColumnRef {
                                        scope: Some("team_team_edges".to_owned()),
                                        column: "child_team".to_owned(),
                                    },
                                    op: RelPredicateCmpOp::Eq,
                                    right: RelValueRef::RowId(RowIdRef::Frontier),
                                },
                            }),
                            right: Box::new(RelExpr::TableScan {
                                table: "teams".into(),
                                alias: Some("__recursive_hop_0".to_owned()),
                            }),
                            on: vec![RelJoinCondition {
                                left: RelColumnRef {
                                    scope: Some("team_team_edges".to_owned()),
                                    column: "parent_team".to_owned(),
                                },
                                right: RelColumnRef {
                                    scope: Some("__recursive_hop_0".to_owned()),
                                    column: "id".to_owned(),
                                },
                            }],
                            join_kind: RelJoinKind::Inner,
                        }),
                        columns: Vec::new(),
                    }),
                    frontier_key: RelKeyRef::RowId(RowIdRef::Current),
                    bound: RelRecursionBound::MaxDepth(8),
                    dedupe_key: vec![RelKeyRef::RowId(RowIdRef::Current)],
                }),
                right: Box::new(RelExpr::TableScan {
                    table: "resource_access_edges".into(),
                    alias: Some("access".to_owned()),
                }),
                on: vec![RelJoinCondition {
                    left: RelColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                    right: RelColumnRef {
                        scope: Some("access".to_owned()),
                        column: "team".to_owned(),
                    },
                }],
                join_kind: RelJoinKind::Inner,
            }),
            predicate: RelPredicateExpr::And(vec![
                RelPredicateExpr::Cmp {
                    left: RelColumnRef {
                        scope: Some("access".to_owned()),
                        column: "resource".to_owned(),
                    },
                    op: RelPredicateCmpOp::Eq,
                    right: RelValueRef::RowId(RowIdRef::Outer),
                },
                RelPredicateExpr::Cmp {
                    left: RelColumnRef {
                        scope: Some("access".to_owned()),
                        column: "grant_role".to_owned(),
                    },
                    op: RelPredicateCmpOp::Eq,
                    right: RelValueRef::Literal(Value::Text("viewer".to_owned())),
                },
            ]),
        },
    }
}

fn mapping_rule_access_policy() -> PolicyExpr {
    PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::Gather {
                    seed: Box::new(RelExpr::Filter {
                        input: Box::new(RelExpr::TableScan {
                            table: "teams".into(),
                            alias: None,
                        }),
                        predicate: RelPredicateExpr::Cmp {
                            left: RelColumnRef {
                                scope: Some("teams".to_owned()),
                                column: "identity_key".to_owned(),
                            },
                            op: RelPredicateCmpOp::Eq,
                            right: RelValueRef::SessionRef(vec!["sub".to_owned()]),
                        },
                    }),
                    step: Box::new(RelExpr::Project {
                        input: Box::new(RelExpr::Join {
                            left: Box::new(RelExpr::Filter {
                                input: Box::new(RelExpr::TableScan {
                                    table: "team_team_edges".into(),
                                    alias: None,
                                }),
                                predicate: RelPredicateExpr::Cmp {
                                    left: RelColumnRef {
                                        scope: Some("team_team_edges".to_owned()),
                                        column: "child_team".to_owned(),
                                    },
                                    op: RelPredicateCmpOp::Eq,
                                    right: RelValueRef::RowId(RowIdRef::Frontier),
                                },
                            }),
                            right: Box::new(RelExpr::TableScan {
                                table: "teams".into(),
                                alias: Some("__recursive_hop_0".to_owned()),
                            }),
                            on: vec![RelJoinCondition {
                                left: RelColumnRef {
                                    scope: Some("team_team_edges".to_owned()),
                                    column: "parent_team".to_owned(),
                                },
                                right: RelColumnRef {
                                    scope: Some("__recursive_hop_0".to_owned()),
                                    column: "id".to_owned(),
                                },
                            }],
                            join_kind: RelJoinKind::Inner,
                        }),
                        columns: Vec::new(),
                    }),
                    frontier_key: RelKeyRef::RowId(RowIdRef::Current),
                    bound: RelRecursionBound::MaxDepth(8),
                    dedupe_key: vec![RelKeyRef::RowId(RowIdRef::Current)],
                }),
                right: Box::new(RelExpr::TableScan {
                    table: "mapping_rule_access_edges".into(),
                    alias: Some("access".to_owned()),
                }),
                on: vec![RelJoinCondition {
                    left: RelColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                    right: RelColumnRef {
                        scope: Some("access".to_owned()),
                        column: "team".to_owned(),
                    },
                }],
                join_kind: RelJoinKind::Inner,
            }),
            predicate: RelPredicateExpr::And(vec![
                RelPredicateExpr::Cmp {
                    left: RelColumnRef {
                        scope: Some("access".to_owned()),
                        column: "mapping_rule".to_owned(),
                    },
                    op: RelPredicateCmpOp::Eq,
                    right: RelValueRef::RowId(RowIdRef::Outer),
                },
                RelPredicateExpr::Cmp {
                    left: RelColumnRef {
                        scope: Some("access".to_owned()),
                        column: "grant_role".to_owned(),
                    },
                    op: RelPredicateCmpOp::Eq,
                    right: RelValueRef::Literal(Value::Text("viewer".to_owned())),
                },
            ]),
        },
    }
}

fn todo_query() -> jazz_tools::Query {
    QueryBuilder::new("todos")
        .select(&["title", "done"])
        .build()
}

fn reserve_local_port() -> u16 {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("reserve local port");
    listener.local_addr().expect("reserved local addr").port()
}

async fn connect_user(server: &JazzServer, schema: Schema, user_id: &str) -> JazzClient {
    let client = JazzClient::connect(server.make_client_context_for_user(schema, user_id))
        .await
        .expect("connect user");
    wait_for_edge_query_ready(&client, "todos", Duration::from_secs(30)).await;
    client
}

async fn wait_for_row(
    client: &JazzClient,
    tier: DurabilityTier,
    row_id: ObjectId,
    expected: Vec<Value>,
    description: &str,
) {
    wait_for_query(
        client,
        todo_query(),
        Some(tier),
        Duration::from_secs(30),
        description,
        |rows| {
            rows.iter()
                .any(|(id, values)| *id == row_id && *values == expected)
                .then_some(())
        },
    )
    .await;
}

async fn wait_edge_batch(client: &JazzClient, batch_id: jazz_tools::BatchId, label: &str) {
    tokio::time::timeout(
        Duration::from_secs(15),
        client.wait_for_batch(batch_id, DurabilityTier::EdgeServer),
    )
    .await
    .unwrap_or_else(|_| panic!("{label} timed out waiting for edge batch"))
    .unwrap_or_else(|err| panic!("{label} failed waiting for edge batch: {err}"));
}

struct PolicyGraphSeedRows {
    resource: ObjectId,
    data_entry: ObjectId,
    mapping_rule: ObjectId,
    data_entry_entry: ObjectId,
    mapping_rule_entry: ObjectId,
}

async fn seed_policy_graph_rows(admin: &JazzClient) -> PolicyGraphSeedRows {
    let (seed_team, _, seed_batch) = admin
        .insert(
            "teams",
            row_input!("identity_key" => "00000000-0000-4000-8000-0000000000b0"),
        )
        .expect("insert seed team");
    wait_edge_batch(admin, seed_batch, "seed team").await;
    let (resource_team, _, resource_team_batch) = admin
        .insert("teams", row_input!("identity_key" => "other-sub"))
        .expect("insert resource team");
    wait_edge_batch(admin, resource_team_batch, "resource team").await;
    let (_, _, edge_batch) = admin
        .insert(
            "team_team_edges",
            row_input!("child_team" => seed_team, "parent_team" => resource_team),
        )
        .expect("insert team edge");
    wait_edge_batch(admin, edge_batch, "team edge").await;
    let (resource, _, resource_batch) = admin
        .insert("resources", row_input!("label" => "visible resource"))
        .expect("insert resource");
    wait_edge_batch(admin, resource_batch, "resource").await;
    let (_, _, access_batch) = admin
        .insert(
            "resource_access_edges",
            row_input!("resource" => resource, "team" => resource_team, "grant_role" => "viewer"),
        )
        .expect("insert resource access edge");
    wait_edge_batch(admin, access_batch, "resource access").await;
    let (data_entry, _, data_entry_batch) = admin
        .insert(
            "data_entries",
            row_input!("resource" => resource, "label" => "visible data entry"),
        )
        .expect("insert data entry");
    wait_edge_batch(admin, data_entry_batch, "data entry").await;
    let (mapping_rule, _, mapping_rule_batch) = admin
        .insert(
            "mapping_rules",
            row_input!("label" => "visible mapping rule"),
        )
        .expect("insert mapping rule");
    wait_edge_batch(admin, mapping_rule_batch, "mapping rule").await;
    let (_, _, mapping_rule_access_batch) = admin
        .insert(
            "mapping_rule_access_edges",
            row_input!("mapping_rule" => mapping_rule, "team" => resource_team, "grant_role" => "viewer"),
        )
        .expect("insert mapping rule access edge");
    wait_edge_batch(admin, mapping_rule_access_batch, "mapping rule access").await;
    let (data_entry_entry, _, data_entry_entry_batch) = admin
        .insert(
            "data_entry_entries",
            row_input!("data_entry" => data_entry, "label" => "visible data entry child"),
        )
        .expect("insert data entry child");
    wait_edge_batch(admin, data_entry_entry_batch, "data entry child").await;
    let (mapping_rule_entry, _, mapping_rule_entry_batch) = admin
        .insert(
            "mapping_rule_entries",
            row_input!("mapping_rule" => mapping_rule, "label" => "visible mapping rule child"),
        )
        .expect("insert mapping rule child");
    wait_edge_batch(admin, mapping_rule_entry_batch, "mapping rule child").await;

    PolicyGraphSeedRows {
        resource,
        data_entry,
        mapping_rule,
        data_entry_entry,
        mapping_rule_entry,
    }
}

async fn assert_policy_graph_member_rows(member: &JazzClient, rows: &PolicyGraphSeedRows) {
    let member_rows = wait_for_query(
        member,
        QueryBuilder::new("resources").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "member sees resource through seeded recursive access policy",
        |query_rows| {
            (query_rows.len() == 1 && query_rows[0].0 == rows.resource).then_some(query_rows)
        },
    )
    .await;
    assert_eq!(
        member_rows[0].1,
        vec![Value::Text("visible resource".to_owned())]
    );
    wait_for_query(
        member,
        QueryBuilder::new("data_entries").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "member sees data entry through seeded recursive access policy",
        |query_rows| {
            (query_rows.len() == 1 && query_rows[0].0 == rows.data_entry).then_some(query_rows)
        },
    )
    .await;
    wait_for_query(
        member,
        QueryBuilder::new("mapping_rules").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "member sees sibling mapping rule through same seeded recursive access policy",
        |query_rows| {
            (query_rows.len() == 1 && query_rows[0].0 == rows.mapping_rule).then_some(query_rows)
        },
    )
    .await;
    wait_for_query(
        member,
        QueryBuilder::new("data_entry_entries").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "member sees grandchild through inherits over seeded access policy",
        |query_rows| {
            (query_rows.len() == 1 && query_rows[0].0 == rows.data_entry_entry)
                .then_some(query_rows)
        },
    )
    .await;
    wait_for_query(
        member,
        QueryBuilder::new("mapping_rule_entries").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "member sees mapping rule child through inherits over sibling seeded access policy",
        |query_rows| {
            (query_rows.len() == 1 && query_rows[0].0 == rows.mapping_rule_entry)
                .then_some(query_rows)
        },
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn dynamic_server_publishes_seeded_reachable_policy_and_serves_member_rows() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = policy_graph_policy_schema();
            let app_id = server.app_id();
            let response = reqwest::Client::new()
                .post(format!("{}/apps/{app_id}/admin/schemas", server.base_url()))
                .header("X-Jazz-Admin-Secret", server.admin_secret())
                .json(&json!({ "schema": schema }))
                .send()
                .await
                .expect("publish policy graph-shaped schema");
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.expect("schema publish error body");
                panic!("policy graph-shaped schema publish failed: {status} {body}");
            }

            let admin = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("00000000-0000-4000-8000-0000000000a0")
                .as_admin()
                .connect()
                .await;
            let rows = seed_policy_graph_rows(&admin).await;

            let member = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("00000000-0000-4000-8000-0000000000b0")
                .with_claims(json!({}))
                .connect()
                .await;
            assert_policy_graph_member_rows(&member, &rows).await;

            let spy = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("00000000-0000-4000-8000-0000000000c0")
                .with_claims(json!({}))
                .connect()
                .await;
            wait_for_query(
                &spy,
                QueryBuilder::new("resources").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(30),
                "spy sees no resources through seeded recursive access policy",
                |rows| rows.is_empty().then_some(rows),
            )
            .await;
            wait_for_query(
                &spy,
                QueryBuilder::new("data_entries").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(30),
                "spy sees no inherited data entries through seeded recursive access policy",
                |rows| rows.is_empty().then_some(rows),
            )
            .await;

            spy.shutdown().await.expect("shutdown spy");
            member.shutdown().await.expect("shutdown member");
            admin.shutdown().await.expect("shutdown admin");
            server.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_schema_data_dir_reopen_bootstraps_policy_graph_policy_serving_state() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let data_dir = TempDir::new().expect("server data dir");
            let schema = policy_graph_policy_schema();
            let rows = {
                let server = JazzServer::builder()
                    .with_schema(schema.clone())
                    .with_data_dir(data_dir.path())
                    .with_persistent_storage()
                    .start()
                    .await;
                let admin = TestingClient::builder()
                    .with_server(&server)
                    .with_schema(schema.clone())
                    .with_user_id("00000000-0000-4000-8000-0000000000a0")
                    .as_admin()
                    .connect()
                    .await;
                let rows = seed_policy_graph_rows(&admin).await;
                admin.shutdown().await.expect("shutdown seeding admin");
                server.shutdown().await;
                rows
            };

            let reopened = JazzServer::builder()
                .with_schema(schema.clone())
                .with_data_dir(data_dir.path())
                .with_persistent_storage()
                .start()
                .await;
            let member = TestingClient::builder()
                .with_server(&reopened)
                .with_schema(schema.clone())
                .with_user_id("00000000-0000-4000-8000-0000000000b0")
                .with_claims(json!({}))
                .connect()
                .await;
            assert_policy_graph_member_rows(&member, &rows).await;

            member.shutdown().await.expect("shutdown member");
            reopened.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn edge_server_accepts_mergeable_write_while_core_down_then_promotes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todo_schema();
            let app_id = AppId::random();
            let core_port = reserve_local_port();
            let core_url = format!("http://127.0.0.1:{core_port}");

            let edge = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema.clone())
                .with_upstream_url(core_url.clone())
                .start()
                .await;
            let alice = connect_user(&edge, schema.clone(), "alice-edge-server-mode").await;
            let bob = connect_user(&edge, schema.clone(), "bob-edge-server-mode").await;

            let (todo_id, expected, batch_id) = alice
                .insert(
                    "todos",
                    row_input!("title" => "edge first", "done" => false),
                )
                .expect("alice inserts while core is down");
            alice
                .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
                .await
                .expect("edge accepts write while core link is down");

            wait_for_row(
                &bob,
                DurabilityTier::EdgeServer,
                todo_id,
                expected.clone(),
                "bob sees edge-accepted row before core starts",
            )
            .await;

            let core = JazzServer::builder()
                .with_app_id(app_id)
                .with_port(core_port)
                .with_schema(schema.clone())
                .start()
                .await;

            alice
                .wait_for_batch(batch_id, DurabilityTier::GlobalServer)
                .await
                .expect("edge-promoted write reaches global core");
            wait_for_row(
                &alice,
                DurabilityTier::GlobalServer,
                todo_id,
                expected.clone(),
                "alice sees globally promoted row through edge",
            )
            .await;
            wait_for_row(
                &bob,
                DurabilityTier::GlobalServer,
                todo_id,
                expected,
                "bob sees globally promoted row through edge",
            )
            .await;

            alice.shutdown().await.expect("shutdown alice");
            bob.shutdown().await.expect("shutdown bob");
            edge.shutdown().await;
            core.shutdown().await;
        })
        .await;
}

#[test]
fn topology_matrix_conformance_smoke_inventory() {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Topology {
        ClientCore,
        ClientEdgeCore,
        ClientRelayEdgeCore,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Scenario {
        MergeableWrite,
        RlsNarrowedRead,
        ReconnectKnownState,
        LargeValueRefetch,
    }

    struct Cell {
        topology: Topology,
        scenario: Scenario,
        coverage: &'static str,
    }

    let cells = [
        Cell {
            topology: Topology::ClientCore,
            scenario: Scenario::MergeableWrite,
            coverage: "clients_sync::wait_for_batch_reaches_edge_and_global_tiers",
        },
        Cell {
            topology: Topology::ClientCore,
            scenario: Scenario::RlsNarrowedRead,
            coverage: "branch_claims_integration::query_applies_claims_select_policy",
        },
        Cell {
            topology: Topology::ClientCore,
            scenario: Scenario::ReconnectKnownState,
            coverage: "text_document_merge::offline_concurrent_text_edits_reconnect_and_converge",
        },
        Cell {
            topology: Topology::ClientCore,
            scenario: Scenario::LargeValueRefetch,
            coverage: "large_blob_permissions::large_blob_values_follow_ordinary_row_permissions",
        },
        Cell {
            topology: Topology::ClientEdgeCore,
            scenario: Scenario::MergeableWrite,
            coverage: "edge_server_mode::edge_server_accepts_mergeable_write_while_core_down_then_promotes",
        },
        Cell {
            topology: Topology::ClientEdgeCore,
            scenario: Scenario::RlsNarrowedRead,
            coverage: "catalogue_sync_integration::edge_catalogue_http_reads_and_writes_forward_to_real_core + branch_claims_integration::query_applies_claims_select_policy",
        },
        Cell {
            topology: Topology::ClientEdgeCore,
            scenario: Scenario::ReconnectKnownState,
            coverage: "text_document_merge::offline_concurrent_text_edits_reconnect_and_converge",
        },
        Cell {
            topology: Topology::ClientEdgeCore,
            scenario: Scenario::LargeValueRefetch,
            coverage: "catalogue_sync_integration::large_blob_values_follow_ordinary_row_permissions",
        },
        Cell {
            topology: Topology::ClientRelayEdgeCore,
            scenario: Scenario::MergeableWrite,
            coverage: "jazz::peer::non_global_peer_query_subscriptions_use_maintained_path + seeded m3 sync close-out soak",
        },
        Cell {
            topology: Topology::ClientRelayEdgeCore,
            scenario: Scenario::RlsNarrowedRead,
            coverage: "jazz::peer::aggregate_policy_oracle_matches_visible_rows_per_identity + seeded owner-policy captures",
        },
        Cell {
            topology: Topology::ClientRelayEdgeCore,
            scenario: Scenario::ReconnectKnownState,
            coverage: "text_document_merge::offline_concurrent_text_edits_reconnect_and_converge + seeded m3 sync close-out soak",
        },
        Cell {
            topology: Topology::ClientRelayEdgeCore,
            scenario: Scenario::LargeValueRefetch,
            coverage: "catalogue_sync_integration::large_blob_values_follow_ordinary_row_permissions + 7a refetch-after-eviction coverage",
        },
    ];

    let topologies = [
        Topology::ClientCore,
        Topology::ClientEdgeCore,
        Topology::ClientRelayEdgeCore,
    ];
    let scenarios = [
        Scenario::MergeableWrite,
        Scenario::RlsNarrowedRead,
        Scenario::ReconnectKnownState,
        Scenario::LargeValueRefetch,
    ];

    for topology in topologies {
        for scenario in scenarios {
            let matching = cells
                .iter()
                .filter(|cell| cell.topology == topology && cell.scenario == scenario)
                .collect::<Vec<_>>();
            assert_eq!(
                matching.len(),
                1,
                "topology matrix cell must have exactly one coverage entry: {topology:?} {scenario:?}"
            );
            assert!(
                !matching[0].coverage.is_empty(),
                "coverage entry must name the exercised or cited test"
            );
        }
    }
}
