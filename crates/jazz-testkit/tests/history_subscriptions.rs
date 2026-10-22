use std::time::Duration;

use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::row_input;
use jazz::tools::test_support::AllowAll;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, OrderedRowDelta, ReadTier, Schema,
    SchemaBuilder, Session, SubscriptionStream, SubscriptionStreamItem, TableSchema, Value,
};
use jazz_server::JazzServer;
use jazz_testkit::TestingClient;

fn session(name: &str, account: u128) -> Session {
    let mut session = Session::new(jazz_server::TEST_JWT_ISSUER, name);
    session.account_id = Some(jazz::account_registry::AccountId(uuid::Uuid::from_u128(
        account,
    )));
    session
}

async fn backend(schema: Schema) -> (JazzServer, JazzClient) {
    let server = JazzServer::start_with_schema(schema.clone()).await.unwrap();
    let client = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("history-backend")
        .as_admin()
        .ready_on("tasks", Duration::from_secs(30))
        .connect()
        .await;
    (server, client)
}

async fn settled(client: &JazzClient, tx: Option<jazz::tools::TransactionId>) {
    tokio::time::timeout(
        Duration::from_secs(10),
        client.wait_for_transaction(tx.unwrap(), DurabilityTier::GlobalServer),
    )
    .await
    .expect("write settles")
    .unwrap();
}

async fn next(stream: &mut SubscriptionStream) -> OrderedRowDelta {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match stream.next().await.expect("subscription remains open") {
                SubscriptionStreamItem::Delta(delta) if !delta.is_empty() => return delta,
                SubscriptionStreamItem::Delta(_) => {}
                other => panic!("subscription failed: {other:?}"),
            }
        }
    })
    .await
    .expect("subscription emits its expected change")
}

fn history(row: ObjectId) -> Query {
    Query::from("history.tasks")
        .filter(eq(col("id"), lit(jazz::records::Value::Uuid(*row.uuid()))))
        .order_by("$acceptedAt", OrderDirection::Asc)
}

/// Alice watches locally retained history on a backend client while Bob's
/// mutations settle at the server. Equal projections retain distinct identities;
/// delete/restore hide and reveal content versions without adding event rows.
/// bob -> server acceptance -> alice's local history subscription
#[tokio::test(flavor = "current_thread")]
async fn history_subscription_tracks_locally_retained_versions() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (_server, client) = backend(schema).await;
            let bob = client.for_session(session("bob", 2));
            let alice = client.for_session(session("alice", 1));
            let (row, _, tx) = bob.insert("tasks", row_input!("title" => "same")).unwrap();
            settled(&bob, tx).await;
            let query = history(row).select(["title"]);
            let mut stream = alice
                .subscribe_with_read_tier(query.clone(), ReadTier::LocalOnly)
                .await
                .unwrap();
            let initial = next(&mut stream).await;
            assert_eq!(initial.added.len(), 1);
            let first = initial.added[0].id.clone();
            assert!(first.as_occurrence().root_version().is_some());
            assert_eq!(
                initial.added[0].row.get("title"),
                Some(&Value::Text("same".into()))
            );
            assert!(initial.added[0].row.get("$transactionId").is_none());
            assert!(initial.added[0].row.get("$acceptedAt").is_none());
            let page = Query::from("history.tasks")
                .filter(eq(col("id"), lit(jazz::records::Value::Uuid(*row.uuid()))))
                .select(["title"])
                .order_by("$acceptedAt", OrderDirection::Desc)
                .limit(1);
            let mut page_stream = alice
                .subscribe_with_read_tier(page, ReadTier::LocalOnly)
                .await
                .unwrap();
            assert_eq!(next(&mut page_stream).await.added[0].id, first);
            settled(
                &bob,
                bob.update(
                    "tasks",
                    row,
                    vec![("title".into(), Value::Text("same".into()))],
                )
                .unwrap(),
            )
            .await;
            let delta = next(&mut stream).await;
            assert_eq!(delta.added.len(), 1);
            assert!(delta.removed.is_empty() && delta.updated.is_empty());
            let second = delta.added[0].id.clone();
            assert_ne!(first, second);
            assert_eq!(delta.added[0].index, 1);
            let page = next(&mut page_stream).await;
            assert_eq!(page.added.len(), 1);
            assert_eq!(page.removed.len(), 1);
            assert_eq!(page.added[0].id, second);
            assert_eq!(page.removed[0].id, first);

            let projected_titles = alice
                .query(query.clone(), ReadTier::LocalOnly)
                .await
                .unwrap();
            assert_eq!(
                projected_titles
                    .iter()
                    .map(|row| row.key.clone())
                    .collect::<Vec<_>>(),
                vec![first.clone(), second.clone()]
            );
            assert!(projected_titles.iter().all(|row| row.fields.len() == 1));

            // Both full and explicitly projected metadata decode through the public API.
            let rows = alice
                .query(history(row), ReadTier::LocalOnly)
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            for result in &rows {
                let (time, node) = result.key.as_occurrence().root_version().unwrap();
                assert_eq!(
                    result.get("$transactionId"),
                    Some(&Value::Row {
                        id: None,
                        values: vec![
                            Value::Timestamp(time.0),
                            Value::Uuid(ObjectId::from_uuid(node.0))
                        ],
                    })
                );
                assert!(
                    matches!(result.get("$acceptedAt"), Some(Value::Timestamp(seq)) if *seq > 0)
                );
            }
            let metadata_query = history(row).select(["$transactionId", "$acceptedAt"]);
            let mut metadata_stream = alice
                .subscribe_with_read_tier(metadata_query.clone(), ReadTier::LocalOnly)
                .await
                .unwrap();
            let metadata = next(&mut metadata_stream).await;
            assert_eq!(metadata.added.len(), 2);
            let projected = alice
                .query(metadata_query, ReadTier::LocalOnly)
                .await
                .unwrap();
            for (index, added) in metadata.added.iter().enumerate() {
                assert_eq!(
                    added.row.get("$transactionId"),
                    rows[index].get("$transactionId")
                );
                assert_eq!(added.row.get("$acceptedAt"), rows[index].get("$acceptedAt"));
                assert_eq!(
                    projected[index].get("$acceptedAt"),
                    rows[index].get("$acceptedAt")
                );
            }

            settled(&bob, bob.delete("tasks", row).unwrap()).await;
            let deleted = next(&mut stream).await;
            assert!(deleted.added.is_empty());
            assert_eq!(
                deleted
                    .removed
                    .iter()
                    .map(|r| r.id.clone())
                    .collect::<std::collections::BTreeSet<_>>(),
                [first.clone(), second.clone()].into()
            );
            assert!(
                alice
                    .query(query.clone(), ReadTier::LocalOnly)
                    .await
                    .unwrap()
                    .is_empty()
            );
            settled(&bob, bob.restore("tasks", row, None).unwrap()).await;
            let restored = next(&mut stream).await;
            assert!(restored.removed.is_empty());
            assert_eq!(
                restored
                    .added
                    .iter()
                    .map(|r| r.id.clone())
                    .collect::<Vec<_>>(),
                vec![first, second]
            );
            assert_eq!(
                alice.query(query, ReadTier::LocalOnly).await.unwrap().len(),
                2
            );
            client.shutdown().await.unwrap();
        })
        .await;
}

/// Alice's history follows current permissions, including a separate access row.
/// Bob changes those rows on the backend; subscriptions react without a query
/// being issued to drive delivery.
/// bob -> permission mutation -> server acceptance -> alice retracts/reveals history
#[tokio::test(flavor = "current_thread")]
async fn history_subscription_reacts_to_permission_dependencies() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use jazz::tools::{CmpOp, PolicyExpr, PolicyValue, TablePolicies};
            let grant = ObjectId::from_uuid(uuid::Uuid::from_u128(77));
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("tasks")
                        .column("title", ColumnType::Text)
                        .column("visible", ColumnType::Boolean)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::and(vec![
                                    PolicyExpr::Cmp {
                                        column: "visible".into(),
                                        op: CmpOp::Eq,
                                        value: PolicyValue::Literal(Value::Boolean(true)),
                                    },
                                    PolicyExpr::Exists {
                                        table: "access".into(),
                                        condition: Box::new(PolicyExpr::and(vec![
                                            PolicyExpr::Cmp {
                                                column: "id".into(),
                                                op: CmpOp::Eq,
                                                value: PolicyValue::Literal(Value::Uuid(grant)),
                                            },
                                            PolicyExpr::Cmp {
                                                column: "allowed".into(),
                                                op: CmpOp::Eq,
                                                value: PolicyValue::Literal(Value::Boolean(true)),
                                            },
                                        ])),
                                    },
                                ]))
                                .with_insert(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                                .with_delete(PolicyExpr::True),
                        ),
                )
                .table(
                    TableSchema::builder("access")
                        .column("allowed", ColumnType::Boolean)
                        .allow_all(),
                )
                .build();
            let (_server, bob) = backend(schema).await;
            let alice = bob.for_session(session("alice", 1));
            let (_, _, tx) = bob
                .insert_with_id("access", *grant.uuid(), row_input!("allowed" => true))
                .unwrap();
            settled(&bob, tx).await;
            let (row, _, tx) = bob
                .insert("tasks", row_input!("title" => "first", "visible" => true))
                .unwrap();
            settled(&bob, tx).await;
            let query = history(row);
            let mut stream = alice
                .subscribe_with_read_tier(query.clone(), ReadTier::LocalOnly)
                .await
                .unwrap();
            let initial = next(&mut stream).await;
            assert_eq!(initial.added.len(), 1);
            let first = initial.added[0].id.clone();
            for allowed in [false, true] {
                settled(
                    &bob,
                    bob.update(
                        "access",
                        grant,
                        vec![("allowed".into(), Value::Boolean(allowed))],
                    )
                    .unwrap(),
                )
                .await;
                let delta = next(&mut stream).await;
                if allowed {
                    assert_eq!(delta.added.len(), 1);
                    assert_eq!(delta.added[0].id, first);
                    assert!(delta.removed.is_empty());
                } else {
                    assert!(delta.added.is_empty());
                    assert_eq!(delta.removed.len(), 1);
                    assert_eq!(delta.removed[0].id, first);
                }
            }
            for visible in [false, true] {
                settled(
                    &bob,
                    bob.update(
                        "tasks",
                        row,
                        vec![("visible".into(), Value::Boolean(visible))],
                    )
                    .unwrap(),
                )
                .await;
                let delta = next(&mut stream).await;
                if visible {
                    // Current permission can reveal retained versions before
                    // the restoring mutation itself becomes accepted history.
                    let mut delta = delta;
                    while delta.added.len() < 3 {
                        let more = next(&mut stream).await;
                        assert!(more.removed.is_empty() && more.updated.is_empty());
                        delta.added.extend(more.added);
                    }
                    assert_eq!(
                        delta.added.len(),
                        3,
                        "current permission reveals every content version"
                    );
                    assert_eq!(delta.added[0].id, first);
                    assert_eq!(
                        delta.added[1].row.get("visible"),
                        Some(&Value::Boolean(false))
                    );
                    assert!(delta.removed.is_empty());
                } else {
                    assert!(delta.added.is_empty());
                    assert_eq!(delta.removed.len(), 1);
                }
                assert_eq!(
                    alice
                        .query(query.clone(), ReadTier::LocalOnly)
                        .await
                        .unwrap()
                        .len(),
                    if visible { 3 } else { 0 }
                );
            }
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Bob restores replacement content in a public transaction. Alice sees the
/// restored content and a new history version only after commit and settlement.
#[tokio::test(flavor = "current_thread")]
async fn restore_replacement_content_in_transaction() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (_server, client) = backend(schema).await;
            let bob = client.for_session(session("bob", 2));
            let alice = client.for_session(session("alice", 1));
            let (row, _, tx) = bob
                .insert("tasks", row_input!("title" => "before"))
                .unwrap();
            settled(&bob, tx).await;
            settled(&bob, bob.delete("tasks", row).unwrap()).await;
            let tx = bob.begin_transaction().unwrap();
            assert!(
                tx.client()
                    .restore(
                        "tasks",
                        row,
                        Some(vec![("title".into(), Value::Text("after".into()))])
                    )
                    .unwrap()
                    .is_none()
            );
            assert!(
                alice
                    .query(history(row), ReadTier::LocalOnly)
                    .await
                    .unwrap()
                    .is_empty()
            );
            settled(&bob, Some(tx.commit().unwrap())).await;
            let rows = alice
                .query(history(row), ReadTier::LocalOnly)
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get("title"), Some(&Value::Text("before".into())));
            assert_eq!(rows[1].get("title"), Some(&Value::Text("after".into())));
            assert!(
                bob.restore("tasks", row, None).is_err(),
                "restoring a live row must fail"
            );
            client.shutdown().await.unwrap();
        })
        .await;
}

/// Mallory cannot restore Alice's deleted task through a backend session when
/// its update policy denies her, with or without replacement content.
#[tokio::test(flavor = "current_thread")]
async fn restore_preserves_session_write_permissions() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use jazz::tools::{PolicyExpr, TablePolicies};
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("tasks")
                        .column("title", ColumnType::Text)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::True)
                                .with_insert(PolicyExpr::True)
                                .with_delete(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::False), PolicyExpr::False),
                        ),
                )
                .build();
            let (_server, client) = backend(schema).await;
            let alice = client.for_session(session("alice", 1));
            let mallory = client.for_session(session("mallory", 3));
            let (row, _, tx) = alice
                .insert("tasks", row_input!("title" => "private"))
                .unwrap();
            settled(&alice, tx).await;
            settled(&alice, alice.delete("tasks", row).unwrap()).await;
            for replacement in [
                None,
                Some(vec![("title".into(), Value::Text("changed".into()))]),
            ] {
                match mallory.restore("tasks", row, replacement) {
                    Err(error) => assert!(
                        error.to_string().to_lowercase().contains("denied"),
                        "{error}"
                    ),
                    Ok(tx) => {
                        let error = tokio::time::timeout(
                            Duration::from_secs(10),
                            mallory.wait_for_transaction(tx.unwrap(), DurabilityTier::GlobalServer),
                        )
                        .await
                        .expect("restore receives a decision")
                        .expect_err("restore policy denies Mallory");
                        assert!(
                            error.to_string().to_lowercase().contains("denied"),
                            "{error}"
                        );
                    }
                }
            }
            assert!(
                alice
                    .query(history(row), ReadTier::LocalOnly)
                    .await
                    .unwrap()
                    .is_empty()
            );
            client.shutdown().await.unwrap();
        })
        .await;
}

/// Alice counts Bob's accepted content versions, including identical values.
#[tokio::test(flavor = "current_thread")]
async fn history_count_counts_versions_not_distinct_rows() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (_server, bob) = backend(schema).await;
            let alice = bob.for_session(session("alice", 1));
            let (row, _, tx) = bob.insert("tasks", row_input!("title" => "same")).unwrap();
            settled(&bob, tx).await;
            settled(
                &bob,
                bob.update(
                    "tasks",
                    row,
                    vec![("title".into(), Value::Text("same".into()))],
                )
                .unwrap(),
            )
            .await;
            let mut query = history(row).count();
            query.order_by.clear();
            let rows = alice.query(query, ReadTier::LocalOnly).await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("count"), Some(&Value::Timestamp(2)));
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice groups Bob's historical titles after filtering, rather than grouping
/// the current title or collapsing versions with equal values.
#[tokio::test(flavor = "current_thread")]
async fn history_grouped_count_filters_version_values() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (_server, bob) = backend(schema).await;
            let alice = bob.for_session(session("alice", 1));
            let (row, _, tx) = bob.insert("tasks", row_input!("title" => "first")).unwrap();
            settled(&bob, tx).await;
            for title in ["first", "second"] {
                settled(
                    &bob,
                    bob.update(
                        "tasks",
                        row,
                        vec![("title".into(), Value::Text(title.into()))],
                    )
                    .unwrap(),
                )
                .await;
            }
            let mut query = history(row).count().group_by("title");
            query.order_by.clear();
            let rows = alice
                .query(
                    query.clone().order_by("title", OrderDirection::Asc),
                    ReadTier::LocalOnly,
                )
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get("title"), Some(&Value::Text("first".into())));
            assert_eq!(rows[0].get("count"), Some(&Value::Timestamp(2)));
            assert_eq!(rows[1].get("title"), Some(&Value::Text("second".into())));
            assert_eq!(rows[1].get("count"), Some(&Value::Timestamp(1)));
            let filtered = alice
                .query(
                    query.filter(eq(
                        col("title"),
                        lit(jazz::records::Value::String("first".into())),
                    )),
                    ReadTier::LocalOnly,
                )
                .await
                .unwrap();
            assert_eq!(filtered.len(), 1);
            assert_eq!(filtered[0].get("count"), Some(&Value::Timestamp(2)));
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice's count subscription changes when Bob adds an accepted version.
/// bob -> server acceptance -> alice's aggregate replacement
#[tokio::test(flavor = "current_thread")]
async fn history_count_subscription_tracks_new_versions() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (_server, bob) = backend(schema).await;
            let alice = bob.for_session(session("alice", 1));
            let (row, _, tx) = bob.insert("tasks", row_input!("title" => "same")).unwrap();
            settled(&bob, tx).await;
            let mut query = history(row).count();
            query.order_by.clear();
            let mut stream = alice
                .subscribe_with_read_tier(query, ReadTier::LocalOnly)
                .await
                .unwrap();
            let initial = next(&mut stream).await;
            assert_eq!(initial.added.len(), 1);
            let key = initial.added[0].id.clone();
            assert_eq!(
                initial.added[0].row.get("count"),
                Some(&Value::Timestamp(1))
            );
            settled(
                &bob,
                bob.update(
                    "tasks",
                    row,
                    vec![("title".into(), Value::Text("same".into()))],
                )
                .unwrap(),
            )
            .await;
            let delta = next(&mut stream).await;
            assert!(delta.added.is_empty() && delta.removed.is_empty());
            assert_eq!(delta.updated.len(), 1);
            assert_eq!(delta.updated[0].id, key);
            assert_eq!(
                delta.updated[0].row.as_ref().unwrap().get("count"),
                Some(&Value::Timestamp(2))
            );
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice's numeric summaries include every accepted value Bob wrote, not just
/// the current value. All functions consume the same history source.
#[tokio::test(flavor = "current_thread")]
async fn history_numeric_aggregates_summarize_version_values() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use jazz::query::Aggregate;
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("score", ColumnType::Integer))
                .allow_all()
                .build();
            let (_server, bob) = backend(schema).await;
            let alice = bob.for_session(session("alice", 1));
            let (row, _, tx) = bob.insert("tasks", row_input!("score" => 3)).unwrap();
            settled(&bob, tx).await;
            settled(
                &bob,
                bob.update("tasks", row, vec![("score".into(), Value::Integer(5))])
                    .unwrap(),
            )
            .await;
            let mut query = history(row).aggregate([
                Aggregate::sum("score"),
                Aggregate::avg("score"),
                Aggregate::min("score"),
                Aggregate::max("score"),
            ]);
            query.order_by.clear();
            let rows = alice.query(query, ReadTier::LocalOnly).await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("sum_score"), Some(&Value::Integer(8)));
            assert_eq!(rows[0].get("avg_score"), Some(&Value::Double(4.0)));
            assert_eq!(rows[0].get("min_score"), Some(&Value::Integer(3)));
            assert_eq!(rows[0].get("max_score"), Some(&Value::Integer(5)));
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice's count becomes zero when Bob deletes the current row and returns
/// when he restores it; neither mutation contributes a content version.
/// bob -> delete/restore acceptance -> alice's count replacement
#[tokio::test(flavor = "current_thread")]
async fn history_count_subscription_tracks_current_visibility() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (_server, bob) = backend(schema).await;
            let alice = bob.for_session(session("alice", 1));
            let (row, _, tx) = bob.insert("tasks", row_input!("title" => "same")).unwrap();
            settled(&bob, tx).await;
            let mut query = history(row).count();
            query.order_by.clear();
            let mut stream = alice
                .subscribe_with_read_tier(query.clone(), ReadTier::LocalOnly)
                .await
                .unwrap();
            assert_eq!(
                next(&mut stream).await.added[0].row.get("count"),
                Some(&Value::Timestamp(1))
            );
            settled(&bob, bob.delete("tasks", row).unwrap()).await;
            let deleted = next(&mut stream).await;
            assert_eq!(
                deleted.updated[0].row.as_ref().unwrap().get("count"),
                Some(&Value::Timestamp(0))
            );
            settled(&bob, bob.restore("tasks", row, None).unwrap()).await;
            let restored = next(&mut stream).await;
            assert_eq!(
                restored.updated[0].row.as_ref().unwrap().get("count"),
                Some(&Value::Timestamp(1))
            );
            assert_eq!(
                alice.query(query, ReadTier::LocalOnly).await.unwrap()[0].get("count"),
                Some(&Value::Timestamp(1))
            );
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice receives an explicit unsupported error for metadata aggregates instead
/// of a malformed aggregate result or an internal decoding failure.
#[tokio::test(flavor = "current_thread")]
async fn history_metadata_aggregates_are_explicitly_unsupported() {
    tokio::task::LocalSet::new().run_until(async {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
            .allow_all().build();
        let (_server, alice) = backend(schema).await;
        let row = ObjectId::from_uuid(uuid::Uuid::from_u128(1));
        for mut query in [history(row).min("$acceptedAt"), history(row).count().group_by("$acceptedAt")] {
            query.order_by.clear();
            let error = alice.query(query, ReadTier::LocalOnly).await.unwrap_err();
            assert_eq!(error.to_string(), "Query error: Query: unsupported relation query: Reading aggregates over history metadata is not supported in history queries yet");
        }
        alice.shutdown().await.unwrap();
    }).await;
}

/// Alice counts Bob's versions across a finite list of rows. Repeated IDs
/// must not multiply contributions, and unrelated rows must not contribute.
#[tokio::test(flavor = "current_thread")]
async fn history_count_combines_selected_rows_without_duplicate_contributions() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (_server, bob) = backend(schema).await;
            let alice = bob.for_session(session("alice", 1));
            let (a, _, tx) = bob.insert("tasks", row_input!("title" => "same")).unwrap();
            settled(&bob, tx).await;
            let (b, _, tx) = bob.insert("tasks", row_input!("title" => "same")).unwrap();
            settled(&bob, tx).await;
            let (_, _, tx) = bob
                .insert("tasks", row_input!("title" => "unrelated"))
                .unwrap();
            settled(&bob, tx).await;
            settled(
                &bob,
                bob.update(
                    "tasks",
                    a,
                    vec![("title".into(), Value::Text("same".into()))],
                )
                .unwrap(),
            )
            .await;
            let query = Query::from("history.tasks")
                .filter(jazz::query::in_list(
                    col("id"),
                    [a, b, a].map(|id| lit(jazz::records::Value::Uuid(*id.uuid()))),
                ))
                .count();
            let rows = alice
                .query(query.clone(), ReadTier::LocalOnly)
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("count"), Some(&Value::Timestamp(3)));
            let mut stream = alice
                .subscribe_with_read_tier(query, ReadTier::LocalOnly)
                .await
                .unwrap();
            assert_eq!(
                next(&mut stream).await.added[0].row.get("count"),
                Some(&Value::Timestamp(3))
            );
            settled(
                &bob,
                bob.update(
                    "tasks",
                    b,
                    vec![("title".into(), Value::Text("new".into()))],
                )
                .unwrap(),
            )
            .await;
            assert_eq!(
                next(&mut stream).await.updated[0]
                    .row
                    .as_ref()
                    .unwrap()
                    .get("count"),
                Some(&Value::Timestamp(4))
            );
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice receives the ordinary zero count for an empty set of historical rows.
#[tokio::test(flavor = "current_thread")]
async fn history_empty_id_list_has_zero_count() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (_server, alice) = backend(schema).await;
            let query = Query::from("history.tasks")
                .filter(jazz::query::in_list(col("id"), []))
                .count();
            let rows = alice
                .query(query.clone(), ReadTier::LocalOnly)
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("count"), Some(&Value::Timestamp(0)));
            let mut stream = alice
                .subscribe_with_read_tier(query, ReadTier::LocalOnly)
                .await
                .unwrap();
            assert_eq!(
                next(&mut stream).await.added[0].row.get("count"),
                Some(&Value::Timestamp(0))
            );
            alice.shutdown().await.unwrap();
        })
        .await;
}

/// Alice joins current tasks to their retained versions. Bob's identical update
/// creates a distinct joined occurrence even though its projected content agrees.
#[tokio::test(flavor = "current_thread")]
async fn history_join_on_right_preserves_versions() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (_server, alice) = backend(schema).await;
            let (row, _, tx) = alice
                .insert("tasks", row_input!("title" => "same"))
                .unwrap();
            settled(&alice, tx).await;
            let tx = alice
                .update(
                    "tasks",
                    row,
                    vec![("title".into(), Value::Text("same".into()))],
                )
                .unwrap();
            settled(&alice, tx).await;
            let query =
                Query::from("tasks").flat_join("history.tasks", "tasks.id", "history.tasks.id");
            let rows = alice.query(query, ReadTier::LocalOnly).await.unwrap();
            assert_eq!(rows.len(), 2);
            assert_ne!(rows[0].key, rows[1].key);
            assert_eq!(
                rows[0].get("history.tasks.title"),
                Some(&Value::Text("same".into()))
            );
            alice.shutdown().await.unwrap();
        })
        .await;
}

/// Alice can put history on the left: the current task IDs bound the same
/// physical history lookup, without an explicit literal ID filter.
#[tokio::test(flavor = "current_thread")]
async fn history_join_on_left_preserves_versions() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (_server, alice) = backend(schema).await;
            let (row, _, tx) = alice
                .insert("tasks", row_input!("title" => "first"))
                .unwrap();
            settled(&alice, tx).await;
            settled(
                &alice,
                alice
                    .update(
                        "tasks",
                        row,
                        vec![("title".into(), Value::Text("second".into()))],
                    )
                    .unwrap(),
            )
            .await;
            let query =
                Query::from("history.tasks").flat_join("tasks", "history.tasks.id", "tasks.id");
            let rows = alice.query(query, ReadTier::LocalOnly).await.unwrap();
            assert_eq!(rows.len(), 2);
            assert_ne!(rows[0].key, rows[1].key);
            for row in rows {
                assert_eq!(row.get("tasks.title"), Some(&Value::Text("second".into())));
            }
            alice.shutdown().await.unwrap();
        })
        .await;
}

/// Alice's subscription follows Bob's foreign-key changes and accepted
/// versions: reference -> bounded history lookup -> distinct result deltas.
#[tokio::test(flavor = "current_thread")]
async fn history_join_subscription_follows_reference_changes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .table(TableSchema::builder("links").column("task", ColumnType::Uuid))
                .allow_all()
                .build();
            let (_server, alice) = backend(schema).await;
            let (first, _, tx) = alice
                .insert("tasks", row_input!("title" => "first"))
                .unwrap();
            settled(&alice, tx).await;
            let (second, _, tx) = alice
                .insert("tasks", row_input!("title" => "second"))
                .unwrap();
            settled(&alice, tx).await;
            let (link, _, tx) = alice
                .insert("links", row_input!("task" => Value::Uuid(first)))
                .unwrap();
            settled(&alice, tx).await;
            let query =
                Query::from("links").flat_join("history.tasks", "links.task", "history.tasks.id");
            let mut stream = alice
                .subscribe_with_read_tier(query.clone(), ReadTier::LocalOnly)
                .await
                .unwrap();
            let initial = next(&mut stream).await;
            assert_eq!(initial.added.len(), 1);
            let old_key = initial.added[0].id.clone();
            settled(
                &alice,
                alice
                    .update("links", link, vec![("task".into(), Value::Uuid(second))])
                    .unwrap(),
            )
            .await;
            let changed = next(&mut stream).await;
            assert_eq!(changed.removed.len(), 1);
            assert_eq!(changed.removed[0].id, old_key);
            assert_eq!(changed.added.len(), 1);
            let second_key = changed.added[0].id.clone();
            settled(
                &alice,
                alice
                    .update(
                        "tasks",
                        second,
                        vec![("title".into(), Value::Text("second".into()))],
                    )
                    .unwrap(),
            )
            .await;
            let added = next(&mut stream).await;
            assert_eq!(added.added.len(), 1);
            assert!(added.removed.is_empty());
            assert_ne!(added.added[0].id, second_key);
            assert_eq!(
                alice.query(query, ReadTier::LocalOnly).await.unwrap().len(),
                2
            );
            drop(stream);
            alice.shutdown().await.unwrap();
        })
        .await;
}

/// Alice joins two historical occurrences of Bob's task. Each pair of content
/// versions has its own result key, even when all application values agree.
#[tokio::test(flavor = "current_thread")]
async fn history_join_two_histories_preserves_version_pairs() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (_server, alice) = backend(schema).await;
            let (row, _, tx) = alice
                .insert("tasks", row_input!("title" => "same"))
                .unwrap();
            settled(&alice, tx).await;
            settled(
                &alice,
                alice
                    .update(
                        "tasks",
                        row,
                        vec![("title".into(), Value::Text("same".into()))],
                    )
                    .unwrap(),
            )
            .await;
            let query = Query::from(jazz::query::table("history.tasks").alias("a"))
                .flat_join(
                    jazz::query::table("history.tasks").alias("b"),
                    "a.id",
                    "b.id",
                )
                .filter(eq(
                    col("a.id"),
                    lit(jazz::records::Value::Uuid(*row.uuid())),
                ));
            let rows = alice
                .query(query.clone(), ReadTier::LocalOnly)
                .await
                .unwrap();
            assert_eq!(rows.len(), 4);
            assert_eq!(
                rows.iter()
                    .map(|row| row.key.clone())
                    .collect::<std::collections::BTreeSet<_>>()
                    .len(),
                4
            );
            let mut stream = alice
                .subscribe_with_read_tier(query, ReadTier::LocalOnly)
                .await
                .unwrap();
            assert_eq!(next(&mut stream).await.added.len(), 4);
            settled(
                &alice,
                alice
                    .update(
                        "tasks",
                        row,
                        vec![("title".into(), Value::Text("same".into()))],
                    )
                    .unwrap(),
            )
            .await;
            let changed = next(&mut stream).await;
            assert_eq!(changed.added.len(), 5);
            assert!(changed.removed.is_empty());
            drop(stream);
            alice.shutdown().await.unwrap();
        })
        .await;
}

/// Alice's joined history is gated by the current task's read policy.
/// Bob hides the task, retracting every joined historical version.
#[tokio::test(flavor = "current_thread")]
async fn history_join_retracts_versions_when_current_access_is_revoked() {
    use jazz::tools::{CmpOp, PolicyExpr, PolicyValue, TablePolicies};
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("tasks")
                        .column("title", ColumnType::Text)
                        .column("visible", ColumnType::Boolean)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::Cmp {
                                    column: "visible".into(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::Literal(Value::Boolean(true)),
                                })
                                .with_insert(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                                .with_delete(PolicyExpr::True),
                        ),
                )
                .table(
                    TableSchema::builder("links")
                        .column("task", ColumnType::Uuid)
                        .allow_all(),
                )
                .build();
            let (_server, bob) = backend(schema).await;
            let alice = bob.for_session(session("alice", 1));
            let (row, _, tx) = bob
                .insert("tasks", row_input!("title" => "same", "visible" => true))
                .unwrap();
            settled(&bob, tx).await;
            settled(
                &bob,
                bob.update(
                    "tasks",
                    row,
                    vec![("title".into(), Value::Text("same".into()))],
                )
                .unwrap(),
            )
            .await;
            let (_, _, tx) = bob
                .insert("links", row_input!("task" => Value::Uuid(row)))
                .unwrap();
            settled(&bob, tx).await;
            let query =
                Query::from("links").flat_join("history.tasks", "links.task", "history.tasks.id");
            let mut stream = alice
                .subscribe_with_read_tier(query.clone(), ReadTier::LocalOnly)
                .await
                .unwrap();
            assert_eq!(next(&mut stream).await.added.len(), 2);
            settled(
                &bob,
                bob.update(
                    "tasks",
                    row,
                    vec![("visible".into(), Value::Boolean(false))],
                )
                .unwrap(),
            )
            .await;
            let revoked = next(&mut stream).await;
            assert_eq!(revoked.removed.len(), 2);
            assert!(revoked.added.is_empty());
            assert!(
                alice
                    .query(query, ReadTier::LocalOnly)
                    .await
                    .unwrap()
                    .is_empty()
            );
            drop(stream);
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice gets an explicit error when Bob's history would require an unbounded
/// scan: an application-field join alone does not supply historical row IDs.
#[tokio::test(flavor = "current_thread")]
async fn history_join_rejects_unbounded_history_reads() {
    tokio::task::LocalSet::new().run_until(async {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
            .allow_all().build();
        let (_server, alice) = backend(schema).await;
        let query = Query::from("tasks").flat_join("history.tasks", "tasks.title", "history.tasks.title");
        assert_eq!(alice.query(query, ReadTier::LocalOnly).await.unwrap_err().to_string(),
            "Query error: Query: unsupported relation query: Reading history without a bounded row access path is not supported in history queries yet");
        alice.shutdown().await.unwrap();
    }).await;
}

/// Alice reads Bob's deleted task history only while its retained current image
/// satisfies the ordinary read policy; an older visible version grants no access.
#[tokio::test]
async fn history_include_deleted_uses_current_row_permissions() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use jazz::tools::{CmpOp, PolicyExpr, PolicyValue, TablePolicies};
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("tasks")
                        .column("title", ColumnType::Text)
                        .column("visible", ColumnType::Boolean)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::Cmp {
                                    column: "visible".into(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::Literal(Value::Boolean(true)),
                                })
                                .with_insert(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                                .with_delete(PolicyExpr::True),
                        ),
                )
                .build();
            let (_server, bob) = backend(schema).await;
            let alice = bob.for_session(session("alice", 1));
            let (row, _, tx) = bob
                .insert("tasks", row_input!("title" => "first", "visible" => true))
                .unwrap();
            settled(&bob, tx).await;
            let opts = jazz::db::ReadOpts {
                tier: ReadTier::LocalOnly,
                read_view: jazz::protocol::ReadViewSpec {
                    include_deleted: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            settled(&bob, bob.delete("tasks", row).unwrap()).await;
            assert!(
                alice
                    .query(history(row), ReadTier::LocalOnly)
                    .await
                    .unwrap()
                    .is_empty()
            );
            let retained = alice
                .query_with_opts(history(row), opts.clone())
                .await
                .unwrap();
            assert_eq!(retained.len(), 1);
            settled(
                &bob,
                bob.restore(
                    "tasks",
                    row,
                    Some(vec![
                        ("title".into(), Value::Text("hidden".into())),
                        ("visible".into(), Value::Boolean(false)),
                    ]),
                )
                .unwrap(),
            )
            .await;
            settled(&bob, bob.delete("tasks", row).unwrap()).await;
            assert!(
                alice
                    .query_with_opts(history(row), opts.clone())
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                bob.query_with_opts(history(row), opts).await.unwrap().len(),
                2
            );
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Bob creates two versions before Alice connects. Alice's remote query fetches
/// both versions through normal sync, including equal projected values.
/// bob -> authority retains versions -> fresh alice -> remote history
#[tokio::test(flavor = "current_thread")]
async fn remote_history_fetches_missing_versions_on_fresh_client() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (server, bob) = backend(schema.clone()).await;
            let (row, _, tx) = bob.insert("tasks", row_input!("title" => "same")).unwrap();
            settled(&bob, tx).await;
            settled(
                &bob,
                bob.update(
                    "tasks",
                    row,
                    vec![("title".into(), Value::Text("same".into()))],
                )
                .unwrap(),
            )
            .await;
            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("alice-history")
                .ready_on("tasks", Duration::from_secs(30))
                .connect()
                .await;
            let rows = tokio::time::timeout(
                Duration::from_secs(10),
                alice.query(history(row).select(["title"]), ReadTier::Remote),
            )
            .await
            .expect("remote history settles")
            .unwrap();
            assert_eq!(rows.len(), 2);
            assert!(
                rows.iter()
                    .all(|row| row.get("title") == Some(&Value::Text("same".into())))
            );
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice receives an authoritative empty result for an ID Bob never wrote.
#[tokio::test(flavor = "current_thread")]
async fn remote_history_settles_an_empty_result() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (server, bob) = backend(schema.clone()).await;
            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("alice-empty-history")
                .ready_on("tasks", Duration::from_secs(30))
                .connect()
                .await;
            let rows = tokio::time::timeout(
                Duration::from_secs(10),
                alice.query(
                    history(ObjectId::from_uuid(uuid::Uuid::new_v4())),
                    ReadTier::Remote,
                ),
            )
            .await
            .expect("empty remote history settles")
            .unwrap();
            assert!(rows.is_empty());
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice's remote subscription fetches Bob's old versions, then follows new
/// content and deletion without collapsing versions that share the same row ID.
#[tokio::test(flavor = "current_thread")]
async fn remote_history_subscription_tracks_versions_and_deletion() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (server, bob) = backend(schema.clone()).await;
            let (row, _, tx) = bob.insert("tasks", row_input!("title" => "first")).unwrap();
            settled(&bob, tx).await;
            settled(
                &bob,
                bob.update(
                    "tasks",
                    row,
                    vec![("title".into(), Value::Text("second".into()))],
                )
                .unwrap(),
            )
            .await;
            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("alice-live-history")
                .ready_on("tasks", Duration::from_secs(30))
                .connect()
                .await;
            let mut stream = alice
                .subscribe_with_read_tier(history(row).select(["title"]), ReadTier::Remote)
                .await
                .unwrap();
            let initial = next(&mut stream).await;
            assert_eq!(initial.added.len(), 2);
            assert_ne!(initial.added[0].id, initial.added[1].id);
            settled(
                &bob,
                bob.update(
                    "tasks",
                    row,
                    vec![("title".into(), Value::Text("third".into()))],
                )
                .unwrap(),
            )
            .await;
            let added = next(&mut stream).await;
            assert_eq!(added.added.len(), 1);
            assert!(added.removed.is_empty());
            settled(&bob, bob.delete("tasks", row).unwrap()).await;
            let removed = next(&mut stream).await;
            assert_eq!(removed.removed.len(), 3);
            assert!(removed.added.is_empty());
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice joins the current task to its history; Bob's updates change the current
/// side once while retaining every historical occurrence.
/// bob -> authority -> alice's mixed remote query and subscription
#[tokio::test(flavor = "current_thread")]
async fn remote_history_joins_current_versions_of_the_same_table() {
    mixed_current_history_subscription(ReadTier::Remote).await;
}

/// Alice's local-first mixed query converges to the same version-aware join
/// as the authority, then follows Bob's current-row updates.
#[tokio::test(flavor = "current_thread")]
async fn local_first_history_joins_current_versions_of_the_same_table() {
    mixed_current_history_subscription(ReadTier::LocalFirst).await;
}

async fn mixed_current_history_subscription(tier: ReadTier) {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (server, bob) = backend(schema.clone()).await;
            let (row, _, tx) = bob.insert("tasks", row_input!("title" => "first")).unwrap();
            settled(&bob, tx).await;
            settled(
                &bob,
                bob.update(
                    "tasks",
                    row,
                    vec![("title".into(), Value::Text("second".into()))],
                )
                .unwrap(),
            )
            .await;
            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("alice-mixed-history")
                .ready_on("tasks", Duration::from_secs(30))
                .connect()
                .await;
            let query =
                Query::from("tasks").flat_join("history.tasks", "tasks.id", "history.tasks.id");
            let rows = alice.query(query.clone(), ReadTier::Remote).await.unwrap();
            assert_eq!(rows.len(), 2);
            for result in &rows {
                assert_eq!(
                    result.get("tasks.title"),
                    Some(&Value::Text("second".into()))
                );
            }
            assert_ne!(rows[0].key, rows[1].key);
            let mut stream = alice
                .subscribe_with_read_tier(query.clone(), tier)
                .await
                .unwrap();
            let initial = next(&mut stream).await;
            assert_eq!(initial.added.len(), 2);
            settled(
                &bob,
                bob.update(
                    "tasks",
                    row,
                    vec![("title".into(), Value::Text("third".into()))],
                )
                .unwrap(),
            )
            .await;
            let change = next(&mut stream).await;
            assert_eq!(change.added.len() + change.updated.len(), 3);
            let rows = alice.query(query, ReadTier::Remote).await.unwrap();
            assert_eq!(rows.len(), 3);
            for result in &rows {
                assert_eq!(
                    result.get("tasks.title"),
                    Some(&Value::Text("third".into()))
                );
            }
            // Reversing the join must still select the true current row, even
            // when the history predicate selects only the oldest content.
            let historical_root = Query::from("history.tasks")
                .flat_join("tasks", "history.tasks.id", "tasks.id")
                .filter(eq(
                    col("history.tasks.title"),
                    lit(jazz::records::Value::String("first".into())),
                ));
            let rows = alice
                .query(historical_root, ReadTier::Remote)
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].get("tasks.title"),
                Some(&Value::Text("third".into()))
            );
            settled(&bob, bob.delete("tasks", row).unwrap()).await;
            let removed = next(&mut stream).await;
            assert_eq!(removed.removed.len(), 3);
            assert!(removed.added.is_empty());
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice's local-first history follows a delete and restore-only write from Bob,
/// recovering the same immutable versions through normal upstream sync.
#[tokio::test(flavor = "current_thread")]
async fn local_first_history_subscription_restores_versions() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (server, bob) = backend(schema.clone()).await;
            let (row, _, tx) = bob.insert("tasks", row_input!("title" => "first")).unwrap();
            settled(&bob, tx).await;
            settled(
                &bob,
                bob.update(
                    "tasks",
                    row,
                    vec![("title".into(), Value::Text("second".into()))],
                )
                .unwrap(),
            )
            .await;
            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("alice-local-first-history")
                .ready_on("tasks", Duration::from_secs(30))
                .connect()
                .await;
            // Retain both versions before testing local-first visibility.
            assert_eq!(
                alice
                    .query(history(row), ReadTier::Remote)
                    .await
                    .unwrap()
                    .len(),
                2
            );
            // A cached opening does not mean the upstream subscription is ready.
            // Wait for its first answer before Bob starts the live mutations.
            let mut stream = alice
                .subscribe_local_first(history(row).select(["title"]), Duration::from_secs(10))
                .await
                .unwrap();
            let initial = next(&mut stream).await;
            assert!(
                !initial.pending,
                "upstream subscription is ready before deletion"
            );
            assert_eq!(initial.added.len(), 2);
            assert_ne!(initial.added[0].id, initial.added[1].id);
            settled(&bob, bob.delete("tasks", row).unwrap()).await;
            let removed = next(&mut stream).await;
            assert_eq!(removed.removed.len(), 2);
            assert!(removed.added.is_empty());
            settled(&bob, bob.restore("tasks", row, None).unwrap()).await;
            let restored = next(&mut stream).await;
            assert_eq!(restored.added.len(), 2);
            assert!(restored.removed.is_empty());
            assert_eq!(
                restored.added.iter().map(|row| &row.id).collect::<Vec<_>>(),
                initial.added.iter().map(|row| &row.id).collect::<Vec<_>>()
            );
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice's remote subscription receives Bob's deleted row content through the
/// normal server protocol; the public Rust API retains it with include_deleted.
/// bob -> delete -> server -> fresh alice subscription
#[tokio::test(flavor = "current_thread")]
async fn include_deleted_subscription_fetches_current_content() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = SchemaBuilder::new()
                .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
                .allow_all()
                .build();
            let (server, bob) = backend(schema.clone()).await;
            let (row, _, tx) = bob
                .insert("tasks", row_input!("title" => "retained"))
                .unwrap();
            settled(&bob, tx).await;
            settled(&bob, bob.delete("tasks", row).unwrap()).await;
            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("alice-deleted")
                .ready_on("tasks", Duration::from_secs(30))
                .connect()
                .await;
            let query = Query::from("tasks")
                .filter(eq(col("id"), lit(jazz::records::Value::Uuid(*row.uuid()))))
                .select(["title"]);
            let mut stream = alice
                .subscribe_with_opts(
                    query,
                    jazz::db::ReadOpts {
                        tier: jazz::db::ReadTier::Remote,
                        read_view: jazz::protocol::ReadViewSpec {
                            include_deleted: true,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let added = next(&mut stream).await.added;
            assert_eq!(added.len(), 1);
            assert_eq!(
                added[0].row.get("title"),
                Some(&Value::Text("retained".into()))
            );
            drop(stream);
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice receives only Bob's readable deleted task; retaining version metadata
/// must not let the unreadable task bypass the server's read policy.
/// bob -> delete visible and hidden tasks -> server policy -> alice
#[tokio::test(flavor = "current_thread")]
async fn include_deleted_subscription_respects_read_policy() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use jazz::tools::{CmpOp, PolicyExpr, PolicyValue, TablePolicies};
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("tasks")
                        .column("title", ColumnType::Text)
                        .column("visible", ColumnType::Boolean)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::Cmp {
                                    column: "visible".into(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::Literal(Value::Boolean(true)),
                                })
                                .with_insert(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                                .with_delete(PolicyExpr::True),
                        ),
                )
                .build();
            let (server, bob) = backend(schema.clone()).await;
            for (title, visible) in [("readable", true), ("hidden", false)] {
                let (row, _, tx) = bob
                    .insert("tasks", row_input!("title" => title, "visible" => visible))
                    .unwrap();
                settled(&bob, tx).await;
                settled(&bob, bob.delete("tasks", row).unwrap()).await;
            }
            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("alice-deleted-policy")
                .ready_on("tasks", Duration::from_secs(30))
                .connect()
                .await;
            let mut stream = alice
                .subscribe_with_opts(
                    Query::from("tasks").select(["title"]),
                    jazz::db::ReadOpts {
                        tier: ReadTier::Remote,
                        read_view: jazz::protocol::ReadViewSpec {
                            include_deleted: true,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let added = next(&mut stream).await.added;
            assert_eq!(added.len(), 1);
            assert_eq!(
                added[0].row.get("title"),
                Some(&Value::Text("readable".into()))
            );
            drop(stream);
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice reads Bob's deleted task through a separate access row. Revoking that
/// row, and later deleting it, retracts the deleted task from her remote
/// include-deleted subscription: only the protected root is widened to deleted
/// rows, so a deleted grant grants nothing.
/// bob -> revoke / re-grant / delete access -> server policy -> alice
#[tokio::test(flavor = "current_thread")]
async fn include_deleted_subscription_follows_access_row_revocation_and_deletion() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use jazz::tools::{CmpOp, PolicyExpr, PolicyValue, TablePolicies};
            let grant = ObjectId::from_uuid(uuid::Uuid::from_u128(78));
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("tasks")
                        .column("title", ColumnType::Text)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::Exists {
                                    table: "access".into(),
                                    condition: Box::new(PolicyExpr::and(vec![
                                        PolicyExpr::Cmp {
                                            column: "id".into(),
                                            op: CmpOp::Eq,
                                            value: PolicyValue::Literal(Value::Uuid(grant)),
                                        },
                                        PolicyExpr::Cmp {
                                            column: "allowed".into(),
                                            op: CmpOp::Eq,
                                            value: PolicyValue::Literal(Value::Boolean(true)),
                                        },
                                    ])),
                                })
                                .with_insert(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                                .with_delete(PolicyExpr::True),
                        ),
                )
                .table(
                    TableSchema::builder("access")
                        .column("allowed", ColumnType::Boolean)
                        .allow_all(),
                )
                .build();
            let (server, bob) = backend(schema.clone()).await;
            let (_, _, tx) = bob
                .insert_with_id("access", *grant.uuid(), row_input!("allowed" => true))
                .unwrap();
            settled(&bob, tx).await;
            let (row, _, tx) = bob
                .insert("tasks", row_input!("title" => "protected"))
                .unwrap();
            settled(&bob, tx).await;
            settled(&bob, bob.delete("tasks", row).unwrap()).await;
            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("alice-deleted-access")
                .ready_on("tasks", Duration::from_secs(30))
                .connect()
                .await;
            let mut stream = alice
                .subscribe_with_opts(
                    Query::from("tasks").select(["title"]),
                    jazz::db::ReadOpts {
                        tier: ReadTier::Remote,
                        read_view: jazz::protocol::ReadViewSpec {
                            include_deleted: true,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let initial = next(&mut stream).await;
            assert_eq!(initial.added.len(), 1);
            assert_eq!(
                initial.added[0].row.get("title"),
                Some(&Value::Text("protected".into()))
            );
            let protected = initial.added[0].id.clone();

            // Revoke the grant without deleting it.
            settled(
                &bob,
                bob.update(
                    "access",
                    grant,
                    vec![("allowed".into(), Value::Boolean(false))],
                )
                .unwrap(),
            )
            .await;
            let revoked = next(&mut stream).await;
            assert!(revoked.added.is_empty());
            assert_eq!(revoked.removed.len(), 1);
            assert_eq!(revoked.removed[0].id, protected);

            settled(
                &bob,
                bob.update(
                    "access",
                    grant,
                    vec![("allowed".into(), Value::Boolean(true))],
                )
                .unwrap(),
            )
            .await;
            let granted = next(&mut stream).await;
            assert_eq!(granted.added.len(), 1);
            assert_eq!(granted.added[0].id, protected);

            // A deleted grant is not a grant, even for an include-deleted read.
            settled(&bob, bob.delete("access", grant).unwrap()).await;
            let deleted_grant = next(&mut stream).await;
            assert!(deleted_grant.added.is_empty());
            assert_eq!(deleted_grant.removed.len(), 1);
            assert_eq!(deleted_grant.removed[0].id, protected);

            drop(stream);
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
        })
        .await;
}

/// Alice reads `$deletedAt`/`$deletedBy` only for the deleted task she may
/// read. The hidden deleted task's deletion metadata stays hidden on the
/// current table, including through a deletion-time filter, and on history,
/// for remote and local-first reads and for history and current
/// subscriptions. Bob both created and deleted each task, so the deletion
/// metadata must decode to his author and a time no earlier than creation.
/// No deletion witness of the hidden task reaches alice's client: a local
/// inspection under carol's identity, which may read the hidden task, finds
/// nothing.
/// bob -> delete readable and hidden tasks -> server policy -> alice
#[tokio::test(flavor = "current_thread")]
async fn deletion_metadata_respects_read_policy() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use jazz::query::{in_list, is_null, not};
            use jazz::tools::{CmpOp, PolicyExpr, PolicyValue, TablePolicies};
            const ALICE: &str = "alice-deletion-metadata";
            const CAROL: &str = "carol-deletion-metadata";
            let schema = SchemaBuilder::new()
                .table(
                    TableSchema::builder("tasks")
                        .column("title", ColumnType::Text)
                        .column("reader", ColumnType::Text)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::Cmp {
                                    column: "reader".into(),
                                    op: CmpOp::Eq,
                                    value: PolicyValue::SessionRef(vec![
                                        "user".into(),
                                        "identity".into(),
                                        "subject".into(),
                                    ]),
                                })
                                .with_insert(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                                .with_delete(PolicyExpr::True),
                        ),
                )
                .build();
            let (server, bob) = backend(schema.clone()).await;
            let mut rows = Vec::new();
            for (title, reader) in [("readable", ALICE), ("hidden", CAROL)] {
                let (row, _, tx) = bob
                    .insert("tasks", row_input!("title" => title, "reader" => reader))
                    .unwrap();
                settled(&bob, tx).await;
                settled(&bob, bob.delete("tasks", row).unwrap()).await;
                rows.push(row);
            }
            let (readable, hidden) = (rows[0], rows[1]);
            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id(ALICE)
                .ready_on("tasks", Duration::from_secs(30))
                .connect()
                .await;
            let opts = |tier| jazz::db::ReadOpts {
                tier,
                read_view: jazz::protocol::ReadViewSpec {
                    include_deleted: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            let ids = || {
                in_list(
                    col("id"),
                    rows.iter()
                        .map(|row| lit(jazz::records::Value::Uuid(*row.uuid()))),
                )
            };
            let columns = [
                "title",
                "$createdBy",
                "$createdAt",
                "$deletedAt",
                "$deletedBy",
            ];
            let current = Query::from("tasks")
                .filter(not(is_null(col("$deletedAt"))))
                .select(columns);
            let history = Query::from("history.tasks").filter(ids()).select(columns);
            fn assert_readable_deletion(get: impl Fn(&str) -> Option<Value>, what: &str) {
                assert_eq!(get("title"), Some(Value::Text("readable".into())), "{what}");
                let created_by = get("$createdBy").expect("selected $createdBy");
                assert_ne!(created_by, Value::Null, "{what}");
                assert_eq!(
                    get("$deletedBy"),
                    Some(created_by),
                    "{what}: bob created and deleted the task"
                );
                let Some(Value::Timestamp(created_at)) = get("$createdAt") else {
                    panic!("{what}: $createdAt is a timestamp: {:?}", get("$createdAt"));
                };
                let Some(Value::Timestamp(deleted_at)) = get("$deletedAt") else {
                    panic!("{what}: $deletedAt is a timestamp: {:?}", get("$deletedAt"));
                };
                assert!(deleted_at >= created_at, "{what}");
            }
            for tier in [ReadTier::Remote, ReadTier::LocalFirst] {
                for query in [current.clone(), history.clone()] {
                    let found = alice.query_with_opts(query, opts(tier)).await.unwrap();
                    assert_eq!(found.len(), 1, "{tier:?} reveals only the readable task");
                    assert_eq!(found[0].key.row_id(), Some(readable));
                    assert_readable_deletion(
                        |name| found[0].get(name).cloned(),
                        &format!("{tier:?} read"),
                    );
                }
            }
            for query in [history, current] {
                let mut stream = alice
                    .subscribe_with_opts(query, opts(ReadTier::Remote))
                    .await
                    .unwrap();
                let added = next(&mut stream).await.added;
                assert_eq!(added.len(), 1);
                assert_eq!(added[0].id.row_id(), Some(readable));
                assert_readable_deletion(|name| added[0].row.get(name).cloned(), "subscription");
                drop(stream);
            }

            // Carol may read the hidden task, so a local inspection under her
            // identity would expose any of its witnesses alice's reads stored.
            let carol_cache = alice.for_session(Session::new(jazz_server::TEST_JWT_ISSUER, CAROL));
            for query in [
                Query::from("tasks").select(["title", "$deletedAt", "$deletedBy"]),
                Query::from("history.tasks").filter(ids()).select([
                    "title",
                    "$deletedAt",
                    "$deletedBy",
                ]),
            ] {
                let cached = carol_cache
                    .query_with_opts(query, opts(ReadTier::LocalOnly))
                    .await
                    .unwrap();
                assert!(
                    cached.iter().all(|row| row.key.row_id() != Some(hidden)),
                    "the hidden task's deletion witness never reaches alice: {cached:?}"
                );
            }
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
        })
        .await;
}
