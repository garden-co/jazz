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
        .filter(eq(
            col("id"),
            lit(jazz::groove::records::Value::Uuid(*row.uuid())),
        ))
        .order_by("$seq", OrderDirection::Asc)
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
            assert!(initial.added[0].row.get("$seq").is_none());
            let page = Query::from("history.tasks")
                .filter(eq(
                    col("id"),
                    lit(jazz::groove::records::Value::Uuid(*row.uuid())),
                ))
                .select(["title"])
                .order_by("$seq", OrderDirection::Desc)
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
                assert!(matches!(result.get("$seq"), Some(Value::Timestamp(seq)) if *seq > 0));
            }
            let metadata_query = history(row).select(["$transactionId", "$seq"]);
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
                assert_eq!(added.row.get("$seq"), rows[index].get("$seq"));
                assert_eq!(projected[index].get("$seq"), rows[index].get("$seq"));
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
