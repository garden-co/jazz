use std::time::Duration;

use jazz::db::{LocalUpdates, Propagation, ReadOpts};
use jazz::query::{Query, col, eq, lit};
use jazz::row_input;
use jazz::tools::sync::ReadTier;
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, Schema, SchemaBuilder, Session, TableSchema, Value,
    WriteContext, permissions, policy_expr as pe,
};
use jazz_server::JazzServer;
use jazz_testkit::{TestingClient, has_added_id, has_removed, wait_for_subscription_update};

const TIMEOUT: Duration = Duration::from_secs(20);

fn schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("tasks")
                .column("owner", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .column("title", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_insert().always();
                    p.allow_update().always();
                    p.allow_delete().always();
                    p.allow_read().where_(pe::eq(
                        "owner",
                        pe::session(vec!["user", "identity", "subject"]),
                    ));
                })),
        )
        .build()
}

fn filtered() -> Query {
    Query::from("tasks").filter(eq(
        col("done"),
        lit(jazz::groove::records::Value::Bool(false)),
    ))
}

async fn local_rows(client: &JazzClient, query: Query) -> Vec<(ObjectId, Vec<Value>)> {
    // Inspection must never install an unfiltered upstream subscription, which
    // would independently repair the stale row and hide the defect.
    client
        .query_with_opts(
            query,
            ReadOpts {
                tier: jazz::tx::DurabilityTier::Local,
                local_updates: LocalUpdates::Immediate,
                propagation: Propagation::LocalOnly,
                ..Default::default()
            },
        )
        .await
        .expect("inspect local cache")
}

async fn run_readable_exit(relayed: bool) {
    let schema = schema();
    let authority = JazzServer::start_with_schema(schema.clone()).await;
    let relay = if relayed {
        Some(
            JazzServer::builder()
                .with_schema(schema.clone())
                .with_app_id(authority.app_id())
                .with_backend_secret(authority.backend_secret())
                .with_upstream_url(authority.base_url())
                .with_native_transport_connector(jazz_testkit::native_connector())
                .start()
                .await,
        )
    } else {
        None
    };
    let server = relay.as_ref().unwrap_or(&authority);
    let bob = TestingClient::builder()
        .with_server(&authority)
        .with_schema(schema.clone())
        .with_user_id("bob")
        .as_admin()
        .ready_on("tasks", TIMEOUT)
        .connect()
        .await;
    let (task, _, tx) = bob
        .insert(
            "tasks",
            row_input!("owner" => "alice", "done" => false, "title" => "before"),
        )
        .unwrap();
    let (sibling, _, sibling_tx) = bob
        .insert(
            "tasks",
            row_input!("owner" => "alice", "done" => false, "title" => "sibling before"),
        )
        .unwrap();
    jazz_testkit::wait_for_edge_txs(&bob, &[tx.unwrap(), sibling_tx.unwrap()]).await;
    let alice = TestingClient::builder()
        .with_server(server)
        .with_schema(schema)
        .with_user_id("alice")
        .as_user()
        .connect()
        .await;
    let mut remote = alice
        .subscribe_with_read_tier(filtered(), ReadTier::Remote)
        .await
        .unwrap();
    let mut local = alice
        .subscribe_with_read_tier(filtered(), ReadTier::LocalFirst)
        .await
        .unwrap();
    let mut remote_log = Vec::new();
    let mut local_log = Vec::new();
    wait_for_subscription_update(
        &mut remote,
        &mut remote_log,
        TIMEOUT,
        "alice receives remote task",
        |log| has_added_id(log, task),
    )
    .await;
    wait_for_subscription_update(
        &mut local,
        &mut local_log,
        TIMEOUT,
        "alice receives local task",
        |log| has_added_id(log, task),
    )
    .await;
    assert_eq!(local_rows(&alice, filtered()).await.len(), 2);

    let tx = bob.begin_transaction().unwrap().transaction_id();
    let staged = bob.with_write_context(WriteContext::default().with_transaction_id(tx));
    staged
        .update(
            task,
            vec![
                ("done".into(), Value::Boolean(true)),
                ("title".into(), Value::Text("after".into())),
            ],
        )
        .unwrap();
    staged
        .update(
            sibling,
            vec![("title".into(), Value::Text("sibling after".into()))],
        )
        .unwrap();
    let (forbidden, _, _) = staged
        .insert(
            "tasks",
            row_input!("owner" => "bob", "done" => true, "title" => "private sibling"),
        )
        .unwrap();
    let tx = bob.commit_transaction(tx).unwrap();
    jazz_testkit::wait_for_edge_txs(&bob, &[tx]).await;
    wait_for_subscription_update(
        &mut remote,
        &mut remote_log,
        TIMEOUT,
        "remote membership retracts",
        |log| has_removed(log, task),
    )
    .await;
    wait_for_subscription_update(
        &mut local,
        &mut local_log,
        TIMEOUT,
        "local-first task exits",
        |log| has_removed(log, task),
    )
    .await;
    assert_eq!(
        local_rows(&alice, filtered())
            .await
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>(),
        vec![sibling]
    );
    let cached = local_rows(&alice, Query::from("tasks")).await;
    assert!(cached.iter().any(|(id, values)| *id == task
        && values
            == &vec![
                Value::Text("alice".into()),
                Value::Boolean(true),
                Value::Text("after".into())
            ]));
    assert!(cached.iter().any(
        |(id, values)| *id == sibling && values.contains(&Value::Text("sibling after".into()))
    ));
    // A local inspection under bob's identity can see any leaked private row;
    // alice's ordinary policy-filtered query alone would hide a wire leak.
    let bob_cache = alice.for_session(Session::new(jazz_server::TEST_JWT_ISSUER, "bob"));
    assert!(
        !local_rows(&bob_cache, Query::from("tasks"))
            .await
            .iter()
            .any(|(id, _)| *id == forbidden)
    );

    let start = local_log.len();
    let tx = bob
        .update(task, vec![("done".into(), Value::Boolean(false))])
        .unwrap()
        .unwrap();
    jazz_testkit::wait_for_edge_txs(&bob, &[tx]).await;
    wait_for_subscription_update(
        &mut local,
        &mut local_log,
        TIMEOUT,
        "task reenters",
        |log| has_added_id(&log[start..], task),
    )
    .await;
    let start = local_log.len();
    let tx = bob.delete(task).unwrap().unwrap();
    jazz_testkit::wait_for_edge_txs(&bob, &[tx]).await;
    wait_for_subscription_update(
        &mut local,
        &mut local_log,
        TIMEOUT,
        "ordinary deletion removes task",
        |log| has_removed(&log[start..], task),
    )
    .await;
    assert!(
        !local_rows(&alice, Query::from("tasks"))
            .await
            .iter()
            .any(|(id, _)| *id == task)
    );
    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
    if let Some(relay) = relay {
        relay.shutdown().await;
    }
    authority.shutdown().await;
}

/// Alice's filtered subscription must refresh a still-readable exit version.
/// Bob updates an admitted sibling and inserts a forbidden sibling in the same
/// transaction; only the admitted rows reach alice. Reentry and deletion work.
/// bob -> server -> alice: done=false -> done=true -> done=false -> deleted
#[tokio::test]
async fn readable_scalar_exit_refreshes_local_cache_without_expanding_membership() {
    tokio::task::LocalSet::new()
        .run_until(run_readable_exit(false))
        .await;
}

/// The same cache and confidentiality contract survives a trusted relay.
/// bob -> authority -> relay -> alice: scalar exit plus same-transaction siblings
#[tokio::test]
async fn readable_scalar_exit_through_relay_refreshes_local_cache() {
    tokio::task::LocalSet::new()
        .run_until(run_readable_exit(true))
        .await;
}

async fn run_revoked_exit(dependency: bool, relayed: bool) {
    let schema = if dependency {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("grants")
                    .column("owner", ColumnType::Text)
                    .policies(permissions(|p| {
                        p.allow_insert().always();
                        p.allow_update().always();
                        p.allow_read().where_(pe::eq(
                            "owner",
                            pe::session(vec!["user", "identity", "subject"]),
                        ));
                    })),
            )
            .table(
                TableSchema::builder("tasks")
                    .column("owner", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("title", ColumnType::Text)
                    .fk_column("grant", "grants")
                    .policies(permissions(|p| {
                        p.allow_insert().always();
                        p.allow_update().always();
                        p.allow_read().where_(pe::allowed_to_read("grant"));
                    })),
            )
            .build()
    } else {
        schema()
    };
    let authority = JazzServer::start_with_schema(schema.clone()).await;
    let relay = if relayed {
        Some(
            JazzServer::builder()
                .with_schema(schema.clone())
                .with_app_id(authority.app_id())
                .with_backend_secret(authority.backend_secret())
                .with_upstream_url(authority.base_url())
                .with_native_transport_connector(jazz_testkit::native_connector())
                .start()
                .await,
        )
    } else {
        None
    };
    let server = relay.as_ref().unwrap_or(&authority);
    let bob = TestingClient::builder()
        .with_server(&authority)
        .with_schema(schema.clone())
        .with_user_id("bob")
        .as_admin()
        .ready_on("tasks", TIMEOUT)
        .connect()
        .await;
    let grant = if dependency {
        let (grant, _, tx) = bob
            .insert("grants", row_input!("owner" => "alice"))
            .unwrap();
        jazz_testkit::wait_for_edge_txs(&bob, &[tx.unwrap()]).await;
        Some(grant)
    } else {
        None
    };
    let mut input =
        row_input!("owner" => "alice", "done" => false, "title" => "old authorized title");
    if let Some(grant) = grant {
        input.insert("grant".into(), grant.into());
    }
    let (task, _, tx) = bob.insert("tasks", input).unwrap();
    jazz_testkit::wait_for_edge_txs(&bob, &[tx.unwrap()]).await;
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("alice")
        .as_user()
        .connect()
        .await;
    let mut remote = alice
        .subscribe_with_read_tier(filtered(), ReadTier::Remote)
        .await
        .unwrap();
    let mut log = Vec::new();
    wait_for_subscription_update(
        &mut remote,
        &mut log,
        TIMEOUT,
        "alice receives old authorized task",
        |log| has_added_id(log, task),
    )
    .await;
    assert!(
        local_rows(&alice, Query::from("tasks"))
            .await
            .iter()
            .any(|(id, _)| *id == task)
    );
    let tx = bob.begin_transaction().unwrap().transaction_id();
    let staged = bob.with_write_context(WriteContext::default().with_transaction_id(tx));
    let mut changes = vec![
        ("done".into(), Value::Boolean(true)),
        ("title".into(), Value::Text("new forbidden title".into())),
    ];
    if let Some(grant) = grant {
        staged
            .update(grant, vec![("owner".into(), Value::Text("bob".into()))])
            .unwrap();
    } else {
        changes.push(("owner".into(), Value::Text("bob".into())));
    }
    staged.update(task, changes).unwrap();
    jazz_testkit::wait_for_edge_txs(&bob, &[bob.commit_transaction(tx).unwrap()]).await;
    wait_for_subscription_update(
        &mut remote,
        &mut log,
        TIMEOUT,
        "revoked task leaves remote scope",
        |log| has_removed(log, task),
    )
    .await;
    for reader in [
        alice.clone(),
        alice.for_session(Session::new(jazz_server::TEST_JWT_ISSUER, "bob")),
    ] {
        assert!(
            !local_rows(&reader, Query::from("tasks"))
                .await
                .iter()
                .any(|(_, values)| values.contains(&Value::Text("new forbidden title".into()))),
            "revocation must withhold the new content, not merely hide membership"
        );
    }
    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
    if let Some(relay) = relay {
        relay.shutdown().await;
    }
    authority.shutdown().await;
}

/// Bob changes the scalar filter and revokes alice's read grant atomically.
/// Alice loses remote membership but must not receive the forbidden successor.
/// bob -> server: done=true + owner=bob -> alice: removal, no new content
#[tokio::test]
async fn scalar_exit_with_simultaneous_read_revocation_withholds_successor() {
    tokio::task::LocalSet::new()
        .run_until(run_revoked_exit(false, false))
        .await;
}

/// The point probe must reevaluate a related grant in the same transaction.
/// Bob revokes alice's parent grant while changing the task out of the filter.
/// bob -> server: grant.owner=bob + task.done=true -> alice: no new task content
#[tokio::test]
async fn scalar_exit_with_simultaneous_dependency_revocation_withholds_successor() {
    tokio::task::LocalSet::new()
        .run_until(run_revoked_exit(true, false))
        .await;
}

/// Alice must not receive a revoked successor through the relay.
/// bob -> authority -> relay -> alice: done=true + owner=bob
#[tokio::test]
async fn relayed_scalar_exit_with_simultaneous_read_revocation_withholds_successor() {
    tokio::task::LocalSet::new()
        .run_until(run_revoked_exit(false, true))
        .await;
}

/// Alice's relay must not use a stale parent grant to authorize exit content.
/// bob -> authority: grant.owner=bob + task.done=true -> relay -> alice
#[tokio::test]
async fn relayed_scalar_exit_with_simultaneous_dependency_revocation_withholds_successor() {
    tokio::task::LocalSet::new()
        .run_until(run_revoked_exit(true, true))
        .await;
}
