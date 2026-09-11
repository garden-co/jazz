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
    let table = TableSchema::builder("tasks")
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
        }));
    SchemaBuilder::new().table(table).build()
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

/// A partial edge repairs readable scalar exits with an ordinary Core query.
/// Bob's successor reaches alice under her current query authorization.
/// bob -> authority -> edge -> alice: ordinary point query refreshes exit
#[tokio::test]
async fn partial_edge_revalidates_scalar_exit_with_authorized_point_query() {
    tokio::task::LocalSet::new()
        .run_until(run_readable_exit(true))
        .await;
}

fn revocation_schema(dependency: bool) -> Schema {
    if dependency {
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
    }
}

async fn run_revoked_exit(dependency: bool, relayed: bool) {
    run_revoked_exit_case(dependency, relayed, true).await;
}

async fn run_revoked_exit_case(dependency: bool, relayed: bool, changes_filter: bool) {
    run_revoked_exit_shared_case(dependency, relayed, changes_filter, false).await;
}

async fn run_revoked_exit_shared_case(
    dependency: bool,
    relayed: bool,
    changes_filter: bool,
    shared_cache: bool,
) {
    let schema = revocation_schema(dependency);
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
    // A different reader can populate this same Edge's cache with a newer
    // task. Alice's narrowed scope must not leak that shared-cache successor.
    let shared_reader = if shared_cache {
        Some(
            TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("shared-backend")
                .as_admin()
                .connect()
                .await,
        )
    } else {
        None
    };
    let _shared_subscription = if let Some(reader) = &shared_reader {
        let mut subscription = reader
            .subscribe_with_read_tier(Query::from("tasks"), ReadTier::Remote)
            .await
            .unwrap();
        wait_for_subscription_update(
            &mut subscription,
            &mut Vec::new(),
            TIMEOUT,
            "shared Edge cache receives task",
            |log| has_added_id(log, task),
        )
        .await;
        Some(subscription)
    } else {
        None
    };
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
        ("done".into(), Value::Boolean(changes_filter)),
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
    // Restoring access must clear the exact reader's denial, without requiring
    // a new client or discarding the Edge's shared cache.
    let start = log.len();
    let tx = bob.begin_transaction().unwrap().transaction_id();
    let staged = bob.with_write_context(WriteContext::default().with_transaction_id(tx));
    staged
        .update(
            task,
            vec![
                ("done".into(), Value::Boolean(false)),
                ("owner".into(), Value::Text("alice".into())),
                ("title".into(), Value::Text("readmitted".into())),
            ],
        )
        .unwrap();
    if let Some(grant) = grant {
        staged
            .update(grant, vec![("owner".into(), Value::Text("alice".into()))])
            .unwrap();
    }
    jazz_testkit::wait_for_edge_txs(&bob, &[bob.commit_transaction(tx).unwrap()]).await;
    wait_for_subscription_update(
        &mut remote,
        &mut log,
        TIMEOUT,
        "same Edge readmits task",
        |log| has_added_id(&log[start..], task),
    )
    .await;
    alice.shutdown().await.unwrap();
    if let Some(reader) = shared_reader {
        reader.shutdown().await.unwrap();
    }
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

/// Alice reconnects with a retained scalar result after bob changes its filter.
/// No upstream predecessor survives the detached subscription. The ordinary
/// query must revalidate the extra local input through a partial relay.
/// alice caches -> disconnect/drop -> bob updates -> alice subscribes/reconnects
async fn run_reconnect_scalar_query(count: usize) {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = schema();
            let authority = JazzServer::start_with_schema(schema.clone()).await;
            let relay = JazzServer::builder()
                .with_schema(schema.clone())
                .with_app_id(authority.app_id())
                .with_backend_secret(authority.backend_secret())
                .with_upstream_url(authority.base_url())
                .with_native_transport_connector(jazz_testkit::native_connector())
                .start()
                .await;
            let bob = TestingClient::builder()
                .with_server(&authority)
                .with_schema(schema.clone())
                .with_user_id("bob")
                .as_admin()
                .ready_on("tasks", TIMEOUT)
                .connect()
                .await;
            let mut tasks = Vec::new();
            let mut txs = Vec::new();
            for _ in 0..count {
                let input = row_input!("owner" => "alice", "done" => false, "title" => "before");
                let (task, _, tx) = bob.insert("tasks", input).unwrap();
                tasks.push(task);
                txs.push(tx.unwrap());
            }
            jazz_testkit::wait_for_edge_txs(&bob, &txs).await;
            let alice = TestingClient::builder()
                .with_server(&relay)
                .with_schema(schema)
                .with_user_id("alice")
                .as_user()
                .connect()
                .await;
            let mut initial = alice
                .subscribe_with_read_tier(filtered(), ReadTier::Remote)
                .await
                .unwrap();
            let mut log = Vec::new();
            wait_for_subscription_update(
                &mut initial,
                &mut log,
                TIMEOUT,
                "alice caches original row",
                |log| tasks.iter().all(|task| has_added_id(log, *task)),
            )
            .await;
            assert!(jazz::tools::test_support::disconnect_client(&alice));
            drop(initial);
            let tx = bob.begin_transaction().unwrap().transaction_id();
            let staged = bob.with_write_context(WriteContext::default().with_transaction_id(tx));
            for task in &tasks {
                staged
                    .update(
                        *task,
                        vec![
                            ("done".into(), Value::Boolean(true)),
                            ("title".into(), Value::Text("after reconnect".into())),
                        ],
                    )
                    .unwrap();
            }
            jazz_testkit::wait_for_edge_txs(&bob, &[bob.commit_transaction(tx).unwrap()]).await;
            assert_eq!(local_rows(&alice, filtered()).await.len(), count);
            let mut local = alice
                .subscribe_with_read_tier(filtered(), ReadTier::LocalFirst)
                .await
                .unwrap();
            let mut local_log = Vec::new();
            wait_for_subscription_update(
                &mut local,
                &mut local_log,
                TIMEOUT,
                "offline local-first row",
                |log| tasks.iter().all(|task| has_added_id(log, *task)),
            )
            .await;
            assert!(
                jazz::tools::test_support::reconnect_client(&alice)
                    .await
                    .unwrap()
            );
            wait_for_subscription_update(
                &mut local,
                &mut local_log,
                TIMEOUT,
                "reconnected query repairs stale local input",
                |log| tasks.iter().all(|task| has_removed(log, *task)),
            )
            .await;
            let cached = local_rows(&alice, Query::from("tasks")).await;
            assert!(
                tasks
                    .iter()
                    .all(|task| cached.iter().any(|(id, values)| id == task
                        && values.contains(&Value::Text("after reconnect".into()))))
            );
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
            relay.shutdown().await;
            authority.shutdown().await;
        })
        .await;
}

/// Alice's retained scalar row converges through a partial relay after bob's
/// offline update. alice caches -> disconnect/drop -> bob updates -> reconnect
#[tokio::test]
async fn reconnect_scalar_query_revalidates_extra_local_input_through_relay() {
    run_reconnect_scalar_query(1).await;
}

/// Alice's 65 retained roots require more than one bounded 64-ID probe batch.
/// bob changes every root while alice is offline; every cached input refreshes.
/// alice caches 65 -> offline -> bob changes 65 -> reconnect -> two batches
#[tokio::test]
async fn reconnect_scalar_reconciliation_continues_past_first_batch() {
    run_reconnect_scalar_query(65).await;
}

async fn run_reconnect_revoked_input(dependency: bool, persistent: bool) {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = revocation_schema(dependency);
            let authority = JazzServer::start_with_schema(schema.clone()).await;
            let relay = JazzServer::builder()
                .with_schema(schema.clone())
                .with_app_id(authority.app_id())
                .with_backend_secret(authority.backend_secret())
                .with_upstream_url(authority.base_url())
                .with_native_transport_connector(jazz_testkit::native_connector())
                .start()
                .await;
            let bob = TestingClient::builder()
                .with_server(&authority)
                .with_schema(schema.clone())
                .with_user_id("bob")
                .as_admin()
                .ready_on("tasks", TIMEOUT)
                .connect()
                .await;
            let edge_system = TestingClient::builder()
                .with_server(&relay)
                .with_schema(schema.clone())
                .with_user_id("edge-system")
                .as_admin()
                .connect()
                .await;
            let mut tasks = Vec::new();
            let mut grants = Vec::new();
            for _ in 0..2 {
                let mut input =
                    row_input!("owner" => "alice", "done" => false, "title" => "before");
                if dependency {
                    let (grant, _, tx) = edge_system
                        .insert("grants", row_input!("owner" => "alice"))
                        .unwrap();
                    jazz_testkit::wait_for_edge_txs(&edge_system, &[tx.unwrap()]).await;
                    grants.push(grant);
                    input.insert("grant".into(), grant.into());
                }
                let (task, _, tx) = bob.insert("tasks", input).unwrap();
                jazz_testkit::wait_for_edge_txs(&bob, &[tx.unwrap()]).await;
                tasks.push(task);
            }
            // Close the separate Edge seed writer before changing its grants
            // through Core; keep only the independent task reader below.
            edge_system.shutdown().await.unwrap();
            let edge_system = TestingClient::builder()
                .with_server(&relay)
                .with_schema(schema.clone())
                .with_user_id("edge-system-reader")
                .as_admin()
                .connect()
                .await;
            if dependency {
                let mut observed = bob
                    .subscribe_with_read_tier(Query::from("grants"), ReadTier::Remote)
                    .await
                    .unwrap();
                let mut observed_log = Vec::new();
                wait_for_subscription_update(
                    &mut observed,
                    &mut observed_log,
                    TIMEOUT,
                    "Core writer observes grants",
                    |log| grants.iter().all(|grant| has_added_id(log, *grant)),
                )
                .await;
            }
            let builder = TestingClient::builder()
                .with_server(&relay)
                .with_schema(schema.clone())
                .with_user_id("alice")
                .as_user();
            let builder = if persistent {
                builder.with_persistent_storage()
            } else {
                builder
            };
            let (context, mut alice) = builder.connect_with_context().await;
            let mut initial = alice
                .subscribe_with_read_tier(filtered(), ReadTier::Remote)
                .await
                .unwrap();
            let mut log = Vec::new();
            wait_for_subscription_update(
                &mut initial,
                &mut log,
                TIMEOUT,
                "alice caches both inputs",
                |log| tasks.iter().all(|task| has_added_id(log, *task)),
            )
            .await;
            // An independent trusted scope deliberately fills the Edge's shared
            // task cache. Its SYSTEM-authorized successors must not become Alice's
            // authority merely because the Edge still holds an old grant.
            let mut system_scope = edge_system
                .subscribe_with_read_tier(Query::from("tasks"), ReadTier::Remote)
                .await
                .unwrap();
            let mut system_log = Vec::new();
            wait_for_subscription_update(
                &mut system_scope,
                &mut system_log,
                TIMEOUT,
                "trusted scope caches tasks",
                |log| tasks.iter().all(|task| has_added_id(log, *task)),
            )
            .await;
            assert!(jazz::tools::test_support::disconnect_client(&alice));
            drop(initial);
            let tx = bob.begin_transaction().unwrap().transaction_id();
            let staged = bob.with_write_context(WriteContext::default().with_transaction_id(tx));
            let mut revoked = vec![
                ("done".into(), Value::Boolean(true)),
                ("title".into(), Value::Text("forbidden successor".into())),
            ];
            if dependency {
                staged
                    .update(grants[0], vec![("owner".into(), Value::Text("bob".into()))])
                    .unwrap();
            } else {
                revoked.push(("owner".into(), Value::Text("bob".into())));
            }
            staged.update(tasks[0], revoked).unwrap();
            staged
                .update(
                    tasks[1],
                    vec![
                        ("done".into(), Value::Boolean(true)),
                        ("title".into(), Value::Text("readable control".into())),
                    ],
                )
                .unwrap();
            jazz_testkit::wait_for_edge_txs(&bob, &[bob.commit_transaction(tx).unwrap()]).await;
            system_log.clear();
            wait_for_subscription_update(
                &mut system_scope,
                &mut system_log,
                TIMEOUT,
                "SYSTEM receives successor in Edge shared cache",
                |log| jazz_testkit::has_updated(log, tasks[0]),
            )
            .await;
            assert!(
                local_rows(&edge_system, Query::from("tasks"))
                    .await
                    .iter()
                    .any(|(id, values)| *id == tasks[0]
                        && values.contains(&Value::Text("forbidden successor".into())))
            );
            let mut local = alice
                .subscribe_with_read_tier(filtered(), ReadTier::LocalFirst)
                .await
                .unwrap();
            let mut local_log = Vec::new();
            wait_for_subscription_update(
                &mut local,
                &mut local_log,
                TIMEOUT,
                "offline retained inputs",
                |log| tasks.iter().all(|task| has_added_id(log, *task)),
            )
            .await;
            assert!(
                jazz::tools::test_support::reconnect_client(&alice)
                    .await
                    .unwrap()
            );
            // The readable control proves the automatic reconciliation query was
            // answered; checking before its delivery could miss a delayed leak.
            wait_for_subscription_update(
                &mut local,
                &mut local_log,
                TIMEOUT,
                "readable control repairs",
                |log| has_removed(log, tasks[1]),
            )
            .await;
            for reader in [
                alice.clone(),
                alice.for_session(Session::new(jazz_server::TEST_JWT_ISSUER, "bob")),
            ] {
                assert!(!local_rows(&reader, Query::from("tasks")).await.iter().any(
                    |(_, values)| values.contains(&Value::Text("forbidden successor".into()))
                ));
            }
            let cached = local_rows(&alice, Query::from("tasks")).await;
            assert!(cached.iter().any(|(id, values)| *id == tasks[1]
                && values.contains(&Value::Text("readable control".into()))));
            wait_for_subscription_update(
                &mut local,
                &mut local_log,
                TIMEOUT,
                "explicit unavailability removes revoked cached row",
                |log| has_removed(log, tasks[0]),
            )
            .await;
            assert!(
                !local_rows(&alice, Query::from("tasks"))
                    .await
                    .iter()
                    .any(|(id, _)| *id == tasks[0])
            );

            if persistent {
                drop(local);
                alice.shutdown().await.unwrap();
                alice = jazz_testkit::connect(context.clone()).await.unwrap();
                assert!(
                    !local_rows(&alice, Query::from("tasks"))
                        .await
                        .iter()
                        .any(|(id, _)| *id == tasks[0]),
                    "reopen restores scoped unavailability without another probe"
                );
                local = alice
                    .subscribe_with_read_tier(filtered(), ReadTier::LocalFirst)
                    .await
                    .unwrap();
            }
            local_log.clear();
            let tx = bob.begin_transaction().unwrap().transaction_id();
            let staged = bob.with_write_context(WriteContext::default().with_transaction_id(tx));
            if dependency {
                staged
                    .update(
                        grants[0],
                        vec![("owner".into(), Value::Text("alice".into()))],
                    )
                    .unwrap();
            }
            staged
                .update(
                    tasks[0],
                    vec![
                        ("owner".into(), Value::Text("alice".into())),
                        ("done".into(), Value::Boolean(false)),
                        ("title".into(), Value::Text("readmitted".into())),
                    ],
                )
                .unwrap();
            jazz_testkit::wait_for_edge_txs(&bob, &[bob.commit_transaction(tx).unwrap()]).await;
            wait_for_subscription_update(
                &mut local,
                &mut local_log,
                TIMEOUT,
                "authoritative inclusion clears scoped exclusion",
                |log| has_added_id(log, tasks[0]),
            )
            .await;
            assert!(
                local_rows(&alice, Query::from("tasks"))
                    .await
                    .iter()
                    .any(|(id, values)| *id == tasks[0]
                        && values.contains(&Value::Text("readmitted".into())))
            );
            edge_system.shutdown().await.unwrap();
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
            relay.shutdown().await;
            authority.shutdown().await;
        })
        .await;
}

/// Bob revokes alice while she is offline and changes a readable control too.
/// The control proves the relay's point batch completed without disclosing the
/// revoked successor. alice disconnects -> bob changes -> relay probes -> alice
#[tokio::test]
async fn reconnect_scalar_probe_withholds_same_row_revoked_successor() {
    run_reconnect_revoked_input(false, false).await;
}

/// Bob revokes a related grant while alice is offline. The relay's cached grant
/// cannot authorize the new task bytes; the same batch repairs a readable task.
/// alice offline -> bob revokes grant -> relay/Core point batch -> no disclosure
#[tokio::test]
async fn reconnect_scalar_probe_withholds_related_grant_revoked_successor() {
    run_reconnect_revoked_input(true, false).await;
}

/// A durable client retains the denied source exclusion across reopen and
/// clears it only after a new authoritative readable receipt.
#[tokio::test]
async fn unavailable_scalar_input_survives_reopen_and_readmits() {
    run_reconnect_revoked_input(false, true).await;
}

/// Changing only the permissions catalogue must retract a retained local input;
/// restoring the rule must re-admit the unchanged native row through the Edge.
#[tokio::test]
async fn scalar_input_policy_rule_change_revokes_and_readmits() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = schema();
            let authority = JazzServer::start_with_schema(schema.clone()).await;
            let relay = JazzServer::builder()
                .with_schema(schema.clone())
                .with_app_id(authority.app_id())
                .with_backend_secret(authority.backend_secret())
                .with_upstream_url(authority.base_url())
                .with_native_transport_connector(jazz_testkit::native_connector())
                .start()
                .await;
            let writer = TestingClient::builder()
                .with_server(&authority)
                .with_schema(schema.clone())
                .with_user_id("writer")
                .as_admin()
                .ready_on("tasks", TIMEOUT)
                .connect()
                .await;
            let (task, _, tx) = writer
                .insert(
                    "tasks",
                    row_input!("owner" => "alice", "done" => false, "title" => "unchanged"),
                )
                .unwrap();
            jazz_testkit::wait_for_edge_txs(&writer, &[tx.unwrap()]).await;
            let alice = TestingClient::builder()
                .with_server(&relay)
                .with_schema(schema.clone())
                .with_user_id("alice")
                .as_user()
                .connect()
                .await;
            let mut stream = alice
                .subscribe_with_read_tier(filtered(), ReadTier::LocalFirst)
                .await
                .unwrap();
            let mut log = Vec::new();
            wait_for_subscription_update(
                &mut stream,
                &mut log,
                TIMEOUT,
                "initial policy permits input",
                |log| has_added_id(log, task),
            )
            .await;
            let denied = permissions(|p| {
                p.allow_insert().always();
                p.allow_update().always();
                p.allow_delete().always();
                p.allow_read().where_(pe::eq("owner", "nobody"));
            });
            jazz_testkit::publish_permissions(
                &authority.base_url(),
                authority.app_id(),
                authority.admin_secret(),
                &schema,
                [("tasks".into(), denied)],
                None,
            )
            .await;
            wait_for_subscription_update(
                &mut stream,
                &mut log,
                TIMEOUT,
                "new policy excludes unchanged local input",
                |log| has_removed(log, task),
            )
            .await;
            assert!(
                !local_rows(&alice, Query::from("tasks"))
                    .await
                    .iter()
                    .any(|(id, _)| *id == task)
            );
            log.clear();
            let restored = schema
                .iter()
                .map(|(name, table)| (*name, table.policies.clone()))
                .collect::<Vec<_>>();
            jazz_testkit::publish_permissions(
                &authority.base_url(),
                authority.app_id(),
                authority.admin_secret(),
                &schema,
                restored,
                None,
            )
            .await;
            wait_for_subscription_update(
                &mut stream,
                &mut log,
                TIMEOUT,
                "restored policy re-admits unchanged input",
                |log| has_added_id(log, task),
            )
            .await;
            assert!(
                local_rows(&alice, Query::from("tasks"))
                    .await
                    .iter()
                    .any(|(id, values)| *id == task
                        && values.contains(&Value::Text("unchanged".into())))
            );
            alice.shutdown().await.unwrap();
            writer.shutdown().await.unwrap();
            relay.shutdown().await;
            authority.shutdown().await;
        })
        .await;
}

/// Access alone removes the row; a scalar-filter change cannot hide a stale
/// Edge authorization decision while trusted repair refreshes the task.
#[tokio::test]
async fn edge_direct_access_loss_without_filter_change_withholds_successor() {
    tokio::task::LocalSet::new()
        .run_until(run_revoked_exit_case(false, true, false))
        .await;
}

/// A changed task must not pass an old cached grant while the Edge repairs
/// inputs on its trusted Core connection.
#[tokio::test]
async fn edge_dependency_access_loss_without_filter_change_withholds_successor() {
    tokio::task::LocalSet::new()
        .run_until(run_revoked_exit_case(true, true, false))
        .await;
}

/// Another reader fills the Edge cache with a successor which Alice cannot
/// read. A task-only shared scope deliberately does not hydrate its grant.
#[tokio::test]
async fn edge_shared_cache_dependency_revocation_withholds_successor() {
    tokio::task::LocalSet::new()
        .run_until(run_revoked_exit_shared_case(true, true, false, true))
        .await;
}
