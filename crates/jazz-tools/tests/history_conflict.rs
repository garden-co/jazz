#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::{
    AppContext, ColumnType, DurabilityTier, JazzClient, ObjectId, Query, QueryBuilder,
    SchemaBuilder, TableSchema, Value,
};
use support::{TestingClient, has_updated, wait_for_query, wait_for_subscription_update};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);

fn test_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

fn todo_values(title: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("title".to_string(), Value::Text(title.to_string())),
        ("completed".to_string(), Value::Boolean(false)),
    ])
}

/// Two clients update the same todo concurrently (no sync wait between writes).
/// Both must eventually converge to the same final title.
///
/// ```text
/// alice ──create todo──► server ◄──update same todo── bob
///          (both update title concurrently, no sync between writes)
///
///          both query → see same winner
/// ```
#[tokio::test]
async fn concurrent_updates_resolve_to_lww_winner() {
    let server = TestingServer::start().await;
    let schema = test_schema();

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-conflict")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob-conflict")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    // Alice creates a todo
    let (todo_id, _) = alice
        .create("todos", todo_values("original"))
        .await
        .expect("alice creates todo");

    // Wait for Bob to see it
    let query = QueryBuilder::new("todos").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees alice's todo",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(()),
    )
    .await;

    // Both update concurrently — tokio::spawn runs each on a separate
    // OS thread, giving true parallelism. This maximises the chance of
    // creating diverged tips (true conflict) rather than a linear chain.
    let alice = Arc::new(alice);
    let bob = Arc::new(bob);
    let alice2 = Arc::clone(&alice);
    let bob2 = Arc::clone(&bob);

    let alice_handle = tokio::spawn(async move {
        alice2
            .update(
                todo_id,
                vec![("title".to_string(), Value::Text("alice-edit".to_string()))],
            )
            .await
            .expect("alice updates title");
    });
    let bob_handle = tokio::spawn(async move {
        bob2.update(
            todo_id,
            vec![("title".to_string(), Value::Text("bob-edit".to_string()))],
        )
        .await
        .expect("bob updates title");
    });

    let (alice_res, bob_res) = tokio::join!(alice_handle, bob_handle);
    alice_res.expect("alice task panicked");
    bob_res.expect("bob task panicked");

    // Poll until both clients see the same non-"original" title (convergence).
    support::wait_for(
        QUERY_TIMEOUT,
        "alice and bob converge on same title",
        || {
            let alice = Arc::clone(&alice);
            let bob = Arc::clone(&bob);
            let query = query.clone();
            async move {
                let alice_rows = alice
                    .query(query.clone(), Some(DurabilityTier::EdgeServer))
                    .await
                    .ok()?;
                let bob_rows = bob
                    .query(query, Some(DurabilityTier::EdgeServer))
                    .await
                    .ok()?;

                if alice_rows.len() == 1 && bob_rows.len() == 1 {
                    if let (Value::Text(a_title), Value::Text(b_title)) =
                        (&alice_rows[0].1[0], &bob_rows[0].1[0])
                    {
                        if a_title != "original" && a_title == b_title {
                            return Some(());
                        }
                    }
                }
                None
            }
        },
    )
    .await;

    Arc::try_unwrap(alice)
        .unwrap_or_else(|_| panic!("alice still shared"))
        .shutdown()
        .await
        .expect("shutdown alice");
    Arc::try_unwrap(bob)
        .unwrap_or_else(|_| panic!("bob still shared"))
        .shutdown()
        .await
        .expect("shutdown bob");
    server.shutdown().await;
}

/// Two clients each create a todo concurrently. Both should eventually see 2 todos.
///
/// ```text
/// alice ──create "buy milk"──► server ◄──create "buy eggs"── bob
///
///          both query → see 2 todos
/// ```
#[tokio::test]
async fn concurrent_creates_both_survive() {
    let server = TestingServer::start().await;
    let schema = test_schema();

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-creates")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob-creates")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    // Both create concurrently
    let alice = Arc::new(alice);
    let bob = Arc::new(bob);
    let alice2 = Arc::clone(&alice);
    let bob2 = Arc::clone(&bob);
    let alice_handle = tokio::spawn(async move {
        alice2
            .create("todos", todo_values("buy milk"))
            .await
            .expect("alice creates");
    });
    let bob_handle = tokio::spawn(async move {
        bob2.create("todos", todo_values("buy eggs"))
            .await
            .expect("bob creates");
    });

    let (alice_res, bob_res) = tokio::join!(alice_handle, bob_handle);
    alice_res.expect("alice task panicked");
    bob_res.expect("bob task panicked");

    let query = QueryBuilder::new("todos").build();

    // Both should eventually see 2 todos
    wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice sees 2 todos",
        |rows| (rows.len() == 2).then_some(()),
    )
    .await;

    wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees 2 todos",
        |rows| (rows.len() == 2).then_some(()),
    )
    .await;

    Arc::try_unwrap(alice)
        .unwrap_or_else(|_| panic!("alice still shared"))
        .shutdown()
        .await
        .expect("shutdown alice");
    Arc::try_unwrap(bob)
        .unwrap_or_else(|_| panic!("bob still shared"))
        .shutdown()
        .await
        .expect("shutdown bob");
    server.shutdown().await;
}

/// Both clients fire 10 rapid updates to the same row. They must converge.
///
/// ```text
/// alice ──update ×10──► server ◄──update ×10── bob
///               (interleaved, no explicit sync waits)
///
///          both query → same final value
/// ```
#[tokio::test]
async fn rapid_concurrent_updates_converge() {
    let server = TestingServer::start().await;
    let schema = test_schema();

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-rapid")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob-rapid")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    // Alice creates, wait for Bob to see it
    let (todo_id, _) = alice
        .create("todos", todo_values("start"))
        .await
        .expect("create");

    let query = QueryBuilder::new("todos").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees todo",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(()),
    )
    .await;

    // Both fire 10 rapid updates concurrently — each pair spawned on
    // separate OS threads for true parallelism.
    let alice = Arc::new(alice);
    let bob = Arc::new(bob);

    for i in 0..10 {
        let alice2 = Arc::clone(&alice);
        let bob2 = Arc::clone(&bob);
        let alice_handle = tokio::spawn(async move {
            alice2
                .update(
                    todo_id,
                    vec![("title".to_string(), Value::Text(format!("alice-{i}")))],
                )
                .await
                .expect("alice rapid update");
        });
        let bob_handle = tokio::spawn(async move {
            bob2.update(
                todo_id,
                vec![("title".to_string(), Value::Text(format!("bob-{i}")))],
            )
            .await
            .expect("bob rapid update");
        });
        let (a_res, b_res) = tokio::join!(alice_handle, bob_handle);
        a_res.expect("alice task panicked");
        b_res.expect("bob task panicked");
    }

    // Poll until both see the same non-"start" title (convergence).
    support::wait_for(
        QUERY_TIMEOUT,
        "alice and bob converge after rapid updates",
        || {
            let alice = Arc::clone(&alice);
            let bob = Arc::clone(&bob);
            let query = query.clone();
            async move {
                let alice_rows = alice
                    .query(query.clone(), Some(DurabilityTier::EdgeServer))
                    .await
                    .ok()?;
                let bob_rows = bob
                    .query(query, Some(DurabilityTier::EdgeServer))
                    .await
                    .ok()?;

                if alice_rows.len() == 1 && bob_rows.len() == 1 {
                    if let (Value::Text(a_title), Value::Text(b_title)) =
                        (&alice_rows[0].1[0], &bob_rows[0].1[0])
                    {
                        if a_title != "start" && a_title == b_title {
                            return Some(());
                        }
                    }
                }
                None
            }
        },
    )
    .await;

    Arc::try_unwrap(alice)
        .unwrap_or_else(|_| panic!("alice still shared"))
        .shutdown()
        .await
        .expect("shutdown alice");
    Arc::try_unwrap(bob)
        .unwrap_or_else(|_| panic!("bob still shared"))
        .shutdown()
        .await
        .expect("shutdown bob");
    server.shutdown().await;
}

/// A fresh client connecting after a conflict sees the same winner as
/// the original participants.
///
/// ```text
/// alice + bob conflict on a todo ──► server
///                                       │
///               charlie connects fresh, queries
///                                       │
///                                       └──► sees same winner
/// ```
#[tokio::test]
async fn fresh_client_sees_lww_winner_after_conflict() {
    let server = TestingServer::start().await;
    let schema = test_schema();

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-fresh")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob-fresh")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    // Alice creates, Bob sees it
    let (todo_id, _) = alice
        .create("todos", todo_values("original"))
        .await
        .expect("create");

    let query = QueryBuilder::new("todos").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees todo",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(()),
    )
    .await;

    // Create conflict — tokio::spawn runs each on a separate OS thread.
    let alice = Arc::new(alice);
    let bob = Arc::new(bob);
    let alice2 = Arc::clone(&alice);
    let bob2 = Arc::clone(&bob);

    let alice_handle = tokio::spawn(async move {
        alice2
            .update(
                todo_id,
                vec![("title".to_string(), Value::Text("alice-edit".to_string()))],
            )
            .await
            .expect("alice updates");
    });
    let bob_handle = tokio::spawn(async move {
        bob2.update(
            todo_id,
            vec![("title".to_string(), Value::Text("bob-edit".to_string()))],
        )
        .await
        .expect("bob updates");
    });
    let (a_res, b_res) = tokio::join!(alice_handle, bob_handle);
    a_res.expect("alice task panicked");
    b_res.expect("bob task panicked");

    // Poll until both clients see the same non-"original" title (convergence).
    // We query both in a loop because each may temporarily see different titles
    // as commits propagate through the server.
    let converged_title = support::wait_for(
        QUERY_TIMEOUT,
        "alice and bob converge on same title",
        || {
            let alice = Arc::clone(&alice);
            let bob = Arc::clone(&bob);
            let query = query.clone();
            async move {
                let alice_rows = alice
                    .query(query.clone(), Some(DurabilityTier::EdgeServer))
                    .await
                    .ok()?;
                let bob_rows = bob
                    .query(query, Some(DurabilityTier::EdgeServer))
                    .await
                    .ok()?;

                if alice_rows.len() == 1 && bob_rows.len() == 1 {
                    if let (Value::Text(a_title), Value::Text(b_title)) =
                        (&alice_rows[0].1[0], &bob_rows[0].1[0])
                    {
                        if a_title != "original" && a_title == b_title {
                            return Some(a_title.clone());
                        }
                    }
                }
                None
            }
        },
    )
    .await;

    // Charlie connects fresh
    let charlie = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("charlie-fresh")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    // Charlie must see the same winner
    let charlie_title = wait_for_query(
        &charlie,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "charlie sees converged title",
        |rows| {
            if rows.len() == 1 {
                match &rows[0].1[0] {
                    Value::Text(t) if *t == converged_title => Some(t.clone()),
                    _ => None,
                }
            } else {
                None
            }
        },
    )
    .await;

    assert_eq!(
        charlie_title, converged_title,
        "fresh client must see same winner"
    );

    Arc::try_unwrap(alice)
        .unwrap_or_else(|_| panic!("alice still shared"))
        .shutdown()
        .await
        .expect("shutdown alice");
    Arc::try_unwrap(bob)
        .unwrap_or_else(|_| panic!("bob still shared"))
        .shutdown()
        .await
        .expect("shutdown bob");
    charlie.shutdown().await.expect("shutdown charlie");
    server.shutdown().await;
}

/// Alice subscribes, Bob updates — Alice's subscription fires with the change.
///
/// ```text
/// alice subscribes to todos
/// bob updates a todo alice created
/// alice's subscription stream → sees update delta with bob's change
/// ```
#[tokio::test]
async fn subscription_reflects_concurrent_update() {
    let server = TestingServer::start().await;
    let schema = test_schema();

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-sub")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob-sub")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    // Alice creates a todo
    let (todo_id, _) = alice
        .create("todos", todo_values("task"))
        .await
        .expect("create");

    // Wait for Bob to see it
    let query = QueryBuilder::new("todos").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees todo",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(()),
    )
    .await;

    // Alice subscribes
    let mut stream = alice.subscribe(query).await.expect("subscribe");
    let mut log = Vec::new();

    // Bob updates
    bob.update(
        todo_id,
        vec![("title".to_string(), Value::Text("bob-updated".to_string()))],
    )
    .await
    .expect("bob updates");

    // Alice's subscription should fire with the update
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "alice sees bob's update via subscription",
        |log| has_updated(log, todo_id),
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Sequential updates are non-conflicting — always resolve to the last.
///
/// ```text
/// alice: create → update "v1" → update "v2" → update "v3"
/// bob: queries → sees "v3"
/// ```
#[tokio::test]
async fn sequential_updates_preserve_latest() {
    let server = TestingServer::start().await;
    let schema = test_schema();

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-seq")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    // Alice creates and updates 3 times
    let (todo_id, _) = alice
        .create("todos", todo_values("v0"))
        .await
        .expect("create");

    for version in ["v1", "v2", "v3"] {
        alice
            .update(
                todo_id,
                vec![("title".to_string(), Value::Text(version.to_string()))],
            )
            .await
            .expect("update");
    }

    // Wait for alice to see v3 at EdgeServer
    let query = QueryBuilder::new("todos").build();
    wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice sees v3",
        |rows| (rows.len() == 1 && rows[0].1[0] == Value::Text("v3".to_string())).then_some(()),
    )
    .await;

    // Bob connects fresh, must see v3
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob-seq")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let bob_rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees v3",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == todo_id
                && rows[0].1[0] == Value::Text("v3".to_string()))
            .then_some(rows)
        },
    )
    .await;

    assert_eq!(bob_rows[0].1[0], Value::Text("v3".to_string()));

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Alice edits the title, Bob edits completed — concurrently on the same row.
/// Current LWW is whole-object: the latest commit wins ALL fields.
/// This test documents the behavior (may lose one side's field change).
///
/// ```text
/// alice ──update title──► server ◄──update completed── bob
///
///          both query → see same row (one writer's full state wins)
/// ```
#[tokio::test]
async fn concurrent_edits_on_different_fields() {
    let server = TestingServer::start().await;
    let schema = test_schema();

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-fields")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob-fields")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    // Alice creates a todo: title="task", completed=false
    let (todo_id, _) = alice
        .create("todos", todo_values("task"))
        .await
        .expect("create");

    // Bob sees it
    let query = QueryBuilder::new("todos").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees todo",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(()),
    )
    .await;

    // Concurrent edits on different fields
    let alice = Arc::new(alice);
    let bob = Arc::new(bob);
    let alice2 = Arc::clone(&alice);
    let bob2 = Arc::clone(&bob);

    // Alice updates title only
    let alice_handle = tokio::spawn(async move {
        alice2
            .update(
                todo_id,
                vec![("title".to_string(), Value::Text("alice-title".to_string()))],
            )
            .await
            .expect("alice updates title");
    });
    // Bob updates completed only
    let bob_handle = tokio::spawn(async move {
        bob2.update(
            todo_id,
            vec![("completed".to_string(), Value::Boolean(true))],
        )
        .await
        .expect("bob updates completed");
    });

    let (a_res, b_res) = tokio::join!(alice_handle, bob_handle);
    a_res.expect("alice task panicked");
    b_res.expect("bob task panicked");

    // Both must converge to the same state
    support::wait_for(QUERY_TIMEOUT, "alice and bob converge on same row", || {
        let alice = Arc::clone(&alice);
        let bob = Arc::clone(&bob);
        let query = query.clone();
        async move {
            let alice_rows = alice
                .query(query.clone(), Some(DurabilityTier::EdgeServer))
                .await
                .ok()?;
            let bob_rows = bob
                .query(query, Some(DurabilityTier::EdgeServer))
                .await
                .ok()?;

            if alice_rows.len() == 1 && bob_rows.len() == 1 {
                // Both see the same title and completed values
                if alice_rows[0].1 == bob_rows[0].1 {
                    let title = &alice_rows[0].1[0];
                    let completed = &alice_rows[0].1[1];
                    // Must have moved past the original state
                    if *title != Value::Text("task".to_string())
                        || *completed != Value::Boolean(false)
                    {
                        return Some((title.clone(), completed.clone()));
                    }
                }
            }
            None
        }
    })
    .await;

    // NOTE: With whole-object LWW, the "winner" commit overwrites all fields.
    // If alice's commit wins (higher timestamp), we get title="alice-title"
    // but completed reverts to false (alice's snapshot didn't include bob's change).
    // If bob's commit wins, we get completed=true but title reverts to "task".
    // Per-field CRDTs would preserve both changes — that's a future enhancement.

    Arc::try_unwrap(alice)
        .unwrap_or_else(|_| panic!("alice still shared"))
        .shutdown()
        .await
        .expect("shutdown alice");
    Arc::try_unwrap(bob)
        .unwrap_or_else(|_| panic!("bob still shared"))
        .shutdown()
        .await
        .expect("shutdown bob");
    server.shutdown().await;
}

struct OfflineReconnectBaseline {
    server: TestingServer,
    alice: JazzClient,
    bob: JazzClient,
    bob_ctx: AppContext,
    todo_id: ObjectId,
    query: Query,
}

async fn establish_offline_reconnect_baseline(
    alice_user_id: &str,
    bob_user_id: &str,
) -> OfflineReconnectBaseline {
    let server = TestingServer::start().await;
    let schema = test_schema();

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(alice_user_id)
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let (bob_ctx, bob) = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id(bob_user_id)
        .with_persistent_storage()
        .ready_on("todos", READY_TIMEOUT)
        .connect_with_context()
        .await;

    let (todo_id, _) = alice
        .create("todos", todo_values("create"))
        .await
        .expect("alice creates todo");

    let query = QueryBuilder::new("todos").build();

    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees created todo before alice-v1",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == todo_id
                && rows[0].1[0] == Value::Text("create".to_string()))
            .then_some(())
        },
    )
    .await;

    alice
        .update(
            todo_id,
            vec![("title".to_string(), Value::Text("alice-v1".to_string()))],
        )
        .await
        .expect("alice updates to v1");

    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees alice-v1",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == todo_id
                && rows[0].1[0] == Value::Text("alice-v1".to_string()))
            .then_some(())
        },
    )
    .await;

    OfflineReconnectBaseline {
        server,
        alice,
        bob,
        bob_ctx,
        todo_id,
        query,
    }
}

/// Baseline for offline reconnect coverage: before any offline divergence,
/// a persistent peer must first hydrate and persist the online `alice-v1`
/// state that later reconnect tests rely on.
///
/// ```text
/// alice (online): create → alice-v1
/// bob   (online):           syncs to alice-v1
/// bob.shutdown()
/// bob   (offline reopen):   queries local Fjall/OPFS and still sees alice-v1
/// ```
#[tokio::test]
async fn persistent_peer_reloads_synced_state_before_offline_editing() {
    let OfflineReconnectBaseline {
        server,
        alice,
        bob,
        mut bob_ctx,
        query,
        ..
    } = establish_offline_reconnect_baseline("alice-offline-baseline", "bob-offline-baseline")
        .await;

    bob.shutdown().await.expect("bob shutdown for offline");

    bob_ctx.server_url = String::new();
    let bob_offline = JazzClient::connect(bob_ctx)
        .await
        .expect("bob reconnects offline");

    let bob_rows = bob_offline
        .query(query, None)
        .await
        .expect("bob offline query from persistent storage");
    assert_eq!(bob_rows.len(), 1, "bob should have 1 persisted todo");
    assert_eq!(
        bob_rows[0].1[0],
        Value::Text("alice-v1".to_string()),
        "bob's persisted local state should reflect the last synced online version"
    );

    bob_offline
        .shutdown()
        .await
        .expect("shutdown bob offline runtime");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Focused offline reconnect replay scenario.
///
/// The online baseline (`alice-v1` syncing and persisting locally) is covered
/// by `persistent_peer_reloads_synced_state_before_offline_editing`. This test
/// then verifies the real reconnect behavior: Bob can make a stale local edit
/// offline and still replay it after he rejoins the server.
///
/// ```text
/// baseline: create → alice-v1 ──► bob syncs, persists v1 locally
/// bob.shutdown()
///
/// alice (online):  v1 → alice-v2 → alice-v3 → alice-v4
/// bob   (offline): v1 → bob-offline-edit
///
/// bob reconnects online
/// both should converge to bob-offline-edit
/// ```
#[tokio::test]
async fn offline_reconnect_replays_local_edit_after_rejoin() {
    let OfflineReconnectBaseline {
        server,
        alice,
        bob,
        mut bob_ctx,
        todo_id,
        query,
    } = establish_offline_reconnect_baseline("alice-offline-replay", "bob-offline-replay").await;

    bob.shutdown().await.expect("bob shutdown for offline");

    for v in ["alice-v2", "alice-v3", "alice-v4"] {
        alice
            .update(
                todo_id,
                vec![("title".to_string(), Value::Text(v.to_string()))],
            )
            .await
            .expect("alice update while bob offline");
    }

    wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice sees v4 at edge",
        |rows| {
            (rows.len() == 1 && rows[0].1[0] == Value::Text("alice-v4".to_string())).then_some(())
        },
    )
    .await;

    bob_ctx.server_url = String::new();
    let bob_offline = JazzClient::connect(bob_ctx.clone())
        .await
        .expect("bob connects offline");

    let bob_stale = bob_offline
        .query(query.clone(), None)
        .await
        .expect("bob offline query to hydrate from Fjall");
    assert_eq!(bob_stale.len(), 1, "bob should have 1 todo from Fjall");
    assert_eq!(
        bob_stale[0].1[0],
        Value::Text("alice-v1".to_string()),
        "bob's Fjall state should be at alice-v1 (last synced before offline)"
    );

    bob_offline
        .update(
            todo_id,
            vec![(
                "title".to_string(),
                Value::Text("bob-offline-edit".to_string()),
            )],
        )
        .await
        .expect("bob offline edit");

    // Verify bob sees his own edit locally
    let bob_local = bob_offline
        .query(query.clone(), None)
        .await
        .expect("bob local query");
    assert_eq!(
        bob_local[0].1[0],
        Value::Text("bob-offline-edit".to_string()),
        "bob should see his offline edit locally"
    );

    bob_offline.shutdown().await.expect("bob offline shutdown");

    bob_ctx.server_url = server.base_url();
    let bob_online = JazzClient::connect(bob_ctx)
        .await
        .expect("bob reconnects online");

    let alice = Arc::new(alice);
    let bob_online = Arc::new(bob_online);

    let converged = support::wait_for(QUERY_TIMEOUT, "both converge after bob reconnects", || {
        let alice = Arc::clone(&alice);
        let bob = Arc::clone(&bob_online);
        let query = query.clone();
        async move {
            let alice_rows = alice
                .query(query.clone(), Some(DurabilityTier::EdgeServer))
                .await
                .ok()?;
            let bob_rows = bob
                .query(query, Some(DurabilityTier::EdgeServer))
                .await
                .ok()?;

            if alice_rows.len() == 1 && bob_rows.len() == 1 {
                if let (Value::Text(a), Value::Text(b)) = (&alice_rows[0].1[0], &bob_rows[0].1[0]) {
                    if a == b && a != "create" {
                        return Some(a.clone());
                    }
                }
            }
            None
        }
    })
    .await;

    assert_eq!(
        converged, "bob-offline-edit",
        "bob-offline-edit should win via LWW (bob edited last → highest timestamp)"
    );

    Arc::try_unwrap(alice)
        .unwrap_or_else(|_| panic!("alice still shared"))
        .shutdown()
        .await
        .expect("shutdown alice");
    Arc::try_unwrap(bob_online)
        .unwrap_or_else(|_| panic!("bob still shared"))
        .shutdown()
        .await
        .expect("shutdown bob");
    server.shutdown().await;
}

/// Online user (Alice) wins: her edits happen after Bob's offline edit, so
/// they carry higher timestamps and win via LWW.
///
/// Bob goes offline, makes one stale edit, then reconnects. But Alice has
/// been online making further updates — so Alice's latest commit has a higher
/// timestamp and wins. Unlike `offline_reconnect_replays_local_edit_after_rejoin`,
/// Alice's
/// commits are already on the server when Bob reconnects, so this scenario
/// does not exercise the "push offline Fjall commits" path.
///
/// ```text
///  online:   create → alice-v1 ──► bob syncs, sees v1
///                                   │
///                                   ▼ bob.shutdown() (goes offline)
///
///  bob (offline): v1 → bob-offline-edit           (earlier ts — bob edits first in code)
///  alice (online): v1 → alice-v2 → alice-v3 → alice-v4  (later ts — alice edits after bob)
///
///                                   ▼ bob reconnects to server
///
///  DAG after sync:
///     v1 → alice-v2 → alice-v3 → alice-v4   (tip, ts=highest)
///     v1 → bob-offline-edit                  (tip, ts=lower)
///
///  LWW winner: alice-v4 (higher timestamp — alice edited last)
/// ```
#[tokio::test]
async fn online_user_wins_on_reconnect() {
    let OfflineReconnectBaseline {
        server,
        alice,
        bob,
        mut bob_ctx,
        todo_id,
        query,
    } = establish_offline_reconnect_baseline("alice-alice-wins", "bob-alice-wins").await;

    // --- Phase 2: Bob goes offline. ---

    bob.shutdown().await.expect("bob shutdown for offline");

    // --- Phase 3: Bob makes 1 stale edit from v1 (earlier timestamp). ---

    bob_ctx.server_url = String::new();
    let bob_offline = JazzClient::connect(bob_ctx.clone())
        .await
        .expect("bob connects offline");

    // Hydrate from Fjall before writing
    let bob_stale = bob_offline
        .query(query.clone(), None)
        .await
        .expect("bob offline query");
    assert_eq!(bob_stale.len(), 1, "bob has 1 todo in Fjall");
    assert_eq!(
        bob_stale[0].1[0],
        Value::Text("alice-v1".to_string()),
        "bob's Fjall state is at alice-v1"
    );

    bob_offline
        .update(
            todo_id,
            vec![(
                "title".to_string(),
                Value::Text("bob-offline-edit".to_string()),
            )],
        )
        .await
        .expect("bob offline edit");

    bob_offline.shutdown().await.expect("bob offline shutdown");

    // --- Phase 4: Alice makes 3 more updates (later timestamps — alice wins). ---

    for v in ["alice-v2", "alice-v3", "alice-v4"] {
        alice
            .update(
                todo_id,
                vec![("title".to_string(), Value::Text(v.to_string()))],
            )
            .await
            .expect("alice update while bob offline");
    }

    wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice sees alice-v4 at edge",
        |rows| {
            (rows.len() == 1 && rows[0].1[0] == Value::Text("alice-v4".to_string())).then_some(())
        },
    )
    .await;

    // --- Phase 5: Bob reconnects to the real server. ---

    bob_ctx.server_url = server.base_url();
    let bob_online = JazzClient::connect(bob_ctx)
        .await
        .expect("bob reconnects online");

    // --- Phase 6: Both converge on alice-v4 (alice edited last → highest ts). ---

    wait_for_query(
        &bob_online,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees alice-v4 after reconnect",
        |rows| {
            (rows.len() == 1 && rows[0].1[0] == Value::Text("alice-v4".to_string())).then_some(())
        },
    )
    .await;

    let alice_rows = alice
        .query(query, Some(DurabilityTier::EdgeServer))
        .await
        .expect("alice final query");
    assert_eq!(
        alice_rows[0].1[0],
        Value::Text("alice-v4".to_string()),
        "alice still sees alice-v4"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob_online.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
