# Multi-Server Edge Sync Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand `jazz-tools` edge/core integration coverage so clients connected to different nodes observe propagation and durability tier requests are respected.

**Architecture:** Keep the coverage in the existing Rust integration test binary, `crates/jazz-tools/tests/edge_server_sync.rs`. Add a small in-file cluster fixture for one core and two edges, then add focused tests for edge-origin writes, global-tier writes, core-origin writes, subscriptions, and `QuerySettled(GlobalServer)` observations. Do not change production code unless a new test exposes a real bug.

**Tech Stack:** Rust, Tokio integration tests, `TestingServer`, `TestingClient`, `JazzClient`, `SyncTracer`, in-memory server storage.

---

## File Structure

- Modify: `crates/jazz-tools/tests/edge_server_sync.rs`
  - Owns the multi-server topology tests.
  - Adds an in-file `MultiServerCluster` helper so each test can express actor placement clearly.
  - Adds tests with ASCII topology diagrams in doc comments.

- Do not modify initially: `crates/jazz-tools/tests/support/mod.rs`
  - Existing helpers already cover client setup, query polling, and subscription polling.
  - Move cluster helpers into support only if a second integration test file needs the same topology.

- Do not modify initially: production files under `crates/jazz-tools/src/`
  - These tasks are coverage expansion. If a test fails, investigate and fix production behavior with TDD rather than weakening the test.

---

### Task 1: Add A Reusable Multi-Server Fixture

**Files:**

- Modify: `crates/jazz-tools/tests/edge_server_sync.rs`

- [ ] **Step 1: Update imports**

Replace the current imports at the top of `crates/jazz-tools/tests/edge_server_sync.rs` with:

```rust
use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::sync_manager::SyncPayload;
use jazz_tools::sync_tracer::SyncTracer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use support::{
    TestingClient, has_added, wait_for_edge_query_ready, wait_for_query,
    wait_for_subscription_update,
};
```

- [ ] **Step 2: Add cluster constants**

Add these constants after `todo_schema()`:

```rust
const PEER_SECRET: &str = "cluster-peer-secret";
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(10);
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(30);
```

- [ ] **Step 3: Replace `wait_for_upstream` with the constant-backed version**

Replace the current `wait_for_upstream()` body with:

```rust
async fn wait_for_upstream(edge: &TestingServer) {
    tokio::time::timeout(
        UPSTREAM_TIMEOUT,
        edge.server_state().runtime.transport_wait_until_connected(),
    )
    .await
    .expect("edge should connect to upstream before timeout")
    .then_some(())
    .expect("edge transport should report connected");
}
```

- [ ] **Step 4: Add the cluster helper**

Add this helper below `wait_for_upstream()`:

```rust
struct MultiServerCluster {
    schema: jazz_tools::Schema,
    core: TestingServer,
    edge_us: TestingServer,
    edge_eu: TestingServer,
}

impl MultiServerCluster {
    async fn start() -> Self {
        Self::start_with_tracer(None).await
    }

    async fn start_with_tracer(tracer: Option<SyncTracer>) -> Self {
        let schema = todo_schema();
        let app_id = TestingServer::default_app_id();

        let mut core_builder = TestingServer::builder()
            .with_app_id(app_id)
            .with_schema(schema.clone())
            .with_peer_secret(PEER_SECRET);
        if let Some(tracer) = tracer.clone() {
            core_builder = core_builder.with_tracer(tracer);
        }
        let core = core_builder.start().await;

        let mut edge_us_builder = TestingServer::builder()
            .with_app_id(app_id)
            .with_schema(schema.clone())
            .with_peer_secret(PEER_SECRET)
            .with_upstream_url(core.base_url());
        if let Some(tracer) = tracer.clone() {
            edge_us_builder = edge_us_builder.with_tracer(tracer);
        }
        let edge_us = edge_us_builder.start().await;

        let mut edge_eu_builder = TestingServer::builder()
            .with_app_id(app_id)
            .with_schema(schema.clone())
            .with_peer_secret(PEER_SECRET)
            .with_upstream_url(core.base_url());
        if let Some(tracer) = tracer {
            edge_eu_builder = edge_eu_builder.with_tracer(tracer);
        }
        let edge_eu = edge_eu_builder.start().await;

        wait_for_upstream(&edge_us).await;
        wait_for_upstream(&edge_eu).await;

        Self {
            schema,
            core,
            edge_us,
            edge_eu,
        }
    }

    async fn connect_user(
        &self,
        server: &TestingServer,
        user_id: &str,
        tracer: Option<&SyncTracer>,
    ) -> JazzClient {
        let mut client = TestingClient::builder()
            .with_server(server)
            .with_schema(self.schema.clone())
            .with_user_id(user_id)
            .ready_on("todos", READY_TIMEOUT);

        if let Some(tracer) = tracer {
            client = client.with_tracer(tracer, user_id);
        }

        client.connect().await
    }

    async fn shutdown(self) {
        self.edge_eu.shutdown().await;
        self.edge_us.shutdown().await;
        self.core.shutdown().await;
    }
}
```

- [ ] **Step 5: Refactor the existing test to use the fixture**

Replace the setup in `write_through_one_edge_replica_becomes_visible_through_another_edge()` with:

```rust
let cluster = MultiServerCluster::start().await;

let alice = cluster
    .connect_user(&cluster.edge_us, "alice", None)
    .await;
let bob = cluster
    .connect_user(&cluster.edge_eu, "bob", None)
    .await;
```

Replace the client shutdown tail with:

```rust
alice.shutdown().await.expect("shutdown alice");
bob.shutdown().await.expect("shutdown bob");
cluster.shutdown().await;
```

- [ ] **Step 6: Run the existing edge sync test**

Run:

```bash
cargo test -p jazz-tools --features test --test edge_server_sync write_through_one_edge_replica_becomes_visible_through_another_edge
```

Expected: the test passes after the helper refactor.

---

### Task 2: Cover Edge-Tier Writes Across Same Edge, Core, And Peer Edge

**Files:**

- Modify: `crates/jazz-tools/tests/edge_server_sync.rs`

- [ ] **Step 1: Add the test**

Add this test below the existing edge-to-edge test:

````rust
/// Alice writes at EdgeServer durability through the US edge.
///
/// ```text
/// alice ──EdgeServer write──► edge_us ──sync──► core ──sync──► edge_eu
/// bob   ◄────same-edge read── edge_us
/// carol ◄────global read───── core
/// dave  ◄────peer-edge read── edge_eu
/// ```
#[tokio::test]
async fn edge_tier_write_propagates_from_writer_edge_to_core_and_peer_edge() {
    let cluster = MultiServerCluster::start().await;

    let alice = cluster
        .connect_user(&cluster.edge_us, "alice-edge-us", None)
        .await;
    let bob = cluster
        .connect_user(&cluster.edge_us, "bob-edge-us", None)
        .await;
    let carol = cluster
        .connect_user(&cluster.core, "carol-core", None)
        .await;
    let dave = cluster
        .connect_user(&cluster.edge_eu, "dave-edge-eu", None)
        .await;

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("edge-local then replicated".to_string()),
            )]),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("alice edge-tier create should settle on edge_us");

    let same_edge_rows = wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "bob sees alice row on the same edge",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        same_edge_rows[0].1,
        vec![Value::Text("edge-local then replicated".to_string())]
    );

    let core_rows = wait_for_query(
        &carol,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::GlobalServer),
        REPLICATION_TIMEOUT,
        "carol sees alice row after core settlement",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        core_rows[0].1,
        vec![Value::Text("edge-local then replicated".to_string())]
    );

    let peer_edge_rows = wait_for_query(
        &dave,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "dave sees alice row on the peer edge",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        peer_edge_rows[0].1,
        vec![Value::Text("edge-local then replicated".to_string())]
    );

    let writer_global_rows = wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::GlobalServer),
        REPLICATION_TIMEOUT,
        "alice can ask edge_us for the globally settled row",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        writer_global_rows[0].1,
        vec![Value::Text("edge-local then replicated".to_string())]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    carol.shutdown().await.expect("shutdown carol");
    dave.shutdown().await.expect("shutdown dave");
    cluster.shutdown().await;
}
````

- [ ] **Step 2: Run the new test**

Run:

```bash
cargo test -p jazz-tools --features test --test edge_server_sync edge_tier_write_propagates_from_writer_edge_to_core_and_peer_edge
```

Expected: the test passes. If it fails because the edge-tier write never reaches `carol` or `dave`, investigate upstream propagation rather than changing the test expectation.

---

### Task 3: Cover Global-Tier Writes Through An Edge

**Files:**

- Modify: `crates/jazz-tools/tests/edge_server_sync.rs`

- [ ] **Step 1: Add the test**

Add this test below the edge-tier propagation test:

````rust
/// Alice writes through an edge but requests GlobalServer durability.
///
/// ```text
/// alice ──GlobalServer write──► edge_us ──peer sync──► core
/// alice ◄────return only after global settlement────────
/// carol ◄────global read──────── core
/// dave  ◄────global read──────── edge_eu
/// ```
#[tokio::test]
async fn global_tier_write_through_edge_is_visible_at_global_tier_everywhere() {
    let cluster = MultiServerCluster::start().await;

    let alice = cluster
        .connect_user(&cluster.edge_us, "alice-global-writer", None)
        .await;
    let carol = cluster
        .connect_user(&cluster.core, "carol-core-reader", None)
        .await;
    let dave = cluster
        .connect_user(&cluster.edge_eu, "dave-global-reader", None)
        .await;

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("edge write with global durability".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("global-tier create through edge_us should wait for core");

    let core_rows = wait_for_query(
        &carol,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::GlobalServer),
        REPLICATION_TIMEOUT,
        "core sees global-tier write from edge_us",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        core_rows[0].1,
        vec![Value::Text("edge write with global durability".to_string())]
    );

    let writer_edge_global_rows = wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::GlobalServer),
        REPLICATION_TIMEOUT,
        "writer edge can answer GlobalServer query after settlement",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        writer_edge_global_rows[0].1,
        vec![Value::Text("edge write with global durability".to_string())]
    );

    let peer_edge_global_rows = wait_for_query(
        &dave,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::GlobalServer),
        REPLICATION_TIMEOUT,
        "peer edge can answer GlobalServer query after upstream settlement",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        peer_edge_global_rows[0].1,
        vec![Value::Text("edge write with global durability".to_string())]
    );

    alice.shutdown().await.expect("shutdown alice");
    carol.shutdown().await.expect("shutdown carol");
    dave.shutdown().await.expect("shutdown dave");
    cluster.shutdown().await;
}
````

- [ ] **Step 2: Run the new test**

Run:

```bash
cargo test -p jazz-tools --features test --test edge_server_sync global_tier_write_through_edge_is_visible_at_global_tier_everywhere
```

Expected: the test passes. If `GlobalServer` queries through either edge time out, debug query forwarding and query settlement propagation.

---

### Task 4: Cover Core-Origin Writes Reaching Subscribed Edge Clients

**Files:**

- Modify: `crates/jazz-tools/tests/edge_server_sync.rs`

- [ ] **Step 1: Add the test**

Add this test below the global-tier write test:

````rust
/// Core-origin writes should flow down to active subscribers on both edges.
///
/// ```text
/// alice subscribes on edge_us
/// bob   subscribes on edge_eu
/// carol writes on core ──sync──► edge_us ──► alice
///                      └─sync──► edge_eu ──► bob
/// ```
#[tokio::test]
async fn core_write_reaches_subscribed_clients_on_both_edges() {
    let cluster = MultiServerCluster::start().await;

    let alice = cluster
        .connect_user(&cluster.edge_us, "alice-subscriber-us", None)
        .await;
    let bob = cluster
        .connect_user(&cluster.edge_eu, "bob-subscriber-eu", None)
        .await;
    let carol = cluster
        .connect_user(&cluster.core, "carol-core-writer", None)
        .await;

    let query = QueryBuilder::new("todos").build();
    let mut alice_stream = alice.subscribe(query.clone()).await.expect("alice subscribes");
    let mut bob_stream = bob.subscribe(query.clone()).await.expect("bob subscribes");
    let mut alice_log = Vec::new();
    let mut bob_log = Vec::new();

    let (todo_id, _) = carol
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("core write to both edges".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("core global-tier create");

    wait_for_subscription_update(
        &mut alice_stream,
        &mut alice_log,
        REPLICATION_TIMEOUT,
        "alice subscription on edge_us receives core row",
        |log| has_added(log, todo_id),
    )
    .await;

    wait_for_subscription_update(
        &mut bob_stream,
        &mut bob_log,
        REPLICATION_TIMEOUT,
        "bob subscription on edge_eu receives core row",
        |log| has_added(log, todo_id),
    )
    .await;

    let alice_rows = wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "alice edge query includes core row",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![Value::Text("core write to both edges".to_string())]
    );

    let bob_rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "bob edge query includes core row",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        bob_rows[0].1,
        vec![Value::Text("core write to both edges".to_string())]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    carol.shutdown().await.expect("shutdown carol");
    cluster.shutdown().await;
}
````

- [ ] **Step 2: Run the new test**

Run:

```bash
cargo test -p jazz-tools --features test --test edge_server_sync core_write_reaches_subscribed_clients_on_both_edges
```

Expected: the test passes. If a subscription misses the add while polling queries eventually see it, debug subscription replay across the peer connection.

---

### Task 5: Assert Global Query Settlement Through An Edge

**Files:**

- Modify: `crates/jazz-tools/tests/edge_server_sync.rs`

- [ ] **Step 1: Add the test**

Add this test below the subscription test:

````rust
/// A GlobalServer query issued to an edge should settle as GlobalServer, not
/// merely as EdgeServer.
///
/// ```text
/// alice ──GlobalServer query──► edge_us ──forwards──► core
/// alice ◄─QuerySettled(GlobalServer)─────────────────
/// bob   ──GlobalServer write──► edge_eu ──sync──────► core
/// alice ◄─global-tier query result via edge_us────────
/// ```
#[tokio::test]
async fn edge_global_query_receives_global_query_settled() {
    let tracer = SyncTracer::new();
    let cluster = MultiServerCluster::start_with_tracer(Some(tracer.clone())).await;

    let alice = cluster
        .connect_user(&cluster.edge_us, "alice-global-query", Some(&tracer))
        .await;
    let bob = cluster
        .connect_user(&cluster.edge_eu, "bob-global-writer", Some(&tracer))
        .await;

    tracer.clear();

    let empty_rows = alice
        .query(
            QueryBuilder::new("todos").build(),
            Some(DurabilityTier::GlobalServer),
        )
        .await
        .expect("empty global query through edge_us should settle");
    assert!(empty_rows.is_empty());

    let (todo_id, _) = bob
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("global query settlement through edge".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("bob global-tier create through edge_eu");

    let rows = wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::GlobalServer),
        REPLICATION_TIMEOUT,
        "alice global query through edge_us sees bob row",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows[0].1,
        vec![Value::Text("global query settlement through edge".to_string())]
    );

    tracer.wait_until_settled(Duration::from_secs(10)).await;
    let alice_messages = tracer.to("alice-global-query");
    assert!(
        alice_messages.iter().any(|message| matches!(
            &message.payload,
            SyncPayload::QuerySettled { tier, .. }
                if *tier == DurabilityTier::GlobalServer
        )),
        "alice should receive QuerySettled(GlobalServer); trace:\n{}",
        tracer.dump_for("alice-global-query")
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    cluster.shutdown().await;
}
````

- [ ] **Step 2: Run the new test**

Run:

```bash
cargo test -p jazz-tools --features test --test edge_server_sync edge_global_query_receives_global_query_settled
```

Expected: the test passes. If the rows arrive but no `QuerySettled(GlobalServer)` appears for Alice, debug query settlement tier propagation instead of accepting an edge-tier settlement.

---

### Task 6: Run The Edge Sync Test Binary

**Files:**

- Test only: `crates/jazz-tools/tests/edge_server_sync.rs`

- [ ] **Step 1: Run all edge sync integration tests**

Run:

```bash
cargo test -p jazz-tools --features test --test edge_server_sync
```

Expected: all tests in the binary pass.

- [ ] **Step 2: Run the broader Rust target touched by the prior edge-server work**

Run:

```bash
cargo test -p jazz-tools --features test --lib
```

Expected: all library tests pass.

- [ ] **Step 3: Inspect the changed test file**

Run:

```bash
git diff -- crates/jazz-tools/tests/edge_server_sync.rs
```

Expected: the diff only adds the fixture and the new multi-server tests. It should not change production behavior or weaken existing assertions.

---

## Self-Review

**Spec coverage:**

- Clients connected to different nodes: covered by `alice`/`bob`/`carol`/`dave` on `edge_us`, `edge_eu`, and `core`.
- Propagation from edge to core to peer edge: covered by `edge_tier_write_propagates_from_writer_edge_to_core_and_peer_edge`.
- Global-tier writes through an edge: covered by `global_tier_write_through_edge_is_visible_at_global_tier_everywhere`.
- Core-to-edge propagation: covered by `core_write_reaches_subscribed_clients_on_both_edges`.
- Respect of tier requests: covered by `GlobalServer` write/query assertions and explicit `QuerySettled(GlobalServer)` trace assertion.

**Placeholder scan:** No `TBD`, `TODO`, or open-ended implementation steps remain in this plan.

**Type consistency:** The snippets use existing exported test APIs: `TestingServer`, `TestingClient`, `JazzClient`, `SyncTracer`, `SyncPayload`, `wait_for_query`, `wait_for_subscription_update`, and `has_added`.
