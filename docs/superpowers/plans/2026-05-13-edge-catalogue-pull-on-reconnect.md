# Edge Catalogue Pull On Reconnect Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every edge verify catalogue freshness on reconnect and pull the full core catalogue when its digest is missing or stale.

**Architecture:** Keep the existing handshake digest exchange. On the core side, peer WebSocket registration compares the edge's `catalogue_state_hash` with the core hash and queues a full catalogue replay to that peer only on mismatch. Separately, prevent edge transports from publishing catalogue upstream and reject peer-client catalogue writes on core.

**Tech Stack:** Rust, Tokio integration tests, existing `SyncManager`, `RuntimeCore`, WebSocket `AuthHandshake`, `TestingServer`, `MemoryStorage`.

---

## File Structure

- Modify `crates/jazz-tools/src/sync_manager/mod.rs`
  - Add client registration without automatic catalogue replay.
  - Add hash-gated catalogue replay to a specific client.
- Modify `crates/jazz-tools/src/runtime_core/sync.rs`
  - Add `ensure_client_as_peer_with_catalogue_state_hash`.
  - Keep `ensure_client_as_peer` as the no-hash replaying wrapper.
- Modify `crates/jazz-tools/src/runtime_tokio.rs`
  - Expose the new peer registration method through the Tokio runtime wrapper.
- Modify `crates/jazz-tools/src/server/routes/websocket.rs`
  - Pass `AuthHandshake.catalogue_state_hash` into peer registration.
- Modify `crates/jazz-tools/src/transport_manager.rs`
  - Treat `admin_secret`, not `peer_secret`, as permission to publish catalogue upstream.
  - Add an `AuthConfig::can_publish_catalogue` regression test.
- Modify `crates/jazz-tools/src/sync_manager/inbox.rs`
  - Reject `CatalogueEntryUpdated` from peer clients.
- Modify `crates/jazz-tools/src/runtime_core/tests/schema_catalogue.rs`
  - Add focused red tests for hash-gated peer catalogue replay.
- Modify `crates/jazz-tools/src/sync_manager/tests/permissions.rs`
  - Add a red test proving peer-client catalogue writes are denied.
- Modify `crates/jazz-tools/tests/edge_server_sync.rs`
  - Add real edge/core integration coverage for fresh-edge pull and edge-published catalogue propagation through core.

---

### Task 1: Add Red Tests For Peer Catalogue Replay

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core/tests/schema_catalogue.rs`

- [ ] **Step 1: Add helper and tests**

Append this helper and these tests below `test_matching_catalogue_hash_skips_catalogue_replay_on_add_server()`:

```rust
fn catalogue_replay_to_client_count(messages: &[OutboxEntry], client_id: ClientId) -> usize {
    messages
        .iter()
        .filter(|message| {
            matches!(
                message,
                OutboxEntry {
                    destination: Destination::Client(id),
                    payload: SyncPayload::CatalogueEntryUpdated { .. },
                } if *id == client_id
            )
        })
        .count()
}

fn has_catalogue_replay_to_client(
    messages: &[OutboxEntry],
    client_id: ClientId,
    object_id: ObjectId,
) -> bool {
    messages.iter().any(|message| {
        matches!(
            message,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::CatalogueEntryUpdated { entry },
            } if *id == client_id && entry.object_id == object_id
        )
    })
}

#[test]
fn peer_with_matching_catalogue_hash_skips_catalogue_replay() {
    let schema = test_schema();
    let app_id = AppId::from_name("test-app");
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = new_test_core(schema_manager, MemoryStorage::new(), NoopScheduler);

    core.persist_schema();
    core.batched_tick();
    core.sync_sender().take();

    let catalogue_state_hash = core.schema_manager().catalogue_state_hash();
    let peer_id = ClientId::new();

    core.ensure_client_as_peer_with_catalogue_state_hash(peer_id, Some(&catalogue_state_hash));
    core.batched_tick();

    let messages = core.sync_sender().take();
    assert_eq!(
        catalogue_replay_to_client_count(&messages, peer_id),
        0,
        "peer with matching catalogue hash should not receive catalogue replay; messages: {messages:?}"
    );
}

#[test]
fn peer_without_catalogue_hash_gets_full_catalogue_replay() {
    let schema = test_schema();
    let app_id = AppId::from_name("test-app");
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = new_test_core(schema_manager, MemoryStorage::new(), NoopScheduler);

    let schema_object_id = core.persist_schema();
    core.batched_tick();
    core.sync_sender().take();

    let peer_id = ClientId::new();

    core.ensure_client_as_peer_with_catalogue_state_hash(peer_id, None);
    core.batched_tick();

    let messages = core.sync_sender().take();
    assert!(
        has_catalogue_replay_to_client(&messages, peer_id, schema_object_id),
        "peer without catalogue hash should receive the full catalogue; messages: {messages:?}"
    );
}

#[test]
fn existing_peer_with_stale_catalogue_hash_gets_full_catalogue_replay_on_reconnect() {
    let schema = test_schema();
    let app_id = AppId::from_name("test-app");
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = new_test_core(schema_manager, MemoryStorage::new(), NoopScheduler);

    let empty_catalogue_hash = core.schema_manager().catalogue_state_hash();
    let peer_id = ClientId::new();

    core.ensure_client_as_peer_with_catalogue_state_hash(peer_id, Some(&empty_catalogue_hash));
    core.batched_tick();
    let initial_messages = core.sync_sender().take();
    assert_eq!(
        catalogue_replay_to_client_count(&initial_messages, peer_id),
        0,
        "peer should not receive catalogue while its empty hash matches core"
    );

    let schema_object_id = core.persist_schema();
    core.batched_tick();
    let live_push_that_offline_peer_missed = core.sync_sender().take();
    assert!(
        has_catalogue_replay_to_client(&live_push_that_offline_peer_missed, peer_id, schema_object_id),
        "core publish should still queue live catalogue propagation to connected peer clients"
    );

    core.ensure_client_as_peer_with_catalogue_state_hash(peer_id, Some(&empty_catalogue_hash));
    core.batched_tick();

    let reconnect_messages = core.sync_sender().take();
    assert!(
        has_catalogue_replay_to_client(&reconnect_messages, peer_id, schema_object_id),
        "existing peer with stale hash should receive replay on reconnect; messages: {reconnect_messages:?}"
    );
}
```

- [ ] **Step 2: Run the focused red tests**

Run:

```bash
cargo test -p jazz-tools --features test peer_with_matching_catalogue_hash_skips_catalogue_replay
cargo test -p jazz-tools --features test peer_without_catalogue_hash_gets_full_catalogue_replay
cargo test -p jazz-tools --features test existing_peer_with_stale_catalogue_hash_gets_full_catalogue_replay_on_reconnect
```

Expected: each command fails to compile because `RuntimeCore::ensure_client_as_peer_with_catalogue_state_hash` does not exist yet.

- [ ] **Step 3: Commit the red tests**

```bash
git add crates/jazz-tools/src/runtime_core/tests/schema_catalogue.rs
git commit -m "test: cover peer catalogue replay on reconnect"
```

---

### Task 2: Implement Hash-Gated Peer Catalogue Replay

**Files:**

- Modify: `crates/jazz-tools/src/sync_manager/mod.rs`
- Modify: `crates/jazz-tools/src/runtime_core/sync.rs`

- [ ] **Step 1: Add client registration and hash-gated replay to `SyncManager`**

In `crates/jazz-tools/src/sync_manager/mod.rs`, replace `add_client_with_storage` with this block:

```rust
    /// Add a client connection without automatically replaying catalogue state.
    pub fn add_client(&mut self, client_id: ClientId) {
        self.clients.insert(client_id, ClientState::default());
    }

    /// Add a client connection using storage-backed catalogue replay.
    pub fn add_client_with_storage<H: Storage>(&mut self, storage: &H, client_id: ClientId) {
        self.add_client(client_id);
        self.queue_catalogue_sync_to_client_from_storage(client_id, storage);
    }

    /// Replay catalogue entries to a client when its digest is missing or stale.
    ///
    /// Returns true when a replay was queued.
    pub fn queue_catalogue_sync_to_client_if_hash_mismatch<H: Storage>(
        &mut self,
        storage: &H,
        client_id: ClientId,
        remote_catalogue_state_hash: Option<&str>,
        local_catalogue_state_hash: &str,
    ) -> bool {
        if remote_catalogue_state_hash == Some(local_catalogue_state_hash) {
            return false;
        }

        self.queue_catalogue_sync_to_client_from_storage(client_id, storage);
        true
    }
```

- [ ] **Step 2: Add peer registration with a catalogue hash to `RuntimeCore`**

In `crates/jazz-tools/src/runtime_core/sync.rs`, replace `ensure_client_as_peer` with:

```rust
    /// Ensure a client exists and is marked as Peer without resetting state.
    pub fn ensure_client_as_peer(&mut self, client_id: ClientId) {
        self.ensure_client_as_peer_with_catalogue_state_hash(client_id, None);
    }

    /// Ensure a peer client exists, then replay catalogue entries only when
    /// the peer's catalogue digest is missing or stale.
    pub fn ensure_client_as_peer_with_catalogue_state_hash(
        &mut self,
        client_id: ClientId,
        remote_catalogue_state_hash: Option<&str>,
    ) {
        use crate::sync_manager::ClientRole;

        let local_catalogue_state_hash = self.schema_manager.catalogue_state_hash();
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();

        if sm.get_client(client_id).is_none() {
            sm.add_client(client_id);
        }
        sm.set_client_role(client_id, ClientRole::Peer);

        let queued_catalogue_replay = sm.queue_catalogue_sync_to_client_if_hash_mismatch(
            &self.storage,
            client_id,
            remote_catalogue_state_hash,
            &local_catalogue_state_hash,
        );
        if queued_catalogue_replay {
            self.immediate_tick();
        }
    }
```

- [ ] **Step 3: Run the replay tests**

Run:

```bash
cargo test -p jazz-tools --features test peer_with_matching_catalogue_hash_skips_catalogue_replay
cargo test -p jazz-tools --features test peer_without_catalogue_hash_gets_full_catalogue_replay
cargo test -p jazz-tools --features test existing_peer_with_stale_catalogue_hash_gets_full_catalogue_replay_on_reconnect
```

Expected: all three tests pass.

- [ ] **Step 4: Run existing catalogue replay tests**

Run:

```bash
cargo test -p jazz-tools --features test test_persist_schema_then_add_server_sends_catalogue
cargo test -p jazz-tools --features test test_matching_catalogue_hash_skips_catalogue_replay_on_add_server
```

Expected: both tests pass.

- [ ] **Step 5: Commit implementation**

```bash
git add crates/jazz-tools/src/sync_manager/mod.rs crates/jazz-tools/src/runtime_core/sync.rs
git commit -m "feat: replay core catalogue to stale peer edges"
```

---

### Task 3: Wire The Handshake Hash Into Peer Registration

**Files:**

- Modify: `crates/jazz-tools/src/runtime_tokio.rs`
- Modify: `crates/jazz-tools/src/server/routes/websocket.rs`

- [ ] **Step 1: Expose the new method on `TokioRuntime`**

In `crates/jazz-tools/src/runtime_tokio.rs`, add this method below `ensure_client_as_peer`:

```rust
    /// Ensure a peer client exists and replay catalogue only when its digest is stale.
    pub fn ensure_client_as_peer_with_catalogue_state_hash(
        &self,
        client_id: ClientId,
        remote_catalogue_state_hash: Option<&str>,
    ) -> Result<(), RuntimeError> {
        let mut core = self.core.lock().map_err(|_| RuntimeError::LockError)?;
        core.ensure_client_as_peer_with_catalogue_state_hash(
            client_id,
            remote_catalogue_state_hash,
        );
        Ok(())
    }
```

- [ ] **Step 2: Pass the handshake hash for peer WebSocket clients**

In `crates/jazz-tools/src/server/routes/websocket.rs`, change the `WsClientSetup::Peer` branch from:

```rust
        WsClientSetup::Peer => {
            let _ = state.runtime.ensure_client_as_peer(client_id);
        }
```

to:

```rust
        WsClientSetup::Peer => {
            let _ = state
                .runtime
                .ensure_client_as_peer_with_catalogue_state_hash(
                    client_id,
                    handshake.catalogue_state_hash.as_deref(),
                );
        }
```

- [ ] **Step 3: Run the focused compile check**

Run:

```bash
cargo test -p jazz-tools --features test ws_handshake_accepts_valid_peer_secret_as_peer
```

Expected: the existing peer-auth test passes.

- [ ] **Step 4: Commit the wiring**

```bash
git add crates/jazz-tools/src/runtime_tokio.rs crates/jazz-tools/src/server/routes/websocket.rs
git commit -m "feat: use edge catalogue hash during peer handshake"
```

---

### Task 4: Stop Edge Transports Publishing Catalogue Upstream

**Files:**

- Modify: `crates/jazz-tools/src/transport_manager.rs`

- [ ] **Step 1: Add an auth capability regression test**

In `crates/jazz-tools/src/transport_manager.rs`, inside `mod handshake_tests`, add:

```rust
    #[test]
    fn peer_secret_does_not_grant_catalogue_publish_permission() {
        let peer_auth = AuthConfig {
            peer_secret: Some("cluster-peer-secret".to_string()),
            ..Default::default()
        };
        assert!(
            !peer_auth.can_publish_catalogue(),
            "peer-secret edge transports receive catalogue from core but must not publish catalogue upstream"
        );

        let admin_auth = AuthConfig {
            admin_secret: Some("admin-secret".to_string()),
            ..Default::default()
        };
        assert!(
            admin_auth.can_publish_catalogue(),
            "admin-secret transports keep explicit catalogue publish permission"
        );
    }
```

- [ ] **Step 2: Run the red test**

Run:

```bash
cargo test -p jazz-tools peer_secret_does_not_grant_catalogue_publish_permission
```

Expected: FAIL because `AuthConfig::can_publish_catalogue` currently returns true for `peer_secret`.

- [ ] **Step 3: Change `AuthConfig::can_publish_catalogue`**

Replace the method with:

```rust
    pub fn can_publish_catalogue(&self) -> bool {
        self.admin_secret.is_some()
    }
```

- [ ] **Step 4: Run the test**

Run:

```bash
cargo test -p jazz-tools peer_secret_does_not_grant_catalogue_publish_permission
```

Expected: PASS.

- [ ] **Step 5: Run the upstream catalogue skip regression**

Run:

```bash
cargo test -p jazz-tools --features test test_persist_schema_then_unprivileged_add_server_skips_catalogue
```

Expected: PASS.

- [ ] **Step 6: Commit the transport authority change**

```bash
git add crates/jazz-tools/src/transport_manager.rs
git commit -m "fix: keep peer transports from publishing catalogue"
```

---

### Task 5: Reject Peer-Client Catalogue Writes On Core

**Files:**

- Modify: `crates/jazz-tools/src/sync_manager/tests/permissions.rs`
- Modify: `crates/jazz-tools/src/sync_manager/inbox.rs`

- [ ] **Step 1: Add the red permission test**

Append this test to `crates/jazz-tools/src/sync_manager/tests/permissions.rs`:

```rust
#[test]
fn catalogue_update_from_peer_client_is_denied() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::GlobalServer);
    let mut io = MemoryStorage::new();
    let peer_id = ClientId::new();
    let catalogue_object_id = ObjectId::new();

    add_client(&mut sm, &io, peer_id);
    sm.set_client_role(peer_id, ClientRole::Peer);
    sm.take_outbox();

    let entry = CatalogueEntry {
        object_id: catalogue_object_id,
        metadata: HashMap::from([(
            crate::metadata::MetadataKey::Type.to_string(),
            crate::metadata::ObjectType::CatalogueSchema.to_string(),
        )]),
        content: b"edge-owned-catalogue-entry".to_vec(),
    };

    sm.push_inbox(InboxEntry {
        source: Source::Client(peer_id),
        payload: SyncPayload::CatalogueEntryUpdated {
            entry: entry.clone(),
        },
    });
    sm.process_inbox(&mut io);

    assert!(
        io.load_catalogue_entry(catalogue_object_id)
            .expect("catalogue lookup should succeed")
            .is_none(),
        "core must not persist catalogue entries sent by peer clients"
    );
    assert!(
        sm.take_pending_catalogue_updates().is_empty(),
        "denied peer catalogue writes must not reach SchemaManager"
    );

    let outbox = sm.take_outbox();
    assert!(
        outbox.iter().any(|message| matches!(
            message,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                    object_id,
                    ..
                }),
            } if *id == peer_id && *object_id == catalogue_object_id
        )),
        "peer client should receive CatalogueWriteDenied; outbox: {outbox:?}"
    );
}
```

- [ ] **Step 2: Run the red test**

Run:

```bash
cargo test -p jazz-tools --features test catalogue_update_from_peer_client_is_denied
```

Expected: FAIL because peer clients currently apply `CatalogueEntryUpdated`.

- [ ] **Step 3: Deny peer catalogue writes in `process_from_client`**

In `crates/jazz-tools/src/sync_manager/inbox.rs`, inside the `SyncPayload::CatalogueEntryUpdated` match arm, change the role match so `ClientRole::Peer` returns `CatalogueWriteDenied` and only `ClientRole::Admin` applies the payload:

```rust
                    ClientRole::Peer | ClientRole::Backend => {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(client_id),
                            payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                object_id,
                                branch_name,
                            }),
                        });
                    }
                    ClientRole::Admin => {
                        self.apply_payload_from_client(
                            storage,
                            client_id,
                            payload,
                            AuthoritativeFateRecording::Skip,
                        );
                    }
```

Keep the existing `ClientRole::User` branch unchanged.

- [ ] **Step 4: Run the denial test**

Run:

```bash
cargo test -p jazz-tools --features test catalogue_update_from_peer_client_is_denied
```

Expected: PASS.

- [ ] **Step 5: Run existing auth catalogue denial coverage**

Run:

```bash
cargo test -p jazz-tools --features test --test auth_test test_admin_secret_ws_connection_rejects_structural_schema_catalogue_sync
```

Expected: PASS.

- [ ] **Step 6: Commit the denial path**

```bash
git add crates/jazz-tools/src/sync_manager/tests/permissions.rs crates/jazz-tools/src/sync_manager/inbox.rs
git commit -m "fix: reject peer catalogue writes"
```

---

### Task 6: Add Edge/Core Integration Coverage

**Files:**

- Modify: `crates/jazz-tools/tests/edge_server_sync.rs`

- [ ] **Step 1: Add fresh-edge pull integration test**

Add this test below `core_schema_and_permissions_pushes_reach_every_edge_before_edge_clients_use_them()`:

````rust
/// A fresh edge connects after the core already has schema and permissions.
/// The edge must pull catalogue from core before any edge client query exists.
///
/// ```text
/// mallory --schema + permissions--> core
/// edge_eu --peer reconnect--------> core
/// core    --full catalogue replay-> edge_eu
/// alice   --write-----------------> edge_eu --sync--> core
/// ```
#[tokio::test]
async fn fresh_edge_pulls_existing_core_catalogue_on_connect_without_client_query() {
    let schema = todo_schema();
    let app_id = TestingServer::default_app_id();
    let query = QueryBuilder::new("todos").build();

    let core = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .start()
        .await;

    publish_schema_to_core(&core, &schema).await;
    let permissions_head = publish_allow_all_permissions(
        &core.base_url(),
        core.app_id(),
        core.admin_secret(),
        &schema,
    )
    .await;

    let edge_eu = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .with_upstream_url(core.base_url())
        .start()
        .await;

    wait_for_upstream(&edge_eu).await;
    wait_for_schema_hash(&edge_eu, &schema).await;
    wait_for_permissions_head(&edge_eu, &permissions_head.bundle_object_id).await;

    let alice = TestingClient::builder()
        .with_server(&edge_eu)
        .with_schema(schema.clone())
        .with_user_id("alice-fresh-edge-catalogue")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("fresh edge catalogue pull".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("fresh edge should write after pulling core catalogue");

    let alice_rows = wait_for_query(
        &alice,
        query,
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "alice sees row written after fresh edge catalogue pull",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![Value::Text("fresh edge catalogue pull".to_string())]
    );

    alice.shutdown().await.expect("shutdown alice");
    edge_eu.shutdown().await;
    core.shutdown().await;
}
````

- [ ] **Step 2: Add edge-forwarded publish propagation test**

Add this test below the fresh-edge test:

````rust
/// Catalogue published through one edge is forwarded to core, then core
/// propagates it to another connected edge over peer sync.
///
/// ```text
/// mallory --schema + permissions--> edge_us --HTTP forward--> core
/// core    --catalogue sync----------------------------------> edge_eu
/// alice   --write-------------------------------------------> edge_eu
/// ```
#[tokio::test]
async fn edge_catalogue_publish_reaches_peer_edge_through_core_sync() {
    let schema = todo_schema();
    let app_id = TestingServer::default_app_id();
    let query = QueryBuilder::new("todos").build();

    let core = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .start()
        .await;
    let edge_us = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .with_upstream_url(core.base_url())
        .start()
        .await;
    let edge_eu = TestingServer::builder()
        .with_app_id(app_id)
        .with_peer_secret(PEER_SECRET)
        .with_upstream_url(core.base_url())
        .start()
        .await;

    wait_for_upstream(&edge_us).await;
    wait_for_upstream(&edge_eu).await;

    let publish_response = reqwest::Client::new()
        .post(format!(
            "{}/apps/{}/admin/schemas",
            edge_us.base_url(),
            edge_us.app_id()
        ))
        .header("X-Jazz-Admin-Secret", edge_us.admin_secret())
        .json(&json!({ "schema": schema, "permissions": null }))
        .send()
        .await
        .expect("publish schema through edge_us");
    let status = publish_response.status();
    if status != StatusCode::CREATED {
        let body = publish_response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable response body>".to_string());
        panic!("schema publish through edge_us failed: {status} {body}");
    }

    let permissions_head = publish_allow_all_permissions(
        &edge_us.base_url(),
        edge_us.app_id(),
        edge_us.admin_secret(),
        &schema,
    )
    .await;

    wait_for_schema_hash(&edge_eu, &schema).await;
    wait_for_permissions_head(&edge_eu, &permissions_head.bundle_object_id).await;

    let alice = TestingClient::builder()
        .with_server(&edge_eu)
        .with_schema(schema.clone())
        .with_user_id("alice-peer-edge-catalogue")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([(
                "title".to_string(),
                Value::Text("catalogue forwarded through core".to_string()),
            )]),
            DurabilityTier::GlobalServer,
        )
        .await
        .expect("peer edge should write after receiving forwarded catalogue");

    let alice_rows = wait_for_query(
        &alice,
        query,
        Some(DurabilityTier::EdgeServer),
        REPLICATION_TIMEOUT,
        "alice sees row written after peer edge receives forwarded catalogue",
        |rows| (rows.len() == 1 && rows[0].0 == todo_id).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![Value::Text("catalogue forwarded through core".to_string())]
    );

    alice.shutdown().await.expect("shutdown alice");
    edge_eu.shutdown().await;
    edge_us.shutdown().await;
    core.shutdown().await;
}
````

- [ ] **Step 3: Run the integration tests**

Run:

```bash
cargo test -p jazz-tools --features test --test edge_server_sync fresh_edge_pulls_existing_core_catalogue_on_connect_without_client_query
cargo test -p jazz-tools --features test --test edge_server_sync edge_catalogue_publish_reaches_peer_edge_through_core_sync
```

Expected: both tests pass.

- [ ] **Step 4: Commit integration coverage**

```bash
git add crates/jazz-tools/tests/edge_server_sync.rs
git commit -m "test: cover edge catalogue pull and propagation"
```

---

### Task 7: Final Verification

**Files:**

- Verify all files changed in Tasks 1 through 6.

- [ ] **Step 1: Run focused Rust tests**

Run:

```bash
cargo test -p jazz-tools --features test peer_with_matching_catalogue_hash_skips_catalogue_replay
cargo test -p jazz-tools --features test peer_without_catalogue_hash_gets_full_catalogue_replay
cargo test -p jazz-tools --features test existing_peer_with_stale_catalogue_hash_gets_full_catalogue_replay_on_reconnect
cargo test -p jazz-tools --features test catalogue_update_from_peer_client_is_denied
cargo test -p jazz-tools peer_secret_does_not_grant_catalogue_publish_permission
cargo test -p jazz-tools --features test --test edge_server_sync fresh_edge_pulls_existing_core_catalogue_on_connect_without_client_query
cargo test -p jazz-tools --features test --test edge_server_sync edge_catalogue_publish_reaches_peer_edge_through_core_sync
```

Expected: every command exits 0.

- [ ] **Step 2: Run existing edge catalogue tests**

Run:

```bash
cargo test -p jazz-tools --features test --test edge_server_sync core_schema_and_permissions_pushes_reach_every_edge_before_edge_clients_use_them
cargo test -p jazz-tools --features test --test catalogue_sync_integration edge_catalogue_http_reads_and_writes_forward_to_real_core
```

Expected: both commands exit 0.

- [ ] **Step 3: Run formatting and the full edge sync integration file**

Run:

```bash
cargo fmt --check
cargo test -p jazz-tools --features test --test edge_server_sync
```

Expected: both commands exit 0.

- [ ] **Step 4: Inspect the final diff**

Run:

```bash
git diff main...HEAD --stat
git diff main...HEAD -- crates/jazz-tools/src/sync_manager/mod.rs crates/jazz-tools/src/runtime_core/sync.rs crates/jazz-tools/src/runtime_tokio.rs crates/jazz-tools/src/server/routes/websocket.rs crates/jazz-tools/src/transport_manager.rs crates/jazz-tools/src/sync_manager/inbox.rs crates/jazz-tools/tests/edge_server_sync.rs
```

Expected: diff only contains the catalogue reconnect behavior, authority hardening, and tests from this plan.

- [ ] **Step 5: Commit verification notes if any test-only adjustments were needed**

If Step 1 through Step 4 required test-name or import fixes, commit them:

```bash
git add crates/jazz-tools/src/runtime_core/tests/schema_catalogue.rs crates/jazz-tools/src/sync_manager/tests/permissions.rs crates/jazz-tools/tests/edge_server_sync.rs
git commit -m "test: finish edge catalogue reconnect coverage"
```

If Step 1 through Step 4 required no additional edits, do not create an empty commit.
