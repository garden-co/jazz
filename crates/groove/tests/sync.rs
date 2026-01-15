//! Sync integration tests.
//!
//! These tests verify the complete sync flow including LocalNode integration.

#![cfg(not(target_arch = "wasm32"))]

use std::rc::Rc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;

use groove::sync::test_harness::{
    TestClientEnv, TestHarness, TestTransport, create_synced_node, create_synced_node_with_config,
};
use groove::sync::{ClientError, SseEvent, SyncConfig};
use groove::{Commit, CommitStore, ObjectId};

// ============================================================================
// Helpers
// ============================================================================

/// Helper to receive an event with timeout.
async fn recv_event(stream: &mut BoxStream<'static, Result<SseEvent, ClientError>>) -> SseEvent {
    tokio::time::timeout(Duration::from_millis(100), stream.next())
        .await
        .expect("timeout waiting for event")
        .expect("stream ended")
        .unwrap()
}

/// Helper to check no event is pending.
async fn assert_no_event(stream: &mut BoxStream<'static, Result<SseEvent, ClientError>>) {
    let result = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
    assert!(result.is_err(), "expected no event but received one");
}

// ============================================================================
// Basic Sync Tests
// ============================================================================

#[tokio::test]
async fn test_write_and_push() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");

    let _stream = alice.subscribe_all().await.unwrap();

    let object_id = ObjectId(42);
    let (commit_id, response) = alice
        .write_and_push(object_id, b"hello world")
        .await
        .unwrap();

    // Push should succeed
    assert!(response.accepted);
    assert_eq!(response.frontier, vec![commit_id]);

    // Alice's LocalNode should have the commit
    assert!(alice.has_commit(object_id, &commit_id));
    assert_eq!(
        alice.get_commit_content(object_id, &commit_id),
        Some(b"hello world".to_vec())
    );
}

#[tokio::test]
async fn test_two_clients_sync() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");

    let object_id = ObjectId(123);

    let _alice_stream = alice.subscribe_all().await.unwrap();
    let mut bob_stream = bob.subscribe_all().await.unwrap();

    // Alice writes and pushes
    let (commit_id, response) = alice
        .write_and_push(object_id, b"alice's data")
        .await
        .unwrap();
    assert!(response.accepted);

    // Alice should have the commit
    assert!(alice.has_commit(object_id, &commit_id));

    // Bob receives the broadcast
    let event = recv_event(&mut bob_stream).await;
    match &event {
        SseEvent::Commits {
            object_id: oid,
            commits,
            frontier,
            ..
        } => {
            assert_eq!(*oid, object_id);
            assert_eq!(commits.len(), 1);
            assert_eq!(commits[0].author, "alice");
            assert_eq!(*frontier, vec![commit_id]);
        }
        other => panic!("Expected Commits event, got {:?}", other),
    }

    // Bob applies the event to his LocalNode
    bob.apply_event(&event);

    // Now Bob should have the commit too
    assert!(bob.has_commit(object_id, &commit_id));
    assert_eq!(
        bob.get_commit_content(object_id, &commit_id),
        Some(b"alice's data".to_vec())
    );

    // Both should be able to read the same content
    assert_eq!(alice.read(object_id), Some(b"alice's data".to_vec()));
    assert_eq!(bob.read(object_id), Some(b"alice's data".to_vec()));
}

#[tokio::test]
async fn test_bidirectional_sync() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");

    let object_a = ObjectId(100);
    let object_b = ObjectId(200);

    let mut alice_stream = alice.subscribe_all().await.unwrap();
    let mut bob_stream = bob.subscribe_all().await.unwrap();

    // Alice writes object A
    let (commit_a, _) = alice.write_and_push(object_a, b"from alice").await.unwrap();

    // Bob receives and applies
    let event = recv_event(&mut bob_stream).await;
    bob.apply_event(&event);
    assert!(bob.has_commit(object_a, &commit_a));

    // Bob writes object B
    let (commit_b, _) = bob.write_and_push(object_b, b"from bob").await.unwrap();

    // Alice receives and applies
    let event = recv_event(&mut alice_stream).await;
    alice.apply_event(&event);
    assert!(alice.has_commit(object_b, &commit_b));

    // Both have both objects now
    assert!(alice.has_commit(object_a, &commit_a));
    assert!(alice.has_commit(object_b, &commit_b));
    assert!(bob.has_commit(object_a, &commit_a));
    assert!(bob.has_commit(object_b, &commit_b));
}

#[tokio::test]
async fn test_three_clients_sync() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");
    let mut charlie = harness.create_client("charlie");

    let object_id = ObjectId(300);

    let _alice_stream = alice.subscribe_all().await.unwrap();
    let mut bob_stream = bob.subscribe_all().await.unwrap();
    let mut charlie_stream = charlie.subscribe_all().await.unwrap();

    // Alice writes
    let (commit_id, _) = alice
        .write_and_push(object_id, b"shared data")
        .await
        .unwrap();

    // Both Bob and Charlie receive
    let bob_event = recv_event(&mut bob_stream).await;
    let charlie_event = recv_event(&mut charlie_stream).await;

    bob.apply_event(&bob_event);
    charlie.apply_event(&charlie_event);

    // All three have the commit
    assert!(alice.has_commit(object_id, &commit_id));
    assert!(bob.has_commit(object_id, &commit_id));
    assert!(charlie.has_commit(object_id, &commit_id));

    // All three can read the same content
    assert_eq!(alice.read(object_id), Some(b"shared data".to_vec()));
    assert_eq!(bob.read(object_id), Some(b"shared data".to_vec()));
    assert_eq!(charlie.read(object_id), Some(b"shared data".to_vec()));
}

#[tokio::test]
async fn test_sequential_commits() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");

    let object_id = ObjectId(400);

    let _alice_stream = alice.subscribe_all().await.unwrap();
    let mut bob_stream = bob.subscribe_all().await.unwrap();

    // Alice writes three sequential commits
    let (c1, _) = alice.write_and_push(object_id, b"version 1").await.unwrap();
    let event1 = recv_event(&mut bob_stream).await;
    bob.apply_event(&event1);

    let (c2, _) = alice.write_and_push(object_id, b"version 2").await.unwrap();
    let event2 = recv_event(&mut bob_stream).await;
    bob.apply_event(&event2);

    let (c3, _) = alice.write_and_push(object_id, b"version 3").await.unwrap();
    let event3 = recv_event(&mut bob_stream).await;
    bob.apply_event(&event3);

    // Bob has all commits
    assert!(bob.has_commit(object_id, &c1));
    assert!(bob.has_commit(object_id, &c2));
    assert!(bob.has_commit(object_id, &c3));

    // Bob's content is the latest
    assert_eq!(bob.read(object_id), Some(b"version 3".to_vec()));

    // Both have the same frontier
    assert_eq!(alice.frontier(object_id), vec![c3]);
    assert_eq!(bob.frontier(object_id), vec![c3]);
}

#[tokio::test]
async fn test_pusher_does_not_receive_own_broadcast() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");

    let object_id = ObjectId(500);

    let mut alice_stream = alice.subscribe_all().await.unwrap();

    alice.write_and_push(object_id, b"my data").await.unwrap();

    // Alice should NOT receive her own broadcast
    assert_no_event(&mut alice_stream).await;
}

// ============================================================================
// Multiple Objects Tests
// ============================================================================

#[tokio::test]
async fn test_multiple_objects() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");

    let _alice_stream = alice.subscribe_all().await.unwrap();
    let mut bob_stream = bob.subscribe_all().await.unwrap();

    // Alice creates 5 different objects
    let mut commits = Vec::new();
    for i in 0..5 {
        let object_id = ObjectId(600 + i);
        let (commit_id, _) = alice
            .write_and_push(object_id, format!("object {}", i).as_bytes())
            .await
            .unwrap();
        commits.push((object_id, commit_id));
    }

    // Bob receives all 5 broadcasts
    for _ in 0..5 {
        let event = recv_event(&mut bob_stream).await;
        bob.apply_event(&event);
    }

    // Bob has all commits
    for (object_id, commit_id) in &commits {
        assert!(bob.has_commit(*object_id, commit_id));
    }
}

// ============================================================================
// Concurrent Write Tests
// ============================================================================

#[tokio::test]
async fn test_concurrent_writes_to_different_objects() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");

    let object_a = ObjectId(700);
    let object_b = ObjectId(701);

    let mut alice_stream = alice.subscribe_all().await.unwrap();
    let mut bob_stream = bob.subscribe_all().await.unwrap();

    // Both write simultaneously to different objects
    let (ca, _) = alice
        .write_and_push(object_a, b"alice's object")
        .await
        .unwrap();
    let (cb, _) = bob.write_and_push(object_b, b"bob's object").await.unwrap();

    // Each receives the other's broadcast
    let alice_event = recv_event(&mut alice_stream).await;
    let bob_event = recv_event(&mut bob_stream).await;

    alice.apply_event(&alice_event);
    bob.apply_event(&bob_event);

    // Both have both objects
    assert!(alice.has_commit(object_a, &ca));
    assert!(alice.has_commit(object_b, &cb));
    assert!(bob.has_commit(object_a, &ca));
    assert!(bob.has_commit(object_b, &cb));
}

#[tokio::test]
async fn test_concurrent_writes_to_same_object() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");

    let object_id = ObjectId(800);

    let mut alice_stream = alice.subscribe_all().await.unwrap();
    let mut bob_stream = bob.subscribe_all().await.unwrap();

    // Alice writes first
    let (ca, _) = alice
        .write_and_push(object_id, b"alice's version")
        .await
        .unwrap();

    // Bob receives Alice's commit
    let bob_event = recv_event(&mut bob_stream).await;
    bob.apply_event(&bob_event);

    // Now Bob writes (his commit will have Alice's as parent)
    let (cb, _) = bob
        .write_and_push(object_id, b"bob's version")
        .await
        .unwrap();

    // Alice receives Bob's commit
    let alice_event = recv_event(&mut alice_stream).await;
    alice.apply_event(&alice_event);

    // Both have both commits
    assert!(alice.has_commit(object_id, &ca));
    assert!(alice.has_commit(object_id, &cb));
    assert!(bob.has_commit(object_id, &ca));
    assert!(bob.has_commit(object_id, &cb));

    // Both should have the same frontier (Bob's commit)
    assert_eq!(alice.frontier(object_id), vec![cb]);
    assert_eq!(bob.frontier(object_id), vec![cb]);
}

// ============================================================================
// Data Integrity Tests
// ============================================================================

#[tokio::test]
async fn test_large_content() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");

    let object_id = ObjectId(900);

    let _alice_stream = alice.subscribe_all().await.unwrap();
    let mut bob_stream = bob.subscribe_all().await.unwrap();

    // Create 1MB of data
    let large_content: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

    let (commit_id, _) = alice
        .write_and_push(object_id, &large_content)
        .await
        .unwrap();

    let event = recv_event(&mut bob_stream).await;
    bob.apply_event(&event);

    // Bob should have the exact same content
    assert!(bob.has_commit(object_id, &commit_id));
    assert_eq!(bob.read(object_id), Some(large_content));
}

#[tokio::test]
async fn test_storage_isolation() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");
    let bob = harness.create_client("bob"); // Bob doesn't subscribe

    let object_id = ObjectId(1000);
    let _alice_stream = alice.subscribe_all().await.unwrap();

    // Alice writes
    let (commit_id, _) = alice.write_and_push(object_id, b"private").await.unwrap();

    // Alice has the commit
    assert!(alice.has_commit(object_id, &commit_id));

    // Bob does NOT have it (he never subscribed and never received the event)
    assert!(!bob.has_commit(object_id, &commit_id));
    assert_eq!(bob.read(object_id), None);
}

// ============================================================================
// Many Clients Tests
// ============================================================================

#[tokio::test]
async fn test_many_clients() {
    let harness = TestHarness::new();
    let object_id = ObjectId(1100);

    // Create 10 clients
    let mut clients: Vec<_> = (0..10)
        .map(|i| harness.create_client(format!("client_{}", i)))
        .collect();

    // All subscribe
    let mut streams = Vec::new();
    for client in &mut clients {
        streams.push(client.subscribe_all().await.unwrap());
    }

    // First client writes
    let (commit_id, _) = clients[0]
        .write_and_push(object_id, b"broadcast")
        .await
        .unwrap();

    // All other clients receive and apply
    for (i, stream) in streams.iter_mut().enumerate().skip(1) {
        let event = recv_event(stream).await;
        clients[i].apply_event(&event);
    }

    // All clients have the commit
    for client in &clients {
        assert!(client.has_commit(object_id, &commit_id));
    }
}

// ============================================================================
// Server State Tests
// ============================================================================

#[tokio::test]
async fn test_server_stores_commits() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");

    let object_id = ObjectId(1200);
    let _stream = alice.subscribe_all().await.unwrap();

    let (commit_id, _) = alice
        .write_and_push(object_id, b"server data")
        .await
        .unwrap();

    // Server should have the commit
    let server_commit = harness.server_env().get_commit(&commit_id).await;
    assert!(server_commit.is_some());
    assert_eq!(server_commit.unwrap().content.as_ref(), b"server data");

    // Server frontier should be updated
    let server_frontier = harness.server_env().get_frontier(object_id.0, "main").await;
    assert_eq!(server_frontier, vec![commit_id]);
}

#[tokio::test]
async fn test_new_subscriber_receives_existing_data() {
    let harness = TestHarness::new();
    let mut alice = harness.create_client("alice");

    let object_id = ObjectId(1300);

    // Alice subscribes and writes
    let _alice_stream = alice.subscribe_all().await.unwrap();
    let (commit_id, _) = alice.write_and_push(object_id, b"existing").await.unwrap();

    // Bob subscribes AFTER the write
    let mut bob = harness.create_client("bob");
    let mut bob_stream = bob.subscribe_all().await.unwrap();

    // Bob should receive initial data with the existing commit
    let event = recv_event(&mut bob_stream).await;
    bob.apply_event(&event);

    // Bob now has the commit
    assert!(bob.has_commit(object_id, &commit_id));
    assert_eq!(bob.read(object_id), Some(b"existing".to_vec()));
}

// ============================================================================
// SyncedNode Tests
// ============================================================================

#[tokio::test]
async fn test_synced_node_structure() {
    let transport = Rc::new(TestTransport::new());
    let synced_node = create_synced_node(Rc::clone(&transport), "alice");

    // SyncedNode should have no upstream connections initially
    assert!(!synced_node.has_upstream());

    // Add an upstream connection
    let env = TestClientEnv::new(Rc::clone(&transport), "alice");
    let upstream_id = synced_node.add_upstream(env);

    // Now we have an upstream
    assert!(synced_node.has_upstream());
    assert_eq!(synced_node.upstream_ids(), vec![upstream_id]);

    // Can get the upstream state
    let state = synced_node.upstream_state(upstream_id);
    assert!(state.is_some());

    // Remove the upstream
    let removed = synced_node.remove_upstream(upstream_id);
    assert!(removed);
    assert!(!synced_node.has_upstream());
}

#[tokio::test]
async fn test_synced_node_write_buffer() {
    let transport = Rc::new(TestTransport::new());
    let config = SyncConfig {
        write_debounce_ms: 50,
        max_batch_age_ms: 200,
        ..SyncConfig::default()
    };
    let synced_node = create_synced_node_with_config(Rc::clone(&transport), "alice", config);

    let object_id = ObjectId(1400);

    // Queue a write
    synced_node.queue_for_push(object_id, "main");

    // Initially not ready (debounce hasn't expired)
    let ready = synced_node.ready_to_push();
    assert!(ready.is_empty());

    // Wait for debounce to expire
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Now it should be ready
    let ready = synced_node.ready_to_push();
    assert_eq!(ready, vec![object_id]);

    // Mark as pushed
    synced_node.mark_pushed(&object_id);

    // No longer ready
    let ready = synced_node.ready_to_push();
    assert!(ready.is_empty());
}

#[tokio::test]
async fn test_synced_node_write_buffer_max_age() {
    let transport = Rc::new(TestTransport::new());
    let config = SyncConfig {
        write_debounce_ms: 1000, // Very long debounce
        max_batch_age_ms: 50,    // Short max age
        ..SyncConfig::default()
    };
    let synced_node = create_synced_node_with_config(Rc::clone(&transport), "alice", config);

    let object_id = ObjectId(1500);

    // Queue a write
    synced_node.queue_for_push(object_id, "main");

    // Wait for max age to expire (should trigger before debounce)
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Should be ready due to max age
    let ready = synced_node.ready_to_push();
    assert_eq!(ready, vec![object_id]);
}

#[tokio::test]
async fn test_synced_node_apply_upstream_commits() {
    let transport = Rc::new(TestTransport::new());
    let synced_node = create_synced_node(Rc::clone(&transport), "alice");

    // Add an upstream
    let env = TestClientEnv::new(Rc::clone(&transport), "alice");
    let upstream_id = synced_node.add_upstream(env);

    let object_id = ObjectId(1600);

    // Create a commit to apply
    let commit = Commit {
        parents: vec![],
        content: b"synced data".to_vec().into_boxed_slice(),
        author: "server".to_string(),
        timestamp: 0,
        meta: None,
    };
    let commit_id = commit.compute_id();

    // Ensure object exists
    synced_node.node().ensure_object(object_id, "");

    // Apply commits from upstream (no table name - this is raw object sync)
    synced_node.apply_upstream_commits(upstream_id, object_id, vec![commit], vec![commit_id], None);

    // The commit should be in LocalNode
    assert!(synced_node.node().has_commit(object_id, "main", &commit_id));

    // Can read the content
    let content = synced_node.node().read(object_id, "main").unwrap();
    assert_eq!(content, Some(b"synced data".to_vec()));
}

#[tokio::test]
async fn test_synced_node_connected_clients() {
    let transport = Rc::new(TestTransport::new());
    let synced_node = create_synced_node(Rc::clone(&transport), "server");

    // Create a client identity and SSE channel
    let mut identity = groove::sync::ClientIdentity::simple("client1");
    identity.name = Some("Test Client".to_string());
    let (tx, _rx) = tokio::sync::mpsc::channel(16);

    // Accept the client
    let session_id = synced_node.accept_client(identity, tx);

    // Touch the session (update activity)
    synced_node.touch_session(session_id);

    // Remove the client
    synced_node.remove_client(session_id);
}
