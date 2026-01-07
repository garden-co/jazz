//! WASM tests for IndexedDbEnvironment.
//!
//! Run with: wasm-pack test --headless --chrome

use bytes::Bytes;
use groove::{ChunkHash, ChunkStore, Commit, CommitStore};
use groove_wasm::IndexedDbEnvironment;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

/// Helper to create a unique database name for each test.
fn unique_db_name(test_name: &str) -> String {
    let timestamp = js_sys::Date::now() as u64;
    format!("groove_test_{}_{}", test_name, timestamp)
}

#[wasm_bindgen_test]
async fn test_chunk_store_put_get() {
    let db_name = unique_db_name("chunk_put_get");
    let env = IndexedDbEnvironment::with_name(&db_name)
        .await
        .expect("failed to create environment");

    // Put a chunk
    let data = Bytes::from_static(b"hello, world!");
    let hash = env.put_chunk(data.clone()).await;

    // Get it back
    let retrieved = env.get_chunk(&hash).await;
    assert!(retrieved.is_some(), "chunk should exist");
    assert_eq!(retrieved.unwrap(), data, "chunk data should match");
}

#[wasm_bindgen_test]
async fn test_chunk_store_has_chunk() {
    let db_name = unique_db_name("chunk_has");
    let env = IndexedDbEnvironment::with_name(&db_name)
        .await
        .expect("failed to create environment");

    // Check non-existent chunk
    let fake_hash = ChunkHash::compute(b"nonexistent");
    assert!(!env.has_chunk(&fake_hash).await, "should not have non-existent chunk");

    // Put and check
    let data = Bytes::from_static(b"test data");
    let hash = env.put_chunk(data).await;
    assert!(env.has_chunk(&hash).await, "should have chunk after put");
}

#[wasm_bindgen_test]
async fn test_chunk_hash_is_content_addressed() {
    let db_name = unique_db_name("chunk_hash");
    let env = IndexedDbEnvironment::with_name(&db_name)
        .await
        .expect("failed to create environment");

    // Same data should produce same hash
    let data = Bytes::from_static(b"duplicate data");
    let hash1 = env.put_chunk(data.clone()).await;
    let hash2 = env.put_chunk(data).await;

    assert_eq!(hash1, hash2, "same data should have same hash");
}

#[wasm_bindgen_test]
async fn test_commit_store_put_get() {
    let db_name = unique_db_name("commit_put_get");
    let env = IndexedDbEnvironment::with_name(&db_name)
        .await
        .expect("failed to create environment");

    // Create a commit
    let commit = Commit {
        parents: vec![],
        author: "test".to_string(),
        timestamp: 12345,
        meta: None,
        content: b"test content".to_vec().into_boxed_slice(),
    };

    // Put the commit
    let id = env.put_commit(&commit).await;

    // Get it back
    let retrieved = env.get_commit(&id).await;
    assert!(retrieved.is_some(), "commit should exist");

    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.author, commit.author);
    assert_eq!(retrieved.timestamp, commit.timestamp);
    assert_eq!(retrieved.content, commit.content);
}

#[wasm_bindgen_test]
async fn test_commit_store_with_parents() {
    let db_name = unique_db_name("commit_parents");
    let env = IndexedDbEnvironment::with_name(&db_name)
        .await
        .expect("failed to create environment");

    // Create first commit
    let commit1 = Commit {
        parents: vec![],
        author: "test".to_string(),
        timestamp: 100,
        meta: None,
        content: b"first".to_vec().into_boxed_slice(),
    };
    let id1 = env.put_commit(&commit1).await;

    // Create second commit with parent
    let commit2 = Commit {
        parents: vec![id1],
        author: "test".to_string(),
        timestamp: 200,
        meta: None,
        content: b"second".to_vec().into_boxed_slice(),
    };
    let id2 = env.put_commit(&commit2).await;

    // Verify parent relationship
    let retrieved = env.get_commit(&id2).await.unwrap();
    assert_eq!(retrieved.parents.len(), 1);
    assert_eq!(retrieved.parents[0], id1);
}

#[wasm_bindgen_test]
async fn test_commit_store_with_meta() {
    let db_name = unique_db_name("commit_meta");
    let env = IndexedDbEnvironment::with_name(&db_name)
        .await
        .expect("failed to create environment");

    // Create commit with metadata
    let mut meta = std::collections::BTreeMap::new();
    meta.insert("key1".to_string(), "value1".to_string());
    meta.insert("key2".to_string(), "value2".to_string());

    let commit = Commit {
        parents: vec![],
        author: "test".to_string(),
        timestamp: 12345,
        meta: Some(meta.clone()),
        content: b"with meta".to_vec().into_boxed_slice(),
    };

    let id = env.put_commit(&commit).await;

    // Verify meta is preserved
    let retrieved = env.get_commit(&id).await.unwrap();
    assert!(retrieved.meta.is_some(), "meta should be present");
    let retrieved_meta = retrieved.meta.unwrap();
    assert_eq!(retrieved_meta.get("key1"), Some(&"value1".to_string()));
    assert_eq!(retrieved_meta.get("key2"), Some(&"value2".to_string()));
}

#[wasm_bindgen_test]
async fn test_frontier_get_set() {
    let db_name = unique_db_name("frontier");
    let env = IndexedDbEnvironment::with_name(&db_name)
        .await
        .expect("failed to create environment");

    let object_id: u128 = 123456789;
    let branch = "main";

    // Initially empty
    let frontier = env.get_frontier(object_id, branch).await;
    assert!(frontier.is_empty(), "frontier should start empty");

    // Create some commits
    let commit1 = Commit {
        parents: vec![],
        author: "test".to_string(),
        timestamp: 100,
        meta: None,
        content: b"first".to_vec().into_boxed_slice(),
    };
    let id1 = env.put_commit(&commit1).await;

    // Set frontier
    env.set_frontier(object_id, branch, &[id1]).await;

    // Get frontier
    let frontier = env.get_frontier(object_id, branch).await;
    assert_eq!(frontier.len(), 1);
    assert_eq!(frontier[0], id1);
}

#[wasm_bindgen_test]
async fn test_truncation_get_set() {
    let db_name = unique_db_name("truncation");
    let env = IndexedDbEnvironment::with_name(&db_name)
        .await
        .expect("failed to create environment");

    let object_id: u128 = 987654321;
    let branch = "main";

    // Initially none
    let truncation = env.get_truncation(object_id, branch).await;
    assert!(truncation.is_none(), "truncation should start as None");

    // Create a commit
    let commit = Commit {
        parents: vec![],
        author: "test".to_string(),
        timestamp: 100,
        meta: None,
        content: b"truncation point".to_vec().into_boxed_slice(),
    };
    let id = env.put_commit(&commit).await;

    // Set truncation
    env.set_truncation(object_id, branch, Some(id)).await;

    // Get truncation
    let truncation = env.get_truncation(object_id, branch).await;
    assert!(truncation.is_some());
    assert_eq!(truncation.unwrap(), id);

    // Clear truncation
    env.set_truncation(object_id, branch, None).await;
    let truncation = env.get_truncation(object_id, branch).await;
    assert!(truncation.is_none(), "truncation should be cleared");
}

#[wasm_bindgen_test]
async fn test_persistence_across_reconnect() {
    let db_name = unique_db_name("persistence");

    // Create environment and store data
    let id1 = {
        let env = IndexedDbEnvironment::with_name(&db_name)
            .await
            .expect("failed to create environment");

        let commit = Commit {
            parents: vec![],
            author: "test".to_string(),
            timestamp: 12345,
            meta: None,
            content: b"persistent data".to_vec().into_boxed_slice(),
        };

        let id = env.put_commit(&commit).await;
        env.set_frontier(1, "main", &[id]).await;
        id
    };

    // Create new environment with same name - data should persist
    let env2 = IndexedDbEnvironment::with_name(&db_name)
        .await
        .expect("failed to create second environment");

    // Verify commit persisted
    let retrieved = env2.get_commit(&id1).await;
    assert!(retrieved.is_some(), "commit should persist across reconnect");
    assert_eq!(retrieved.unwrap().content.as_ref(), b"persistent data");

    // Verify frontier persisted
    let frontier = env2.get_frontier(1, "main").await;
    assert_eq!(frontier.len(), 1);
    assert_eq!(frontier[0], id1);
}

#[wasm_bindgen_test]
async fn test_large_chunk() {
    let db_name = unique_db_name("large_chunk");
    let env = IndexedDbEnvironment::with_name(&db_name)
        .await
        .expect("failed to create environment");

    // Create a large chunk (1MB)
    let large_data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
    let data = Bytes::from(large_data.clone());

    let hash = env.put_chunk(data).await;
    let retrieved = env.get_chunk(&hash).await.unwrap();

    assert_eq!(retrieved.len(), large_data.len());
    assert_eq!(retrieved.as_ref(), large_data.as_slice());
}
