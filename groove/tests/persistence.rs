//! Roundtrip persistence tests.
//!
//! These tests verify that a Database can be created, populated,
//! dropped, and then restored from the same Environment.

use std::sync::Arc;

use bytes::Bytes;
use groove::sql::row_buffer::{OwnedRow, RowValue};
use groove::sql::{Database, ExecuteResult, Value};
use groove::{ChunkStore, ContentRef, MemoryEnvironment, ObjectId, INLINE_THRESHOLD};

/// Helper to extract rows from ExecuteResult
fn get_rows(result: ExecuteResult) -> Vec<(ObjectId, OwnedRow)> {
    match result {
        ExecuteResult::Selected(rows) => rows,
        other => panic!("expected Selected, got {:?}", other),
    }
}

/// Helper to extract inserted ID from ExecuteResult
fn get_inserted_id(result: ExecuteResult) -> groove::ObjectId {
    match result {
        ExecuteResult::Inserted(id) => id,
        other => panic!("expected Inserted, got {:?}", other),
    }
}

#[test]
fn database_roundtrip_simple() {
    let env = Arc::new(MemoryEnvironment::new());

    // Create and populate database
    let catalog_id = {
        let db = Database::new(env.clone());

        db.execute("CREATE TABLE users (id I64 NOT NULL, name STRING NOT NULL)")
            .unwrap();

        db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
            .unwrap();

        db.catalog_object_id()
    };

    // Database dropped here - restore from same environment
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    // Verify table exists
    let tables = db.list_tables();
    assert_eq!(tables.len(), 1);
    assert!(tables.contains(&"users".to_string()));

    // Verify schema
    let schema = db.get_table("users").unwrap();
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[1].name, "name");

    // Verify data
    let result = db.execute("SELECT * FROM users").unwrap();
    let rows = get_rows(result);
    assert_eq!(rows.len(), 2, "should have 2 rows");

    // Check row values by name
    let has_alice = rows.iter().any(|r| r.1.get_by_name("name") == Some(RowValue::String("Alice")));
    let has_bob = rows.iter().any(|r| r.1.get_by_name("name") == Some(RowValue::String("Bob")));
    assert!(has_alice, "should contain Alice");
    assert!(has_bob, "should contain Bob");
}

#[test]
fn database_roundtrip_multiple_tables() {
    let env = Arc::new(MemoryEnvironment::new());

    // Create and populate database
    let catalog_id = {
        let db = Database::new(env.clone());

        db.execute("CREATE TABLE orgs (id I64 NOT NULL, name STRING NOT NULL)")
            .unwrap();
        db.execute("CREATE TABLE users (id I64 NOT NULL, name STRING NOT NULL, org_id REFERENCES orgs NOT NULL)")
            .unwrap();

        let acme_id = get_inserted_id(
            db.execute("INSERT INTO orgs (id, name) VALUES (1, 'Acme')")
                .unwrap(),
        );
        let globex_id = get_inserted_id(
            db.execute("INSERT INTO orgs (id, name) VALUES (2, 'Globex')")
                .unwrap(),
        );

        db.execute(&format!(
            "INSERT INTO users (id, name, org_id) VALUES (1, 'Alice', '{}')",
            acme_id
        ))
        .unwrap();
        db.execute(&format!(
            "INSERT INTO users (id, name, org_id) VALUES (2, 'Bob', '{}')",
            globex_id
        ))
        .unwrap();

        db.catalog_object_id()
    };

    // Restore database
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    // Verify both tables exist
    let tables = db.list_tables();
    assert_eq!(tables.len(), 2);
    assert!(tables.contains(&"orgs".to_string()));
    assert!(tables.contains(&"users".to_string()));

    // Verify orgs data
    let orgs = get_rows(db.execute("SELECT * FROM orgs").unwrap());
    assert_eq!(orgs.len(), 2);

    // Verify users data
    let users = get_rows(db.execute("SELECT * FROM users").unwrap());
    assert_eq!(users.len(), 2);

    // Verify ref column type was preserved
    let users_schema = db.get_table("users").unwrap();
    assert!(
        matches!(users_schema.columns[2].ty, groove::sql::ColumnType::Ref(_)),
        "org_id should be Ref type"
    );
}

#[test]
fn database_roundtrip_with_policies() {
    let env = Arc::new(MemoryEnvironment::new());

    // Create database with policies
    let catalog_id = {
        let db = Database::new(env.clone());

        db.execute("CREATE TABLE users (id I64 NOT NULL, name STRING NOT NULL)")
            .unwrap();
        db.execute("CREATE TABLE documents (id I64 NOT NULL, title STRING NOT NULL, owner_id REFERENCES users NOT NULL)")
            .unwrap();

        // Create users
        let alice_id = get_inserted_id(
            db.execute("INSERT INTO users (id, name) VALUES (100, 'Alice')")
                .unwrap(),
        );
        let bob_id = get_inserted_id(
            db.execute("INSERT INTO users (id, name) VALUES (200, 'Bob')")
                .unwrap(),
        );

        // Create documents
        db.execute(&format!(
            "INSERT INTO documents (id, title, owner_id) VALUES (1, 'Doc1', '{}')",
            alice_id
        ))
        .unwrap();
        db.execute(&format!(
            "INSERT INTO documents (id, title, owner_id) VALUES (2, 'Doc2', '{}')",
            bob_id
        ))
        .unwrap();

        // Create policy
        db.execute("CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer")
            .unwrap();

        db.catalog_object_id()
    };

    // Restore database
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    // Verify policies are restored
    let policies = db.get_policies("documents").unwrap();
    assert!(!policies.is_empty(), "policies should be restored");

    // Get Alice's ID to use as viewer
    let users = get_rows(db.execute("SELECT * FROM users").unwrap());
    let alice_id = users
        .iter()
        .find(|r| r.1.get_by_name("name") == Some(RowValue::String("Alice")))
        .unwrap()
        .0;

    // Verify policy works (select_all_as should filter)
    let rows = db.select_all_as("documents", alice_id).unwrap();
    assert_eq!(rows.len(), 1, "policy should filter to owner's docs");
    assert_eq!(
        rows[0].1.get_by_name("title"),
        Some(RowValue::String("Doc1")),
        "should see Doc1"
    );
}

#[test]
fn database_roundtrip_after_delete() {
    let env = Arc::new(MemoryEnvironment::new());

    // Create, populate, then delete some rows
    let catalog_id = {
        let db = Database::new(env.clone());

        db.execute("CREATE TABLE items (id I64 NOT NULL, name STRING NOT NULL)")
            .unwrap();

        db.execute("INSERT INTO items (id, name) VALUES (1, 'Item1')")
            .unwrap();
        db.execute("INSERT INTO items (id, name) VALUES (2, 'Item2')")
            .unwrap();
        db.execute("INSERT INTO items (id, name) VALUES (3, 'Item3')")
            .unwrap();

        // Get ID of Item2 to delete
        let items = get_rows(db.execute("SELECT * FROM items").unwrap());
        let item2_id = items
            .iter()
            .find(|r| r.1.get_by_name("name") == Some(RowValue::String("Item2")))
            .unwrap()
            .0;

        db.execute(&format!("DELETE FROM items WHERE id = '{}'", item2_id))
            .unwrap();

        // Verify delete worked
        let items = get_rows(db.execute("SELECT * FROM items").unwrap());
        assert_eq!(items.len(), 2);

        db.catalog_object_id()
    };

    // Restore and verify delete was persisted
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    let items = get_rows(db.execute("SELECT * FROM items").unwrap());
    assert_eq!(items.len(), 2, "delete should be persisted");

    let has_item2 = items.iter().any(|r| r.1.get_by_name("name") == Some(RowValue::String("Item2")));
    assert!(!has_item2, "Item2 should be deleted");
}

#[test]
fn database_roundtrip_after_update() {
    let env = Arc::new(MemoryEnvironment::new());

    // Create, populate, then update some rows
    let catalog_id = {
        let db = Database::new(env.clone());

        db.execute("CREATE TABLE settings (id I64 NOT NULL, value STRING NOT NULL)")
            .unwrap();

        db.execute("INSERT INTO settings (id, value) VALUES (1, 'old_value')")
            .unwrap();

        // Get ID of the row
        let settings = get_rows(db.execute("SELECT * FROM settings").unwrap());
        let row_id = settings[0].0;

        db.execute(&format!(
            "UPDATE settings SET value = 'new_value' WHERE id = '{}'",
            row_id
        ))
        .unwrap();

        // Verify update worked
        let settings = get_rows(db.execute("SELECT * FROM settings").unwrap());
        assert_eq!(
            settings[0].1.get_by_name("value"),
            Some(RowValue::String("new_value"))
        );

        db.catalog_object_id()
    };

    // Restore and verify update was persisted
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    let settings = get_rows(db.execute("SELECT * FROM settings").unwrap());
    assert_eq!(settings.len(), 1);
    assert_eq!(
        settings[0].1.get_by_name("value"),
        Some(RowValue::String("new_value")),
        "update should be persisted"
    );
}

#[test]
fn database_roundtrip_with_nullable() {
    let env = Arc::new(MemoryEnvironment::new());

    // Create table with nullable column
    let catalog_id = {
        let db = Database::new(env.clone());

        db.execute("CREATE TABLE contacts (id I64 NOT NULL, name STRING NOT NULL, phone STRING)")
            .unwrap();

        db.execute("INSERT INTO contacts (id, name, phone) VALUES (1, 'Alice', '555-1234')")
            .unwrap();
        db.execute("INSERT INTO contacts (id, name) VALUES (2, 'Bob')")
            .unwrap();

        db.catalog_object_id()
    };

    // Restore and verify nulls are preserved
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    let contacts = get_rows(db.execute("SELECT * FROM contacts").unwrap());
    assert_eq!(contacts.len(), 2);

    // Find Alice and Bob
    let alice = contacts
        .iter()
        .find(|r| r.1.get_by_name("name") == Some(RowValue::String("Alice")))
        .unwrap();
    let bob = contacts
        .iter()
        .find(|r| r.1.get_by_name("name") == Some(RowValue::String("Bob")))
        .unwrap();

    // Alice has phone (not null)
    assert!(
        !matches!(alice.1.get_by_name("phone"), Some(RowValue::Null) | None),
        "Alice should have phone"
    );

    // Bob doesn't have phone (should be Null)
    assert!(
        matches!(bob.1.get_by_name("phone"), Some(RowValue::Null)),
        "Bob should have null phone"
    );
}

#[test]
fn database_roundtrip_with_inline_blob() {
    let env = Arc::new(MemoryEnvironment::new());

    // Small blob data (will be stored inline)
    let small_data: Vec<u8> = (0..100).map(|i| i as u8).collect();

    let catalog_id = {
        let db = Database::new(env.clone());

        db.execute("CREATE TABLE files (id I64 NOT NULL, name STRING NOT NULL, data BLOB NOT NULL)")
            .unwrap();

        // Create inline blob
        let blob_ref = ContentRef::inline(small_data.clone());

        // Insert with blob
        db.insert_with("files", |b| b
            .set_i64_by_name("id", 1)
            .set_string_by_name("name", "small.bin")
            .set_blob_by_name("data", blob_ref)
            .build()).unwrap();

        db.catalog_object_id()
    };

    // Restore database
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    // Verify blob data
    let rows = get_rows(db.execute("SELECT * FROM files").unwrap());
    assert_eq!(rows.len(), 1, "should have 1 file");

    // Check blob content
    if let Some(RowValue::Blob(content_ref)) = rows[0].1.get_by_name("data") {
        let data = content_ref.as_inline().expect("should be inline blob");
        assert_eq!(data, small_data.as_slice(), "blob data should match");
    } else {
        panic!("expected Blob value, got {:?}", rows[0].1.get_by_name("data"));
    }
}

#[test]
fn database_roundtrip_with_chunked_blob() {
    use futures::executor::block_on;

    let env = Arc::new(MemoryEnvironment::new());

    // Large blob data (will be chunked) - 2 chunks worth
    let large_data: Vec<u8> = (0..(INLINE_THRESHOLD + 1000))
        .map(|i| (i % 256) as u8)
        .collect();

    let catalog_id = {
        let db = Database::new(env.clone());

        db.execute("CREATE TABLE files (id I64 NOT NULL, name STRING NOT NULL, data BLOB NOT NULL)")
            .unwrap();

        // Manually chunk the data and store in environment
        let chunk_size = INLINE_THRESHOLD;
        let mut hashes = Vec::new();
        for chunk in large_data.chunks(chunk_size) {
            let hash = block_on(env.put_chunk(Bytes::copy_from_slice(chunk)));
            hashes.push(hash);
        }

        // Create chunked blob reference
        let blob_ref = ContentRef::chunked(hashes);

        // Insert with blob
        db.insert_with("files", |b| b
            .set_i64_by_name("id", 1)
            .set_string_by_name("name", "large.bin")
            .set_blob_by_name("data", blob_ref)
            .build()).unwrap();

        db.catalog_object_id()
    };

    // Restore database
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    // Verify blob data
    let rows = get_rows(db.execute("SELECT * FROM files").unwrap());
    assert_eq!(rows.len(), 1, "should have 1 file");

    // Check blob content - need to read chunks from environment
    if let Some(RowValue::Blob(content_ref)) = rows[0].1.get_by_name("data") {
        let chunk_hashes = content_ref.as_chunks().expect("should be chunked blob");
        assert_eq!(chunk_hashes.len(), 2, "should have 2 chunks");

        // Read and concatenate chunks
        let mut restored_data = Vec::new();
        for hash in chunk_hashes {
            let chunk = block_on(env.get_chunk(hash)).expect("chunk should exist");
            restored_data.extend_from_slice(&chunk);
        }

        assert_eq!(restored_data.len(), large_data.len(), "blob size should match");
        assert_eq!(restored_data, large_data, "blob data should match");
    } else {
        panic!("expected Blob value, got {:?}", rows[0].1.get_by_name("data"));
    }
}
