//! Roundtrip persistence tests.
//!
//! These tests verify that a Database can be created, populated,
//! dropped, and then restored from the same Environment.

use std::sync::Arc;

use bytes::Bytes;
use groove::sql::row_buffer::RowValue;
use groove::sql::{Database, ExecuteResult};
use groove::{ChunkStore, ContentRef, INLINE_THRESHOLD, MemoryEnvironment};

/// Helper to extract inserted ID from ExecuteResult
fn get_inserted_id(result: ExecuteResult) -> groove::ObjectId {
    match result {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        other => panic!("expected Inserted, got {:?}", other),
    }
}

#[test]
fn database_roundtrip_simple() {
    let env = Arc::new(MemoryEnvironment::new());

    // Create and populate database
    let catalog_id = {
        let db = Database::new(env.clone());

        // Note: id column is auto-added as ObjectId type
        db.execute("CREATE TABLE users (name STRING NOT NULL)")
            .unwrap();

        db.execute("INSERT INTO users (name) VALUES ('Alice')")
            .unwrap();
        db.execute("INSERT INTO users (name) VALUES ('Bob')")
            .unwrap();

        db.catalog_object_id()
    };

    // Database dropped here - restore from same environment
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    // Verify table exists
    let tables = db.list_tables();
    assert_eq!(tables.len(), 1);
    assert!(tables.contains(&"users".to_string()));

    // Verify schema (id is auto-added + name)
    let schema = db.get_table("users").unwrap();
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[1].name, "name");

    // Verify data
    let rows = db.query("SELECT * FROM users").unwrap();
    assert_eq!(rows.len(), 2, "should have 2 rows");

    // Check row values by name
    let has_alice = rows
        .iter()
        .any(|r| r.1.get_by_name("name") == Some(RowValue::String("Alice")));
    let has_bob = rows
        .iter()
        .any(|r| r.1.get_by_name("name") == Some(RowValue::String("Bob")));
    assert!(has_alice, "should contain Alice");
    assert!(has_bob, "should contain Bob");
}

#[test]
fn database_roundtrip_multiple_tables() {
    let env = Arc::new(MemoryEnvironment::new());

    // Create and populate database
    let catalog_id = {
        let db = Database::new(env.clone());

        // Note: id columns are auto-added as ObjectId type
        db.execute("CREATE TABLE orgs (name STRING NOT NULL)")
            .unwrap();
        db.execute("CREATE TABLE users (name STRING NOT NULL, org_id REFERENCES orgs NOT NULL)")
            .unwrap();

        let acme_id = get_inserted_id(
            db.execute("INSERT INTO orgs (name) VALUES ('Acme')")
                .unwrap(),
        );
        let globex_id = get_inserted_id(
            db.execute("INSERT INTO orgs (name) VALUES ('Globex')")
                .unwrap(),
        );

        db.execute(&format!(
            "INSERT INTO users (name, org_id) VALUES ('Alice', '{}')",
            acme_id
        ))
        .unwrap();
        db.execute(&format!(
            "INSERT INTO users (name, org_id) VALUES ('Bob', '{}')",
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
    let orgs = db.query("SELECT * FROM orgs").unwrap();
    assert_eq!(orgs.len(), 2);

    // Verify users data
    let users = db.query("SELECT * FROM users").unwrap();
    assert_eq!(users.len(), 2);

    // Verify ref column type was preserved (org_id is now at index 2: id, name, org_id)
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

        // Note: id columns are auto-added as ObjectId type
        db.execute("CREATE TABLE users (name STRING NOT NULL)")
            .unwrap();
        db.execute(
            "CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
        )
        .unwrap();

        // Create users
        let alice_id = get_inserted_id(
            db.execute("INSERT INTO users (name) VALUES ('Alice')")
                .unwrap(),
        );
        let bob_id = get_inserted_id(
            db.execute("INSERT INTO users (name) VALUES ('Bob')")
                .unwrap(),
        );

        // Create documents
        db.execute(&format!(
            "INSERT INTO documents (title, owner_id) VALUES ('Doc1', '{}')",
            alice_id
        ))
        .unwrap();
        db.execute(&format!(
            "INSERT INTO documents (title, owner_id) VALUES ('Doc2', '{}')",
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
    let users = db.query("SELECT * FROM users").unwrap();
    let alice_id = users
        .iter()
        .find(|r| r.1.get_by_name("name") == Some(RowValue::String("Alice")))
        .unwrap()
        .0;

    // Verify policy works (query_as should filter)
    let rows = db.query_as("SELECT * FROM documents", alice_id).unwrap();
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

        // Note: id column is auto-added as ObjectId type
        db.execute("CREATE TABLE items (name STRING NOT NULL)")
            .unwrap();

        db.execute("INSERT INTO items (name) VALUES ('Item1')")
            .unwrap();
        db.execute("INSERT INTO items (name) VALUES ('Item2')")
            .unwrap();
        db.execute("INSERT INTO items (name) VALUES ('Item3')")
            .unwrap();

        // Get ID of Item2 to delete
        let items = db.query("SELECT * FROM items").unwrap();
        let item2_id = items
            .iter()
            .find(|r| r.1.get_by_name("name") == Some(RowValue::String("Item2")))
            .unwrap()
            .0;

        db.execute(&format!("DELETE FROM items WHERE id = '{}'", item2_id))
            .unwrap();

        // Verify delete worked
        let items = db.query("SELECT * FROM items").unwrap();
        assert_eq!(items.len(), 2);

        db.catalog_object_id()
    };

    // Restore and verify delete was persisted
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    let items = db.query("SELECT * FROM items").unwrap();
    assert_eq!(items.len(), 2, "delete should be persisted");

    let has_item2 = items
        .iter()
        .any(|r| r.1.get_by_name("name") == Some(RowValue::String("Item2")));
    assert!(!has_item2, "Item2 should be deleted");
}

#[test]
fn database_roundtrip_after_update() {
    let env = Arc::new(MemoryEnvironment::new());

    // Create, populate, then update some rows
    let catalog_id = {
        let db = Database::new(env.clone());

        db.execute("CREATE TABLE settings (value STRING NOT NULL)")
            .unwrap();

        db.execute("INSERT INTO settings (value) VALUES ('old_value')")
            .unwrap();

        // Get ID of the row
        let settings = db.query("SELECT * FROM settings").unwrap();
        let row_id = settings[0].0;

        db.execute(&format!(
            "UPDATE settings SET value = 'new_value' WHERE id = '{}'",
            row_id
        ))
        .unwrap();

        // Verify update worked
        let settings = db.query("SELECT * FROM settings").unwrap();
        assert_eq!(
            settings[0].1.get_by_name("value"),
            Some(RowValue::String("new_value"))
        );

        db.catalog_object_id()
    };

    // Restore and verify update was persisted
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    let settings = db.query("SELECT * FROM settings").unwrap();
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

        db.execute("CREATE TABLE contacts (name STRING NOT NULL, phone STRING)")
            .unwrap();

        db.execute("INSERT INTO contacts (name, phone) VALUES ('Alice', '555-1234')")
            .unwrap();
        db.execute("INSERT INTO contacts (name) VALUES ('Bob')")
            .unwrap();

        db.catalog_object_id()
    };

    // Restore and verify nulls are preserved
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    let contacts = db.query("SELECT * FROM contacts").unwrap();
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

        db.execute("CREATE TABLE files (name STRING NOT NULL, data BLOB NOT NULL)")
            .unwrap();

        // Create inline blob
        let blob_ref = ContentRef::inline(small_data.clone());

        // Insert with blob - test row creation separately
        let schema = db.get_table("files").unwrap();
        let descriptor = std::sync::Arc::new(
            groove::sql::row_buffer::RowDescriptor::from_table_schema(&schema),
        );
        eprintln!(
            "Schema columns: {:?}",
            schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
        eprintln!("Descriptor columns:");
        for (i, col) in descriptor.columns.iter().enumerate() {
            eprintln!(
                "  [{}]: name={}, ty={:?}, offset={}",
                i, col.name, col.ty, col.offset
            );
        }
        let row = groove::sql::row_buffer::RowBuilder::new(descriptor.clone())
            .set_string_by_name("name", "small.bin")
            .set_blob_by_name("data", blob_ref)
            .build();
        eprintln!("Row buffer before insert: {} bytes", row.buffer.len());
        eprintln!("Row data column: {:?}", row.get_by_name("data"));
        db.insert_row("files", row).unwrap();

        // Debug: check if blob was stored correctly
        let rows_before = db.query("SELECT * FROM files").unwrap();
        eprintln!("BEFORE RESTORE:");
        eprintln!("Row buffer len: {}", rows_before[0].1.buffer.len());
        eprintln!("Data value: {:?}", rows_before[0].1.get_by_name("data"));

        db.catalog_object_id()
    };

    // Restore database
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    // Verify blob data
    let rows = db.query("SELECT * FROM files").unwrap();
    assert_eq!(rows.len(), 1, "should have 1 file");

    // Debug: print column names and types
    for (i, col) in rows[0].1.descriptor.columns.iter().enumerate() {
        eprintln!(
            "Col[{}]: name={}, ty={:?}, offset={}, nullable={}",
            i, col.name, col.ty, col.offset, col.nullable
        );
    }
    eprintln!("Row buffer len: {}", rows[0].1.buffer.len());
    eprintln!("Row buffer: {:?}", &rows[0].1.buffer);

    // Try getting by index
    eprintln!("Value at index 2: {:?}", rows[0].1.get(2));

    // Check blob content
    if let Some(RowValue::Blob(content_ref)) = rows[0].1.get_by_name("data") {
        let data = content_ref.as_inline().expect("should be inline blob");
        assert_eq!(data, small_data.as_slice(), "blob data should match");
    } else {
        panic!(
            "expected Blob value, got {:?}",
            rows[0].1.get_by_name("data")
        );
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

        db.execute("CREATE TABLE files (name STRING NOT NULL, data BLOB NOT NULL)")
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
        db.insert_with("files", |b| {
            b.set_string_by_name("name", "large.bin")
                .set_blob_by_name("data", blob_ref)
                .build()
        })
        .unwrap();

        db.catalog_object_id()
    };

    // Restore database
    let db = futures::executor::block_on(Database::from_env(env.clone(), catalog_id)).unwrap();

    // Verify blob data
    let rows = db.query("SELECT * FROM files").unwrap();
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

        assert_eq!(
            restored_data.len(),
            large_data.len(),
            "blob size should match"
        );
        assert_eq!(restored_data, large_data, "blob data should match");
    } else {
        panic!(
            "expected Blob value, got {:?}",
            rows[0].1.get_by_name("data")
        );
    }
}
