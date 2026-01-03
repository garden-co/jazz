/// Unit tests for internal APIs only.
/// Most tests have been moved to tests/sql_database.rs as integration tests.

use super::*;

// ========== Tests Using Internal APIs ==========
// These tests use private methods or #[cfg(test)] methods and must remain as unit tests.

#[test]
fn table_rows_object_created() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Table rows object should exist (uses internal API)
    let rows_id = db.table_rows_object_id("users");
    assert!(rows_id.is_some());
}

#[test]
fn index_object_created() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)").unwrap();

    // Index object should exist (uses internal API)
    let key = IndexKey::new("posts", "author");
    let index_id = db.index_object_id(&key);
    assert!(index_id.is_some());
}

#[test]
fn table_rows_updates_on_insert() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Uses private read_table_rows method
    let table_rows = db.read_table_rows("users").unwrap();
    assert!(table_rows.is_empty());

    db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();

    let table_rows = db.read_table_rows("users").unwrap();
    assert_eq!(table_rows.len(), 1);
}

#[test]
fn table_rows_updates_on_delete() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    let id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Uses private read_table_rows method
    let table_rows = db.read_table_rows("users").unwrap();
    assert_eq!(table_rows.len(), 1);

    db.delete("users", id).unwrap();

    let table_rows = db.read_table_rows("users").unwrap();
    assert!(table_rows.is_empty());
}

// ========== Incremental Query Tests ==========

#[test]
fn incremental_query_basic() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)").unwrap();

    // Create an incremental query before any data
    let query = db.incremental_query("SELECT * FROM users").unwrap();
    assert!(query.rows().is_empty());

    // Insert some data
    db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)").unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)").unwrap();

    // Query should now return both rows
    let rows = query.rows();
    assert_eq!(rows.len(), 2);
}

#[test]
fn incremental_query_with_filter() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)").unwrap();

    // Create an incremental query with a WHERE clause
    let query = db.incremental_query("SELECT * FROM users WHERE active = true").unwrap();

    // Insert some data
    db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)").unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)").unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Carol', true)").unwrap();

    // Query should only return active users
    let rows = query.rows();
    assert_eq!(rows.len(), 2);
}

#[test]
fn incremental_query_update_enters_filter() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)").unwrap();

    // Create an incremental query for active users
    let query = db.incremental_query("SELECT * FROM users WHERE active = true").unwrap();

    // Insert an inactive user
    let id = match db.execute("INSERT INTO users (name, active) VALUES ('Alice', false)").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Should not be in results
    assert!(query.rows().is_empty());

    // Activate the user
    db.update("users", id, &[("active", Value::Bool(true))]).unwrap();

    // Should now appear in results
    let rows = query.rows();
    assert_eq!(rows.len(), 1);
}

#[test]
fn incremental_query_update_leaves_filter() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)").unwrap();

    // Create an incremental query for active users
    let query = db.incremental_query("SELECT * FROM users WHERE active = true").unwrap();

    // Insert an active user
    let id = match db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Should be in results
    assert_eq!(query.rows().len(), 1);

    // Deactivate the user
    db.update("users", id, &[("active", Value::Bool(false))]).unwrap();

    // Should no longer be in results
    assert!(query.rows().is_empty());
}

#[test]
fn incremental_query_delete() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    let query = db.incremental_query("SELECT * FROM users").unwrap();

    let id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    assert_eq!(query.rows().len(), 1);

    db.delete("users", id).unwrap();

    assert!(query.rows().is_empty());
}

#[test]
fn incremental_query_subscribe_delta() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    let query = db.incremental_query("SELECT * FROM users").unwrap();

    let delta_count = Arc::new(AtomicUsize::new(0));
    let delta_count_clone = delta_count.clone();

    let _listener_id = query.subscribe_delta(Box::new(move |delta| {
        delta_count_clone.fetch_add(delta.len(), Ordering::SeqCst);
    }));

    // Insert triggers callback
    db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();
    assert_eq!(delta_count.load(Ordering::SeqCst), 1);

    // Another insert
    db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap();
    assert_eq!(delta_count.load(Ordering::SeqCst), 2);
}

#[test]
fn incremental_query_subscribe_rows() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    let query = db.incremental_query("SELECT * FROM users").unwrap();

    let row_count = Arc::new(AtomicUsize::new(0));
    let row_count_clone = row_count.clone();

    let _listener_id = query.subscribe(move |rows| {
        row_count_clone.store(rows.len(), Ordering::SeqCst);
    });

    // Insert triggers callback with full row set
    db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();
    assert_eq!(row_count.load(Ordering::SeqCst), 1);

    db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap();
    assert_eq!(row_count.load(Ordering::SeqCst), 2);
}

#[test]
fn incremental_query_join_basic() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)").unwrap();

    // Create JOIN query
    let query = db.incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id").unwrap();
    assert!(query.rows().is_empty());

    // Insert a user
    let user_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Still empty - no posts yet
    assert!(query.rows().is_empty());

    // Insert a post by Alice
    db.insert("posts", &["author", "title"], vec![Value::Ref(user_id), Value::String("Hello World".to_string())]).unwrap();

    // Now we should have one joined row
    let rows = query.rows();
    assert_eq!(rows.len(), 1);
}

#[test]
fn incremental_query_join_left_table_change() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)").unwrap();

    // Insert a user first
    let user_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Create JOIN query
    let query = db.incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id").unwrap();

    // Insert multiple posts
    db.insert("posts", &["author", "title"], vec![Value::Ref(user_id), Value::String("Post 1".to_string())]).unwrap();
    db.insert("posts", &["author", "title"], vec![Value::Ref(user_id), Value::String("Post 2".to_string())]).unwrap();

    let rows = query.rows();
    assert_eq!(rows.len(), 2);
}

#[test]
fn incremental_query_join_right_table_change() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)").unwrap();

    // Insert a user
    let user_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Insert a post
    db.insert("posts", &["author", "title"], vec![Value::Ref(user_id), Value::String("Hello".to_string())]).unwrap();

    // Create JOIN query after initial data
    let query = db.incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id").unwrap();

    // Track changes via delta subscription
    let delta_count = Arc::new(AtomicUsize::new(0));
    let delta_count_clone = delta_count.clone();

    let _listener = query.subscribe_delta(Box::new(move |delta| {
        delta_count_clone.fetch_add(delta.len(), Ordering::SeqCst);
    }));

    // Updating the right table (users) should trigger notification
    // because the joined row contains data from that table
    db.update("users", user_id, &[("name", Value::String("Alice Updated".to_string()))]).unwrap();

    // The join should have been notified of the right table change
    assert!(delta_count.load(Ordering::SeqCst) > 0);
}

#[test]
fn incremental_query_table_not_found() {
    let db = Database::in_memory();

    let result = db.incremental_query("SELECT * FROM nonexistent");
    assert!(matches!(result, Err(DatabaseError::TableNotFound(_))));
}

#[test]
fn incremental_query_column_not_found() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    let result = db.incremental_query("SELECT * FROM users WHERE nonexistent = 'foo'");
    assert!(matches!(result, Err(DatabaseError::ColumnNotFound(_))));
}

// ========== Policy-Filtered Query Tests ==========

#[test]
fn select_all_as_filters_by_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    // Create users table
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Create documents table with owner_id
    db.execute("CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();

    // Create users
    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Create documents
    db.insert("documents", &["title", "owner_id"], vec![
        Value::String("Alice's Doc".into()),
        Value::Ref(alice_id),
    ]).unwrap();
    db.insert("documents", &["title", "owner_id"], vec![
        Value::String("Bob's Doc".into()),
        Value::Ref(bob_id),
    ]).unwrap();

    // Without policy: both users see all documents
    let all_docs = db.select_all("documents").unwrap();
    assert_eq!(all_docs.len(), 2);

    // Add policy: owner can read their own documents
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer").unwrap();

    // Alice can only see her document
    let alice_docs = db.select_all_as("documents", alice_id).unwrap();
    assert_eq!(alice_docs.len(), 1);
    assert_eq!(alice_docs[0].values[0], Value::String("Alice's Doc".into()));

    // Bob can only see his document
    let bob_docs = db.select_all_as("documents", bob_id).unwrap();
    assert_eq!(bob_docs.len(), 1);
    assert_eq!(bob_docs[0].values[0], Value::String("Bob's Doc".into()));
}

#[test]
fn select_all_as_with_inheritance() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    // Create users
    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Create folders
    let alice_folder = db.insert("folders", &["name", "owner_id"], vec![
        Value::String("Alice's Folder".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    // Create document in Alice's folder
    db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Secret Doc".into()),
        Value::Ref(alice_folder),
    ]).unwrap();

    // Add policies
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    // Alice can see the document (via folder ownership)
    let alice_docs = db.select_all_as("documents", alice_id).unwrap();
    assert_eq!(alice_docs.len(), 1);

    // Bob cannot see the document
    let bob_docs = db.select_all_as("documents", bob_id).unwrap();
    assert_eq!(bob_docs.len(), 0);
}
