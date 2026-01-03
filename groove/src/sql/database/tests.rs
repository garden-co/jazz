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
