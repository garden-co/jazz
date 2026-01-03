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

// ========== Subscription Cleanup Tests ==========
// These tests use #[cfg(test)] methods: active_query_count(), active_query_count_for_table()

#[test]
fn dropping_reactive_query_removes_from_registry() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    assert_eq!(db.active_query_count(), 0);

    let query = db.reactive_query("SELECT * FROM users").unwrap();
    assert_eq!(db.active_query_count(), 1);
    assert_eq!(db.active_query_count_for_table("users"), 1);

    drop(query);

    assert_eq!(db.active_query_count(), 0);
    assert_eq!(db.active_query_count_for_table("users"), 0);
}

#[test]
fn once_consumes_and_drops_query() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();

    assert_eq!(db.active_query_count(), 0);

    let rows = db.reactive_query("SELECT * FROM users").unwrap().once();
    assert_eq!(rows.len(), 1);

    assert_eq!(db.active_query_count(), 0);
}

#[test]
fn multiple_queries_tracked_independently() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE posts (title STRING NOT NULL)").unwrap();

    assert_eq!(db.active_query_count(), 0);

    let query1 = db.reactive_query("SELECT * FROM users").unwrap();
    assert_eq!(db.active_query_count(), 1);
    assert_eq!(db.active_query_count_for_table("users"), 1);
    assert_eq!(db.active_query_count_for_table("posts"), 0);

    let query2 = db.reactive_query("SELECT * FROM users WHERE name = 'Alice'").unwrap();
    assert_eq!(db.active_query_count(), 2);
    assert_eq!(db.active_query_count_for_table("users"), 2);

    let query3 = db.reactive_query("SELECT * FROM posts").unwrap();
    assert_eq!(db.active_query_count(), 3);
    assert_eq!(db.active_query_count_for_table("users"), 2);
    assert_eq!(db.active_query_count_for_table("posts"), 1);

    drop(query1);
    assert_eq!(db.active_query_count(), 2);
    assert_eq!(db.active_query_count_for_table("users"), 1);

    drop(query2);
    assert_eq!(db.active_query_count(), 1);
    assert_eq!(db.active_query_count_for_table("users"), 0);

    drop(query3);
    assert_eq!(db.active_query_count(), 0);
}

#[test]
fn unsubscribe_callback_does_not_drop_query() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    let query = db.reactive_query("SELECT * FROM users").unwrap();
    assert_eq!(db.active_query_count(), 1);

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let listener_id = query.subscribe(Box::new(move |_rows| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
    }));

    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    assert!(query.unsubscribe(listener_id));

    assert_eq!(db.active_query_count(), 1);

    db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    assert_eq!(db.active_query_count(), 1);

    drop(query);
    assert_eq!(db.active_query_count(), 0);
}

#[test]
fn dropped_query_stops_receiving_updates() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    let call_count = Arc::new(AtomicUsize::new(0));

    {
        let query = db.reactive_query("SELECT * FROM users").unwrap();
        let call_count_clone = call_count.clone();

        let _id = query.subscribe(Box::new(move |_rows| {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
        }));

        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    assert_eq!(db.active_query_count(), 0);

    db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[test]
fn cloned_query_shares_inner_state() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    let query1 = db.reactive_query("SELECT * FROM users").unwrap();
    assert_eq!(db.active_query_count(), 1);

    let query2 = query1.clone();
    assert_eq!(db.active_query_count(), 1);

    drop(query1);
    assert_eq!(db.active_query_count(), 1);

    drop(query2);
    assert_eq!(db.active_query_count(), 0);
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
fn incremental_query_rejects_joins() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)").unwrap();

    // JOINs are not yet supported
    let result = db.incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id");
    assert!(result.is_err());
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
