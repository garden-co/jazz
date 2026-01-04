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

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    let table_rows = db.read_table_rows("users").unwrap();
    assert_eq!(table_rows.len(), 1);
    assert!(table_rows.contains(alice_id), "table_rows should contain inserted row ID");
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
    assert!(table_rows.contains(id), "table_rows should contain inserted row ID");

    db.delete("users", id).unwrap();

    let table_rows = db.read_table_rows("users").unwrap();
    assert!(table_rows.is_empty());
    assert!(!table_rows.contains(id), "table_rows should not contain deleted row ID");
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
    let names: Vec<_> = rows.iter().map(|r| &r.values[0]).collect();
    assert!(names.contains(&&Value::String("Alice".into())));
    assert!(names.contains(&&Value::String("Bob".into())));
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
    let names: Vec<_> = rows.iter().map(|r| &r.values[0]).collect();
    assert!(names.contains(&&Value::String("Alice".into())));
    assert!(names.contains(&&Value::String("Carol".into())));
    assert!(!names.contains(&&Value::String("Bob".into())));
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
    assert_eq!(rows[0].values[0], Value::String("Alice".into()));
    assert_eq!(rows[0].values[1], Value::Bool(true));
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
    let rows = query.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], Value::String("Alice".into()));
    assert_eq!(rows[0].values[1], Value::Bool(true));

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

    let rows = query.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], Value::String("Alice".into()));
    assert_eq!(rows[0].id, id);

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
    assert_eq!(rows[0].values[1], Value::String("Hello World".to_string()));
    assert_eq!(rows[0].values[2], Value::String("Alice".to_string()));
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
    // Both rows should have Alice as author
    for row in &rows {
        assert_eq!(row.values[2], Value::String("Alice".to_string()));
    }
    let titles: Vec<_> = rows.iter().map(|r| &r.values[1]).collect();
    assert!(titles.contains(&&Value::String("Post 1".to_string())));
    assert!(titles.contains(&&Value::String("Post 2".to_string())));
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
    let all_titles: Vec<_> = all_docs.iter().map(|r| &r.values[0]).collect();
    assert!(all_titles.contains(&&Value::String("Alice's Doc".into())));
    assert!(all_titles.contains(&&Value::String("Bob's Doc".into())));

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
    assert_eq!(alice_docs[0].values[0], Value::String("Secret Doc".into()));

    // Bob cannot see the document
    let bob_docs = db.select_all_as("documents", bob_id).unwrap();
    assert_eq!(bob_docs.len(), 0);
}

// ========== Write Policy Tests ==========

#[test]
fn insert_as_checks_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, author_id REFERENCES users NOT NULL)").unwrap();

    // Create users
    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Add INSERT policy: author_id must be viewer
    db.execute("CREATE POLICY ON documents FOR INSERT CHECK (@new.author_id = @viewer)").unwrap();

    // Alice can insert doc with herself as author
    let result = db.insert_as(
        "documents",
        &["title", "author_id"],
        vec![Value::String("Alice's Doc".into()), Value::Ref(alice_id)],
        alice_id,
    );
    assert!(result.is_ok(), "owner should be able to insert: {:?}", result);

    // Alice cannot insert doc with Bob as author
    let result = db.insert_as(
        "documents",
        &["title", "author_id"],
        vec![Value::String("Forged Doc".into()), Value::Ref(bob_id)],
        alice_id,
    );
    assert!(matches!(result, Err(DatabaseError::PolicyDenied { .. })),
        "should deny insert with other as author: {:?}", result);
}

#[test]
fn update_as_checks_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
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

    // Create a document owned by Alice
    let doc_id = db.insert("documents", &["title", "owner_id"], vec![
        Value::String("Original".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    // Add UPDATE policy: owner can update
    db.execute("CREATE POLICY ON documents FOR UPDATE WHERE owner_id = @viewer").unwrap();

    // Alice can update
    let result = db.update_as("documents", doc_id, &[("title", Value::String("Updated".into()))], alice_id);
    assert!(result.is_ok(), "owner should be able to update: {:?}", result);

    // Bob cannot update
    let result = db.update_as("documents", doc_id, &[("title", Value::String("Hacked".into()))], bob_id);
    assert!(matches!(result, Err(DatabaseError::PolicyDenied { .. })),
        "non-owner should not be able to update: {:?}", result);
}

#[test]
fn update_as_checks_both_where_and_check() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    let doc_id = db.insert("documents", &["title", "owner_id"], vec![
        Value::String("Original".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    // Add UPDATE policy: owner can update, but cannot change owner
    db.execute("CREATE POLICY ON documents FOR UPDATE WHERE owner_id = @viewer CHECK (@new.owner_id = @old.owner_id)").unwrap();

    // Alice can update title
    let result = db.update_as("documents", doc_id, &[("title", Value::String("New Title".into()))], alice_id);
    assert!(result.is_ok(), "should allow updating title: {:?}", result);

    // Alice cannot change owner
    let result = db.update_as("documents", doc_id, &[("owner_id", Value::Ref(bob_id))], alice_id);
    assert!(matches!(result, Err(DatabaseError::PolicyDenied { .. })),
        "should deny changing owner: {:?}", result);
}

#[test]
fn delete_as_checks_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    let doc_id = db.insert("documents", &["title", "owner_id"], vec![
        Value::String("To Delete".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    // Add DELETE policy: owner can delete
    db.execute("CREATE POLICY ON documents FOR DELETE WHERE owner_id = @viewer").unwrap();

    // Bob cannot delete
    let result = db.delete_as("documents", doc_id, bob_id);
    assert!(matches!(result, Err(DatabaseError::PolicyDenied { .. })),
        "non-owner should not be able to delete: {:?}", result);

    // Alice can delete
    let result = db.delete_as("documents", doc_id, alice_id);
    assert!(result.is_ok(), "owner should be able to delete: {:?}", result);
}

#[test]
fn delete_as_falls_back_to_update_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    let doc_id = db.insert("documents", &["title", "owner_id"], vec![
        Value::String("Deletable".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    // Only add UPDATE policy (no DELETE policy)
    db.execute("CREATE POLICY ON documents FOR UPDATE WHERE owner_id = @viewer").unwrap();

    // Bob cannot delete (UPDATE policy check fails)
    let result = db.delete_as("documents", doc_id, bob_id);
    assert!(matches!(result, Err(DatabaseError::PolicyDenied { .. })),
        "non-owner should not be able to delete via UPDATE fallback: {:?}", result);

    // Alice can delete (UPDATE policy allows it)
    let result = db.delete_as("documents", doc_id, alice_id);
    assert!(result.is_ok(), "owner should be able to delete via UPDATE fallback: {:?}", result);
}

// ========== Incremental Query with Policy Tests ==========

#[test]
fn incremental_query_as_filters_by_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
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

    // Add SELECT policy: owner can read
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer").unwrap();

    // Alice's incremental query should only see her document
    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();
    let alice_rows = alice_query.rows();
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].values[0], Value::String("Alice's Doc".into()));

    // Bob's incremental query should only see his document
    let bob_query = db.incremental_query_as("SELECT * FROM documents", bob_id).unwrap();
    let bob_rows = bob_query.rows();
    assert_eq!(bob_rows.len(), 1);
    assert_eq!(bob_rows[0].values[0], Value::String("Bob's Doc".into()));
}

#[test]
fn incremental_query_as_updates_on_insert() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Add SELECT policy: owner can read
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer").unwrap();

    // Create query before any documents
    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();
    assert!(alice_query.rows().is_empty());

    // Insert document for Alice
    db.insert("documents", &["title", "owner_id"], vec![
        Value::String("Alice's Doc".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    // Alice should see it
    let alice_rows = alice_query.rows();
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].values[0], Value::String("Alice's Doc".into()));

    // Insert document for Bob
    db.insert("documents", &["title", "owner_id"], vec![
        Value::String("Bob's Doc".into()),
        Value::Ref(bob_id),
    ]).unwrap();

    // Alice should still only see 1 document (her own)
    let alice_rows_after = alice_query.rows();
    assert_eq!(alice_rows_after.len(), 1);
    assert_eq!(alice_rows_after[0].values[0], Value::String("Alice's Doc".into()));
}

#[test]
fn incremental_query_as_combines_with_where_clause() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL, published BOOL NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Add SELECT policy
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer").unwrap();

    // Insert multiple documents for Alice
    db.insert("documents", &["title", "owner_id", "published"], vec![
        Value::String("Draft".into()),
        Value::Ref(alice_id),
        Value::Bool(false),
    ]).unwrap();
    db.insert("documents", &["title", "owner_id", "published"], vec![
        Value::String("Published".into()),
        Value::Ref(alice_id),
        Value::Bool(true),
    ]).unwrap();

    // Query with user WHERE clause combined with policy
    let query = db.incremental_query_as("SELECT * FROM documents WHERE published = true", alice_id).unwrap();
    let rows = query.rows();

    // Should only see published documents owned by Alice
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], Value::String("Published".into()));
}

#[test]
fn incremental_query_as_no_policy_allows_all() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE items (name STRING NOT NULL)").unwrap();

    db.insert("items", &["name"], vec![Value::String("Item 1".into())]).unwrap();
    db.insert("items", &["name"], vec![Value::String("Item 2".into())]).unwrap();

    // No policy - should see all items (with warning)
    let query = db.incremental_query_as("SELECT * FROM items", ObjectId::new(999)).unwrap();
    let rows = query.rows();
    assert_eq!(rows.len(), 2);
    let names: Vec<_> = rows.iter().map(|r| &r.values[0]).collect();
    assert!(names.contains(&&Value::String("Item 1".into())));
    assert!(names.contains(&&Value::String("Item 2".into())));
}

#[test]
fn incremental_query_as_or_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL, public BOOL NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Policy: owner OR public
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer OR public = true").unwrap();

    // Create documents
    db.insert("documents", &["title", "owner_id", "public"], vec![
        Value::String("Alice Private".into()),
        Value::Ref(alice_id),
        Value::Bool(false),
    ]).unwrap();
    db.insert("documents", &["title", "owner_id", "public"], vec![
        Value::String("Alice Public".into()),
        Value::Ref(alice_id),
        Value::Bool(true),
    ]).unwrap();
    db.insert("documents", &["title", "owner_id", "public"], vec![
        Value::String("Bob Private".into()),
        Value::Ref(bob_id),
        Value::Bool(false),
    ]).unwrap();

    // Alice can see: her private, her public (but not bob's private)
    // With "owner_id = @viewer OR public = true":
    // - Alice Private: owner=alice ✓
    // - Alice Public: owner=alice ✓ (also public)
    // - Bob Private: owner=bob, public=false ✗
    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();
    let alice_rows = alice_query.rows();
    assert_eq!(alice_rows.len(), 2);
    let alice_titles: Vec<_> = alice_rows.iter().map(|r| &r.values[0]).collect();
    assert!(alice_titles.contains(&&Value::String("Alice Private".into())));
    assert!(alice_titles.contains(&&Value::String("Alice Public".into())));
    assert!(!alice_titles.contains(&&Value::String("Bob Private".into())));

    // Bob can see: his private, alice's public
    // - Alice Private: owner=alice, public=false ✗
    // - Alice Public: public=true ✓
    // - Bob Private: owner=bob ✓
    let bob_query = db.incremental_query_as("SELECT * FROM documents", bob_id).unwrap();
    let bob_rows = bob_query.rows();
    assert_eq!(bob_rows.len(), 2);
    let bob_titles: Vec<_> = bob_rows.iter().map(|r| &r.values[0]).collect();
    assert!(bob_titles.contains(&&Value::String("Alice Public".into())));
    assert!(bob_titles.contains(&&Value::String("Bob Private".into())));
    assert!(!bob_titles.contains(&&Value::String("Alice Private".into())));
}

#[test]
fn incremental_query_as_inherits_flattened_to_join() {
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
    let bob_folder = db.insert("folders", &["name", "owner_id"], vec![
        Value::String("Bob's Folder".into()),
        Value::Ref(bob_id),
    ]).unwrap();

    // Create documents
    db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Alice's Doc".into()),
        Value::Ref(alice_folder),
    ]).unwrap();
    db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Bob's Doc".into()),
        Value::Ref(bob_folder),
    ]).unwrap();

    // Add policies:
    // - folders: owner can read
    // - documents: inherit from folder
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    // Alice can only see her document (via folder ownership)
    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();
    let alice_rows = alice_query.rows();
    assert_eq!(alice_rows.len(), 1, "Alice should see 1 doc: {:?}", alice_rows);
    assert_eq!(alice_rows[0].values[0], Value::String("Alice's Doc".into()));

    // Bob can only see his document
    let bob_query = db.incremental_query_as("SELECT * FROM documents", bob_id).unwrap();
    let bob_rows = bob_query.rows();
    assert_eq!(bob_rows.len(), 1, "Bob should see 1 doc: {:?}", bob_rows);
    assert_eq!(bob_rows[0].values[0], Value::String("Bob's Doc".into()));
}

#[test]
fn incremental_query_as_inherits_incremental_updates() {
    use crate::sql::policy::clear_policy_warnings;
    use std::sync::atomic::{AtomicUsize, Ordering};

    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Create folder and policy before documents
    let alice_folder = db.insert("folders", &["name", "owner_id"], vec![
        Value::String("Alice's Folder".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    // Create query before any documents
    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();
    assert!(alice_query.rows().is_empty());

    // Track changes via subscription
    let change_count = Arc::new(AtomicUsize::new(0));
    let change_count_clone = change_count.clone();
    let _listener = alice_query.subscribe_delta(Box::new(move |delta| {
        change_count_clone.fetch_add(delta.len(), Ordering::SeqCst);
    }));

    // Insert document into Alice's folder - should trigger update
    db.insert("documents", &["title", "folder_id"], vec![
        Value::String("New Doc".into()),
        Value::Ref(alice_folder),
    ]).unwrap();

    // Query should now have one row
    let rows = alice_query.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], Value::String("New Doc".into()));
    // And we should have received a delta
    assert!(change_count.load(Ordering::SeqCst) > 0, "Should have received delta notification");
}

#[test]
fn incremental_query_as_inherits_folder_ownership_change() {
    // Test that changing folder ownership propagates through INHERITS policy
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Create folder owned by Alice
    let folder_id = db.insert("folders", &["name", "owner_id"], vec![
        Value::String("Shared Folder".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    // Create document in that folder
    db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Important Doc".into()),
        Value::Ref(folder_id),
    ]).unwrap();

    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    // Alice can see the document
    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();
    let alice_rows = alice_query.rows();
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].values[0], Value::String("Important Doc".into()));

    // Bob cannot see the document
    let bob_query = db.incremental_query_as("SELECT * FROM documents", bob_id).unwrap();
    assert_eq!(bob_query.rows().len(), 0);

    // Transfer folder ownership to Bob
    db.update("folders", folder_id, &[("owner_id", Value::Ref(bob_id))]).unwrap();

    // Now Alice should NOT see the document (folder no longer hers)
    // NOTE: This tests incremental propagation through the JOIN
    let alice_rows_after = alice_query.rows();
    assert_eq!(alice_rows_after.len(), 0, "Alice should no longer see doc after folder transfer");

    // Bob should now see the document
    let bob_rows_after = bob_query.rows();
    assert_eq!(bob_rows_after.len(), 1, "Bob should now see doc after folder transfer");
    assert_eq!(bob_rows_after[0].values[0], Value::String("Important Doc".into()));
}

#[test]
fn incremental_query_as_nested_inherits_chain() {
    // Test nested INHERITS chain (2-hop):
    // - documents: INHERITS SELECT FROM folder_id
    // - folders: INHERITS SELECT FROM workspace_id
    // - workspaces: owner_id = @viewer
    //
    // This creates a chain: documents → folders → workspaces
    // Documents should be visible if they're in a folder in a workspace owned by the viewer.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE workspaces (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's workspace with a folder and document
    let alice_workspace_id = db.insert("workspaces", &["name", "owner_id"], vec![
        Value::String("Alice's Workspace".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    let alice_folder_id = db.insert("folders", &["name", "workspace_id"], vec![
        Value::String("Alice's Folder".into()),
        Value::Ref(alice_workspace_id),
    ]).unwrap();

    let alice_doc_id = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Alice's Doc".into()),
        Value::Ref(alice_folder_id),
    ]).unwrap();

    // Bob's workspace with a folder and document
    let bob_workspace_id = db.insert("workspaces", &["name", "owner_id"], vec![
        Value::String("Bob's Workspace".into()),
        Value::Ref(bob_id),
    ]).unwrap();

    let bob_folder_id = db.insert("folders", &["name", "workspace_id"], vec![
        Value::String("Bob's Folder".into()),
        Value::Ref(bob_workspace_id),
    ]).unwrap();

    let _bob_doc_id = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Bob's Doc".into()),
        Value::Ref(bob_folder_id),
    ]).unwrap();

    // Set up 2-hop chain: documents -> folders -> workspaces -> owner_id
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    // Alice should only see her document (via folder → workspace → owner)
    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id)
        .expect("Nested INHERITS chain should work");
    let alice_docs = alice_query.rows();

    assert_eq!(alice_docs.len(), 1, "Alice should see 1 document through the chain");
    assert_eq!(alice_docs[0].id, alice_doc_id, "Alice should see her own document");
    assert_eq!(alice_docs[0].values[0], Value::String("Alice's Doc".into()));

    // Bob should only see his document
    let bob_query = db.incremental_query_as("SELECT * FROM documents", bob_id)
        .expect("Nested INHERITS chain should work");
    let bob_docs = bob_query.rows();

    assert_eq!(bob_docs.len(), 1, "Bob should see 1 document through the chain");
    assert_eq!(bob_docs[0].values[0], Value::String("Bob's Doc".into()));
}

#[test]
fn incremental_query_as_inherits_multiple_docs_same_folder() {
    // Test INHERITS with multiple documents in the same folder
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    let folder_id = db.insert("folders", &["name", "owner_id"], vec![
        Value::String("Alice's Folder".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    // Insert multiple documents
    db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Doc 1".into()),
        Value::Ref(folder_id),
    ]).unwrap();
    db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Doc 2".into()),
        Value::Ref(folder_id),
    ]).unwrap();
    db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Doc 3".into()),
        Value::Ref(folder_id),
    ]).unwrap();

    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();
    let rows = alice_query.rows();

    assert_eq!(rows.len(), 3, "Alice should see all 3 documents in her folder");
    let titles: Vec<_> = rows.iter().map(|r| &r.values[0]).collect();
    assert!(titles.contains(&&Value::String("Doc 1".into())));
    assert!(titles.contains(&&Value::String("Doc 2".into())));
    assert!(titles.contains(&&Value::String("Doc 3".into())));
}

#[test]
fn incremental_query_as_inherits_delete_propagates() {
    // Test that deleting a folder removes access to its documents
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    let folder_id = db.insert("folders", &["name", "owner_id"], vec![
        Value::String("Temp Folder".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Orphan Doc".into()),
        Value::Ref(folder_id),
    ]).unwrap();

    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();

    // Initially Alice can see the document
    let initial_rows = alice_query.rows();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(initial_rows[0].values[0], Value::String("Orphan Doc".into()));

    // Delete the folder
    db.delete("folders", folder_id).unwrap();

    // Now the document should not be visible (no matching folder in JOIN)
    let after_delete = alice_query.rows();
    assert_eq!(after_delete.len(), 0, "Document should not be visible after folder deletion");
}

#[test]
fn incremental_query_as_self_referential_recursive_inherits() {
    // Test true recursive policies with self-referential table structure:
    // folders can have parent folders (unlimited depth), permissions inherit up the tree.
    //
    // Structure:
    // - folders: id, name, parent_id (REFERENCES folders), owner_id (REFERENCES users)
    // - Policy: owner_id = @viewer OR INHERITS SELECT FROM parent_id
    //
    // This uses RecursiveFilter with fixpoint iteration to handle arbitrary depth.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    // Self-referential: parent_id references the same table
    db.execute("CREATE TABLE folders (name STRING NOT NULL, parent_id REFERENCES folders, owner_id REFERENCES users)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Create folder hierarchy owned by Alice:
    // root (owned by Alice) -> child -> grandchild -> great_grandchild
    let root_id = db.insert("folders", &["name", "owner_id"], vec![
        Value::String("Root".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    let child_id = db.insert("folders", &["name", "parent_id"], vec![
        Value::String("Child".into()),
        Value::Ref(root_id),
    ]).unwrap();

    let grandchild_id = db.insert("folders", &["name", "parent_id"], vec![
        Value::String("Grandchild".into()),
        Value::Ref(child_id),
    ]).unwrap();

    let great_grandchild_id = db.insert("folders", &["name", "parent_id"], vec![
        Value::String("GreatGrandchild".into()),
        Value::Ref(grandchild_id),
    ]).unwrap();

    // Policy: owner OR inherit from parent (recursive!)
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer OR INHERITS SELECT FROM parent_id").unwrap();

    // Create incremental query - this should now work with RecursiveFilter
    let query = db.incremental_query_as("SELECT * FROM folders", alice_id)
        .expect("Self-referential INHERITS should work with RecursiveFilter");

    // Alice should see all 4 folders (root via owner_id, others via inheritance)
    let alice_rows = query.rows();
    assert_eq!(alice_rows.len(), 4, "Alice should see all 4 folders through recursive inheritance");

    // Verify specific folders are visible
    let folder_ids: Vec<_> = alice_rows.iter().map(|r| r.id).collect();
    assert!(folder_ids.contains(&root_id), "Root should be visible (owned by Alice)");
    assert!(folder_ids.contains(&child_id), "Child should be visible (inherits from root)");
    assert!(folder_ids.contains(&grandchild_id), "Grandchild should be visible (inherits from child)");
    assert!(folder_ids.contains(&great_grandchild_id), "GreatGrandchild should be visible (inherits from grandchild)");

    // Bob should see no folders (doesn't own any, no inheritance path)
    let bob_query = db.incremental_query_as("SELECT * FROM folders", bob_id)
        .expect("Query should work for Bob too");
    let bob_rows = bob_query.rows();
    assert_eq!(bob_rows.len(), 0, "Bob should see no folders");

    // Test incremental update: create a new folder under grandchild
    let new_folder_id = db.insert("folders", &["name", "parent_id"], vec![
        Value::String("NewFolder".into()),
        Value::Ref(grandchild_id),
    ]).unwrap();

    // Alice should now see 5 folders
    let alice_rows_after = query.rows();
    assert_eq!(alice_rows_after.len(), 5, "Alice should now see 5 folders after insert");
    let folder_ids_after: Vec<_> = alice_rows_after.iter().map(|r| r.id).collect();
    assert!(folder_ids_after.contains(&new_folder_id), "New folder should be visible via inheritance");
}

#[test]
fn incremental_query_as_pure_recursive_inherits_returns_nothing() {
    // Test PURE recursive policy (no base predicate) - this returns no rows
    // because there's no anchor for the recursion.
    //
    // Policy: ONLY INHERITS SELECT FROM parent_id (no owner_id check)
    // This means: "you can see a folder if you can see its parent"
    // But with no base case (like owner_id = @viewer), the recursion never starts.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, parent_id REFERENCES folders, owner_id REFERENCES users)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Create root folder with owner
    let root_id = db.insert("folders", &["name", "owner_id"], vec![
        Value::String("Root".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    db.insert("folders", &["name", "parent_id"], vec![
        Value::String("Child".into()),
        Value::Ref(root_id),
    ]).unwrap();

    // Pure INHERITS policy (no simple predicate fallback)
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM parent_id").unwrap();

    // This should work but return no rows since there's no base case
    let query = db.incremental_query_as("SELECT * FROM folders", alice_id)
        .expect("Pure INHERITS should create a query (but return no rows)");

    let rows = query.rows();
    assert_eq!(rows.len(), 0, "Pure INHERITS with no base predicate should return no rows");
}

#[test]
fn incremental_query_as_3_hop_inherits_chain() {
    // Test 3-hop INHERITS chain:
    // - documents: INHERITS SELECT FROM folder_id
    // - folders: INHERITS SELECT FROM workspace_id
    // - workspaces: INHERITS SELECT FROM org_id
    // - organizations: owner_id = @viewer
    //
    // Chain: documents → folders → workspaces → organizations → owner_id
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE organizations (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE workspaces (name STRING NOT NULL, org_id REFERENCES organizations NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's hierarchy: org → workspace → folder → document
    let alice_org_id = db.insert("organizations", &["name", "owner_id"], vec![
        Value::String("Alice's Org".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    let alice_workspace_id = db.insert("workspaces", &["name", "org_id"], vec![
        Value::String("Alice's Workspace".into()),
        Value::Ref(alice_org_id),
    ]).unwrap();

    let alice_folder_id = db.insert("folders", &["name", "workspace_id"], vec![
        Value::String("Alice's Folder".into()),
        Value::Ref(alice_workspace_id),
    ]).unwrap();

    let alice_doc_id = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Alice's Doc".into()),
        Value::Ref(alice_folder_id),
    ]).unwrap();

    // Bob's hierarchy: org → workspace → folder → document
    let bob_org_id = db.insert("organizations", &["name", "owner_id"], vec![
        Value::String("Bob's Org".into()),
        Value::Ref(bob_id),
    ]).unwrap();

    let bob_workspace_id = db.insert("workspaces", &["name", "org_id"], vec![
        Value::String("Bob's Workspace".into()),
        Value::Ref(bob_org_id),
    ]).unwrap();

    let bob_folder_id = db.insert("folders", &["name", "workspace_id"], vec![
        Value::String("Bob's Folder".into()),
        Value::Ref(bob_workspace_id),
    ]).unwrap();

    let _bob_doc_id = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Bob's Doc".into()),
        Value::Ref(bob_folder_id),
    ]).unwrap();

    // Set up 3-hop chain: documents -> folders -> workspaces -> organizations -> owner_id
    db.execute("CREATE POLICY ON organizations FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE INHERITS SELECT FROM org_id").unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    // Alice should only see her document (via folder → workspace → org → owner)
    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id)
        .expect("3-hop INHERITS chain should work");
    let alice_docs = alice_query.rows();

    assert_eq!(alice_docs.len(), 1, "Alice should see 1 document through 3-hop chain");
    assert_eq!(alice_docs[0].id, alice_doc_id, "Alice should see her own document");
    assert_eq!(alice_docs[0].values[0], Value::String("Alice's Doc".into()));

    // Bob should only see his document
    let bob_query = db.incremental_query_as("SELECT * FROM documents", bob_id)
        .expect("3-hop INHERITS chain should work");
    let bob_docs = bob_query.rows();

    assert_eq!(bob_docs.len(), 1, "Bob should see 1 document through 3-hop chain");
    assert_eq!(bob_docs[0].values[0], Value::String("Bob's Doc".into()));
}

#[test]
fn incremental_query_as_3_hop_chain_delta_from_org_update() {
    // Test delta propagation from the furthest table (organizations) in a 3-hop chain.
    // When an organization's owner changes, documents visibility should update.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE organizations (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE workspaces (name STRING NOT NULL, org_id REFERENCES organizations NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Start with Alice owning the org
    let org_id = db.insert("organizations", &["name", "owner_id"], vec![
        Value::String("Test Org".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    let workspace_id = db.insert("workspaces", &["name", "org_id"], vec![
        Value::String("Test Workspace".into()),
        Value::Ref(org_id),
    ]).unwrap();

    let folder_id = db.insert("folders", &["name", "workspace_id"], vec![
        Value::String("Test Folder".into()),
        Value::Ref(workspace_id),
    ]).unwrap();

    let doc_id = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Test Doc".into()),
        Value::Ref(folder_id),
    ]).unwrap();

    // Set up 3-hop chain policies
    db.execute("CREATE POLICY ON organizations FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE INHERITS SELECT FROM org_id").unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    // Create incremental queries
    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();
    let bob_query = db.incremental_query_as("SELECT * FROM documents", bob_id).unwrap();

    // Initially, Alice sees the document, Bob doesn't
    assert_eq!(alice_query.rows().len(), 1, "Alice should see doc initially");
    assert_eq!(alice_query.rows()[0].id, doc_id);
    assert_eq!(bob_query.rows().len(), 0, "Bob should not see doc initially");

    // Transfer org ownership from Alice to Bob
    db.update("organizations", org_id, &[("owner_id", Value::Ref(bob_id))]).unwrap();

    // Now Bob should see it, Alice shouldn't
    let alice_rows_after = alice_query.rows();
    let bob_rows_after = bob_query.rows();

    assert_eq!(alice_rows_after.len(), 0, "Alice should not see doc after org transfer");
    assert_eq!(bob_rows_after.len(), 1, "Bob should see doc after org transfer");
    assert_eq!(bob_rows_after[0].id, doc_id);
    assert_eq!(bob_rows_after[0].values[0], Value::String("Test Doc".into()));
}

#[test]
fn incremental_query_as_3_hop_chain_delta_from_workspace_update() {
    // Test delta propagation when an intermediate table (workspace) changes.
    // Moving a workspace to a different org should update document visibility.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE organizations (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE workspaces (name STRING NOT NULL, org_id REFERENCES organizations NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's org
    let alice_org_id = db.insert("organizations", &["name", "owner_id"], vec![
        Value::String("Alice's Org".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    // Bob's org
    let bob_org_id = db.insert("organizations", &["name", "owner_id"], vec![
        Value::String("Bob's Org".into()),
        Value::Ref(bob_id),
    ]).unwrap();

    // Workspace starts in Alice's org
    let workspace_id = db.insert("workspaces", &["name", "org_id"], vec![
        Value::String("Movable Workspace".into()),
        Value::Ref(alice_org_id),
    ]).unwrap();

    let folder_id = db.insert("folders", &["name", "workspace_id"], vec![
        Value::String("Test Folder".into()),
        Value::Ref(workspace_id),
    ]).unwrap();

    let doc_id = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Test Doc".into()),
        Value::Ref(folder_id),
    ]).unwrap();

    // Set up 3-hop chain policies
    db.execute("CREATE POLICY ON organizations FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE INHERITS SELECT FROM org_id").unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();
    let bob_query = db.incremental_query_as("SELECT * FROM documents", bob_id).unwrap();

    // Initially, Alice sees the document, Bob doesn't
    assert_eq!(alice_query.rows().len(), 1, "Alice should see doc initially");
    assert_eq!(bob_query.rows().len(), 0, "Bob should not see doc initially");

    // Move workspace from Alice's org to Bob's org
    db.update("workspaces", workspace_id, &[("org_id", Value::Ref(bob_org_id))]).unwrap();

    // Now Bob should see it, Alice shouldn't
    let alice_rows_after = alice_query.rows();
    let bob_rows_after = bob_query.rows();

    assert_eq!(alice_rows_after.len(), 0, "Alice should not see doc after workspace move");
    assert_eq!(bob_rows_after.len(), 1, "Bob should see doc after workspace move");
    assert_eq!(bob_rows_after[0].id, doc_id);
    assert_eq!(bob_rows_after[0].values[0], Value::String("Test Doc".into()));
}

#[test]
fn incremental_query_as_3_hop_chain_delta_from_folder_update() {
    // Test delta propagation when the nearest joined table (folder) changes.
    // Moving a folder to a different workspace should update document visibility.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE organizations (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE workspaces (name STRING NOT NULL, org_id REFERENCES organizations NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's full hierarchy
    let alice_org_id = db.insert("organizations", &["name", "owner_id"], vec![
        Value::String("Alice's Org".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    let alice_workspace_id = db.insert("workspaces", &["name", "org_id"], vec![
        Value::String("Alice's Workspace".into()),
        Value::Ref(alice_org_id),
    ]).unwrap();

    // Bob's full hierarchy
    let bob_org_id = db.insert("organizations", &["name", "owner_id"], vec![
        Value::String("Bob's Org".into()),
        Value::Ref(bob_id),
    ]).unwrap();

    let bob_workspace_id = db.insert("workspaces", &["name", "org_id"], vec![
        Value::String("Bob's Workspace".into()),
        Value::Ref(bob_org_id),
    ]).unwrap();

    // Folder starts in Alice's workspace
    let folder_id = db.insert("folders", &["name", "workspace_id"], vec![
        Value::String("Movable Folder".into()),
        Value::Ref(alice_workspace_id),
    ]).unwrap();

    let doc_id = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Test Doc".into()),
        Value::Ref(folder_id),
    ]).unwrap();

    // Set up 3-hop chain policies
    db.execute("CREATE POLICY ON organizations FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE INHERITS SELECT FROM org_id").unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();
    let bob_query = db.incremental_query_as("SELECT * FROM documents", bob_id).unwrap();

    // Initially, Alice sees the document, Bob doesn't
    assert_eq!(alice_query.rows().len(), 1, "Alice should see doc initially");
    assert_eq!(bob_query.rows().len(), 0, "Bob should not see doc initially");

    // Move folder from Alice's workspace to Bob's workspace
    db.update("folders", folder_id, &[("workspace_id", Value::Ref(bob_workspace_id))]).unwrap();

    // Now Bob should see it, Alice shouldn't
    let alice_rows_after = alice_query.rows();
    let bob_rows_after = bob_query.rows();

    assert_eq!(alice_rows_after.len(), 0, "Alice should not see doc after folder move");
    assert_eq!(bob_rows_after.len(), 1, "Bob should see doc after folder move");
    assert_eq!(bob_rows_after[0].id, doc_id);
    assert_eq!(bob_rows_after[0].values[0], Value::String("Test Doc".into()));
}

#[test]
fn incremental_query_as_3_hop_chain_new_document_insert() {
    // Test that inserting a new document in a 3-hop chain correctly updates visibility.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE organizations (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE workspaces (name STRING NOT NULL, org_id REFERENCES organizations NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's hierarchy (no documents yet)
    let alice_org_id = db.insert("organizations", &["name", "owner_id"], vec![
        Value::String("Alice's Org".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    let alice_workspace_id = db.insert("workspaces", &["name", "org_id"], vec![
        Value::String("Alice's Workspace".into()),
        Value::Ref(alice_org_id),
    ]).unwrap();

    let alice_folder_id = db.insert("folders", &["name", "workspace_id"], vec![
        Value::String("Alice's Folder".into()),
        Value::Ref(alice_workspace_id),
    ]).unwrap();

    // Set up 3-hop chain policies
    db.execute("CREATE POLICY ON organizations FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE INHERITS SELECT FROM org_id").unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();
    let bob_query = db.incremental_query_as("SELECT * FROM documents", bob_id).unwrap();

    // Initially, no documents
    assert_eq!(alice_query.rows().len(), 0, "Alice should see 0 docs initially");
    assert_eq!(bob_query.rows().len(), 0, "Bob should see 0 docs initially");

    // Insert a document in Alice's folder
    let new_doc_id = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("New Alice Doc".into()),
        Value::Ref(alice_folder_id),
    ]).unwrap();

    // Alice should see the new document, Bob shouldn't
    let alice_rows_after = alice_query.rows();
    let bob_rows_after = bob_query.rows();

    assert_eq!(alice_rows_after.len(), 1, "Alice should see new doc after insert");
    assert_eq!(alice_rows_after[0].id, new_doc_id);
    assert_eq!(alice_rows_after[0].values[0], Value::String("New Alice Doc".into()));
    assert_eq!(bob_rows_after.len(), 0, "Bob should still see 0 docs");
}

#[test]
fn incremental_query_as_3_hop_chain_with_filter() {
    // Test 3-hop chain with a WHERE clause filter on the source table.
    // This tests that filters are correctly applied in chain queries.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE organizations (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE workspaces (name STRING NOT NULL, org_id REFERENCES organizations NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, archived BOOL NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's hierarchy
    let alice_org_id = db.insert("organizations", &["name", "owner_id"], vec![
        Value::String("Alice's Org".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    let alice_workspace_id = db.insert("workspaces", &["name", "org_id"], vec![
        Value::String("Alice's Workspace".into()),
        Value::Ref(alice_org_id),
    ]).unwrap();

    let alice_folder_id = db.insert("folders", &["name", "workspace_id"], vec![
        Value::String("Alice's Folder".into()),
        Value::Ref(alice_workspace_id),
    ]).unwrap();

    // Create active and archived documents
    let active_doc_id = db.insert("documents", &["title", "archived", "folder_id"], vec![
        Value::String("Active Doc".into()),
        Value::Bool(false),
        Value::Ref(alice_folder_id),
    ]).unwrap();

    let _archived_doc_id = db.insert("documents", &["title", "archived", "folder_id"], vec![
        Value::String("Archived Doc".into()),
        Value::Bool(true),
        Value::Ref(alice_folder_id),
    ]).unwrap();

    // Set up 3-hop chain policies
    db.execute("CREATE POLICY ON organizations FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE INHERITS SELECT FROM org_id").unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    // Query with a filter: only non-archived documents
    let alice_query = db.incremental_query_as("SELECT * FROM documents WHERE archived = false", alice_id).unwrap();
    let rows = alice_query.rows();

    // Alice should only see the active document (filter applied)
    assert_eq!(rows.len(), 1, "Alice should see only 1 non-archived doc");
    assert_eq!(rows[0].id, active_doc_id);
    assert_eq!(rows[0].values[0], Value::String("Active Doc".into()));
    assert_eq!(rows[0].values[1], Value::Bool(false));
}

#[test]
fn policy_chain_or_condition_with_inherits() {
    // Test OR policy at intermediate level:
    // - documents: INHERITS SELECT FROM folder_id
    // - folders: owner_id = @viewer OR INHERITS SELECT FROM workspace_id
    // - workspaces: owner_id = @viewer
    //
    // Access is granted if ANY level in the chain matches:
    // - folders.owner_id = @viewer, OR
    // - workspaces.owner_id = @viewer
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE workspaces (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users, workspace_id REFERENCES workspaces NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's workspace
    let alice_ws_id = db.insert("workspaces", &["name", "owner_id"], vec![
        Value::String("Alice's Workspace".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    // Bob's workspace
    let bob_ws_id = db.insert("workspaces", &["name", "owner_id"], vec![
        Value::String("Bob's Workspace".into()),
        Value::Ref(bob_id),
    ]).unwrap();

    // Folder in Alice's workspace (no direct owner)
    let folder_in_alice_ws = db.insert("folders", &["name", "workspace_id"], vec![
        Value::String("Folder in Alice WS".into()),
        Value::Ref(alice_ws_id),
    ]).unwrap();

    // Folder owned by Alice but in Bob's workspace
    let alice_folder_in_bob_ws = db.insert("folders", &["name", "owner_id", "workspace_id"], vec![
        Value::String("Alice's Folder in Bob WS".into()),
        Value::Ref(alice_id),
        Value::Ref(bob_ws_id),
    ]).unwrap();

    // Folder owned by Bob in Bob's workspace
    let bob_folder = db.insert("folders", &["name", "owner_id", "workspace_id"], vec![
        Value::String("Bob's Folder".into()),
        Value::Ref(bob_id),
        Value::Ref(bob_ws_id),
    ]).unwrap();

    // Documents in each folder
    let doc_in_alice_ws = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Doc in Alice WS".into()),
        Value::Ref(folder_in_alice_ws),
    ]).unwrap();

    // This doc is in a folder Alice owns but in Bob's workspace.
    // Alice can see it via the OR condition: folders.owner_id = @viewer
    let doc_in_alice_folder = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Doc in Alice's Folder".into()),
        Value::Ref(alice_folder_in_bob_ws),
    ]).unwrap();

    let _doc_in_bob_folder = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Doc in Bob's Folder".into()),
        Value::Ref(bob_folder),
    ]).unwrap();

    // Set up policies
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer OR INHERITS SELECT FROM workspace_id").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    // Alice sees documents via both paths:
    // 1. doc → folder → workspace → owner_id = Alice (INHERITS path)
    // 2. doc → folder → owner_id = Alice (OR path at folder level)
    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id)
        .expect("Policy chain should work");
    let alice_docs = alice_query.rows();

    // Alice sees 2 documents:
    // - Doc in her workspace (via INHERITS to workspace owner)
    // - Doc in folder she owns (via OR condition on folder owner)
    assert_eq!(alice_docs.len(), 2, "Alice should see 2 documents");
    let alice_doc_ids: std::collections::HashSet<_> = alice_docs.iter().map(|r| r.id).collect();
    assert!(alice_doc_ids.contains(&doc_in_alice_ws), "Alice should see doc in her workspace");
    assert!(alice_doc_ids.contains(&doc_in_alice_folder), "Alice should see doc in folder she owns");
}

#[test]
fn policy_chain_multiple_viewers_concurrent() {
    // Test that multiple viewers have independent incremental queries
    // that update correctly when data changes.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE orgs (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE projects (name STRING NOT NULL, org_id REFERENCES orgs NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let charlie_id = match db.execute("INSERT INTO users (name) VALUES ('Charlie')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Each user has their own org
    let alice_org = db.insert("orgs", &["name", "owner_id"], vec![
        Value::String("Alice Org".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    let bob_org = db.insert("orgs", &["name", "owner_id"], vec![
        Value::String("Bob Org".into()),
        Value::Ref(bob_id),
    ]).unwrap();

    // Projects in each org
    let alice_project = db.insert("projects", &["name", "org_id"], vec![
        Value::String("Alice Project".into()),
        Value::Ref(alice_org),
    ]).unwrap();

    let bob_project = db.insert("projects", &["name", "org_id"], vec![
        Value::String("Bob Project".into()),
        Value::Ref(bob_org),
    ]).unwrap();

    // Set up 2-hop chain policies
    db.execute("CREATE POLICY ON orgs FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON projects FOR SELECT WHERE INHERITS SELECT FROM org_id").unwrap();

    // Create queries for all three viewers
    let alice_query = db.incremental_query_as("SELECT * FROM projects", alice_id).unwrap();
    let bob_query = db.incremental_query_as("SELECT * FROM projects", bob_id).unwrap();
    let charlie_query = db.incremental_query_as("SELECT * FROM projects", charlie_id).unwrap();

    // Initially: Alice sees 1, Bob sees 1, Charlie sees 0
    assert_eq!(alice_query.rows().len(), 1, "Alice should see her project");
    assert_eq!(alice_query.rows()[0].id, alice_project);
    assert_eq!(bob_query.rows().len(), 1, "Bob should see his project");
    assert_eq!(bob_query.rows()[0].id, bob_project);
    assert_eq!(charlie_query.rows().len(), 0, "Charlie should see no projects");

    // Transfer Alice's org to Charlie
    db.update("orgs", alice_org, &[("owner_id", Value::Ref(charlie_id))]).unwrap();

    // Now: Alice sees 0, Bob sees 1, Charlie sees 1
    assert_eq!(alice_query.rows().len(), 0, "Alice should no longer see her project");
    assert_eq!(bob_query.rows().len(), 1, "Bob should still see his project");
    assert_eq!(charlie_query.rows().len(), 1, "Charlie should now see Alice's project");
    assert_eq!(charlie_query.rows()[0].id, alice_project);

    // Add a new project to Bob's org
    let new_bob_project = db.insert("projects", &["name", "org_id"], vec![
        Value::String("New Bob Project".into()),
        Value::Ref(bob_org),
    ]).unwrap();

    // Bob should now see 2 projects
    let bob_rows = bob_query.rows();
    assert_eq!(bob_rows.len(), 2, "Bob should now see 2 projects");
    let bob_project_ids: Vec<_> = bob_rows.iter().map(|r| r.id).collect();
    assert!(bob_project_ids.contains(&bob_project));
    assert!(bob_project_ids.contains(&new_bob_project));

    // Alice and Charlie unchanged
    assert_eq!(alice_query.rows().len(), 0);
    assert_eq!(charlie_query.rows().len(), 1);
}

#[test]
fn policy_chain_insert_intermediate_row() {
    // Test that inserting an intermediate row (folder) makes its children visible.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE workspaces (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's workspace
    let alice_ws = db.insert("workspaces", &["name", "owner_id"], vec![
        Value::String("Alice WS".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    // Set up policies before creating folder/docs
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    // Create query before any folders exist
    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();
    assert_eq!(alice_query.rows().len(), 0, "No documents yet");

    // Now create a folder
    let folder = db.insert("folders", &["name", "workspace_id"], vec![
        Value::String("New Folder".into()),
        Value::Ref(alice_ws),
    ]).unwrap();

    // Still no documents
    assert_eq!(alice_query.rows().len(), 0, "Still no documents");

    // Add a document to the new folder
    let doc = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("New Doc".into()),
        Value::Ref(folder),
    ]).unwrap();

    // Now Alice should see the document
    let rows = alice_query.rows();
    assert_eq!(rows.len(), 1, "Alice should see the new document");
    assert_eq!(rows[0].id, doc);
    assert_eq!(rows[0].values[0], Value::String("New Doc".into()));
}

#[test]
fn policy_chain_delete_intermediate_row() {
    // Test that deleting an intermediate row (folder) makes its children invisible.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE workspaces (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)").unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    let alice_ws = db.insert("workspaces", &["name", "owner_id"], vec![
        Value::String("Alice WS".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    let folder = db.insert("folders", &["name", "workspace_id"], vec![
        Value::String("Folder".into()),
        Value::Ref(alice_ws),
    ]).unwrap();

    let doc = db.insert("documents", &["title", "folder_id"], vec![
        Value::String("Doc".into()),
        Value::Ref(folder),
    ]).unwrap();

    // Set up policies
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    let alice_query = db.incremental_query_as("SELECT * FROM documents", alice_id).unwrap();

    // Initially Alice sees the document
    assert_eq!(alice_query.rows().len(), 1, "Alice should see doc initially");
    assert_eq!(alice_query.rows()[0].id, doc);

    // Delete the folder (this should cascade to make doc invisible)
    db.delete("folders", folder).unwrap();

    // Alice should no longer see the document
    // Note: The document still exists but its folder reference is now dangling
    // The join fails, so it's not visible
    assert_eq!(alice_query.rows().len(), 0, "Alice should not see doc after folder deletion");
}

#[test]
fn policy_chain_4_hop_deep() {
    // Test a 4-hop INHERITS chain:
    // files → folders → projects → orgs → owner_id
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE orgs (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE projects (name STRING NOT NULL, org_id REFERENCES orgs NOT NULL)").unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, project_id REFERENCES projects NOT NULL)").unwrap();
    db.execute("CREATE TABLE files (name STRING NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's hierarchy
    let alice_org = db.insert("orgs", &["name", "owner_id"], vec![
        Value::String("Alice Org".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    let alice_project = db.insert("projects", &["name", "org_id"], vec![
        Value::String("Alice Project".into()),
        Value::Ref(alice_org),
    ]).unwrap();

    let alice_folder = db.insert("folders", &["name", "project_id"], vec![
        Value::String("Alice Folder".into()),
        Value::Ref(alice_project),
    ]).unwrap();

    let alice_file = db.insert("files", &["name", "folder_id"], vec![
        Value::String("Alice File".into()),
        Value::Ref(alice_folder),
    ]).unwrap();

    // Bob's hierarchy
    let bob_org = db.insert("orgs", &["name", "owner_id"], vec![
        Value::String("Bob Org".into()),
        Value::Ref(bob_id),
    ]).unwrap();

    let bob_project = db.insert("projects", &["name", "org_id"], vec![
        Value::String("Bob Project".into()),
        Value::Ref(bob_org),
    ]).unwrap();

    let bob_folder = db.insert("folders", &["name", "project_id"], vec![
        Value::String("Bob Folder".into()),
        Value::Ref(bob_project),
    ]).unwrap();

    let _bob_file = db.insert("files", &["name", "folder_id"], vec![
        Value::String("Bob File".into()),
        Value::Ref(bob_folder),
    ]).unwrap();

    // Set up 4-hop chain policies
    db.execute("CREATE POLICY ON orgs FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON projects FOR SELECT WHERE INHERITS SELECT FROM org_id").unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM project_id").unwrap();
    db.execute("CREATE POLICY ON files FOR SELECT WHERE INHERITS SELECT FROM folder_id").unwrap();

    // Alice should only see her file
    let alice_query = db.incremental_query_as("SELECT * FROM files", alice_id)
        .expect("4-hop chain should work");
    let alice_files = alice_query.rows();

    assert_eq!(alice_files.len(), 1, "Alice should see 1 file");
    assert_eq!(alice_files[0].id, alice_file);
    assert_eq!(alice_files[0].values[0], Value::String("Alice File".into()));

    // Bob should only see his file
    let bob_query = db.incremental_query_as("SELECT * FROM files", bob_id).unwrap();
    assert_eq!(bob_query.rows().len(), 1, "Bob should see 1 file");

    // Transfer org from Alice to Bob
    db.update("orgs", alice_org, &[("owner_id", Value::Ref(bob_id))]).unwrap();

    // Alice should see nothing now
    assert_eq!(alice_query.rows().len(), 0, "Alice should see no files after org transfer");

    // Bob should see both files now
    let bob_files = bob_query.rows();
    assert_eq!(bob_files.len(), 2, "Bob should see 2 files after org transfer");
    let bob_file_names: Vec<_> = bob_files.iter()
        .map(|r| r.values[0].clone())
        .collect();
    assert!(bob_file_names.contains(&Value::String("Alice File".into())));
    assert!(bob_file_names.contains(&Value::String("Bob File".into())));
}

#[test]
fn policy_chain_update_at_each_level() {
    // Test updates at every level of a 3-hop chain trigger correct propagation.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE orgs (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)").unwrap();
    db.execute("CREATE TABLE teams (name STRING NOT NULL, org_id REFERENCES orgs NOT NULL)").unwrap();
    db.execute("CREATE TABLE tasks (title STRING NOT NULL, team_id REFERENCES teams NOT NULL)").unwrap();

    let alice_id = match db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap() {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's hierarchy
    let alice_org = db.insert("orgs", &["name", "owner_id"], vec![
        Value::String("Alice Org".into()),
        Value::Ref(alice_id),
    ]).unwrap();

    // Bob's hierarchy
    let bob_org = db.insert("orgs", &["name", "owner_id"], vec![
        Value::String("Bob Org".into()),
        Value::Ref(bob_id),
    ]).unwrap();

    // Team starts in Alice's org
    let team = db.insert("teams", &["name", "org_id"], vec![
        Value::String("The Team".into()),
        Value::Ref(alice_org),
    ]).unwrap();

    // Task in the team
    let task = db.insert("tasks", &["title", "team_id"], vec![
        Value::String("The Task".into()),
        Value::Ref(team),
    ]).unwrap();

    // Set up policies
    db.execute("CREATE POLICY ON orgs FOR SELECT WHERE owner_id = @viewer").unwrap();
    db.execute("CREATE POLICY ON teams FOR SELECT WHERE INHERITS SELECT FROM org_id").unwrap();
    db.execute("CREATE POLICY ON tasks FOR SELECT WHERE INHERITS SELECT FROM team_id").unwrap();

    let alice_query = db.incremental_query_as("SELECT * FROM tasks", alice_id).unwrap();
    let bob_query = db.incremental_query_as("SELECT * FROM tasks", bob_id).unwrap();

    // Initially Alice sees the task
    assert_eq!(alice_query.rows().len(), 1);
    assert_eq!(alice_query.rows()[0].id, task);
    assert_eq!(bob_query.rows().len(), 0);

    // Update 1: Move team to Bob's org
    db.update("teams", team, &[("org_id", Value::Ref(bob_org))]).unwrap();

    assert_eq!(alice_query.rows().len(), 0, "Alice should not see task after team moved");
    assert_eq!(bob_query.rows().len(), 1, "Bob should see task after team moved");
    assert_eq!(bob_query.rows()[0].id, task);

    // Update 2: Move team back to Alice's org
    db.update("teams", team, &[("org_id", Value::Ref(alice_org))]).unwrap();

    assert_eq!(alice_query.rows().len(), 1, "Alice should see task again");
    assert_eq!(bob_query.rows().len(), 0, "Bob should not see task anymore");

    // Update 3: Transfer org ownership
    db.update("orgs", alice_org, &[("owner_id", Value::Ref(bob_id))]).unwrap();

    assert_eq!(alice_query.rows().len(), 0, "Alice should not see task after org transfer");
    assert_eq!(bob_query.rows().len(), 1, "Bob should see task after org transfer");

    // Update 4: Update the task itself (should not change visibility)
    db.update("tasks", task, &[("title", Value::String("Updated Task".into()))]).unwrap();

    assert_eq!(bob_query.rows().len(), 1);
    assert_eq!(bob_query.rows()[0].values[0], Value::String("Updated Task".into()));
}
