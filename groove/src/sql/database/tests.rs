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
