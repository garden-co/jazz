/// Unit tests for internal APIs only.
/// Most tests have been moved to tests/sql_database.rs as integration tests.
use super::*;

// ========== Tests for Buffer-Based Row APIs ==========

#[test]
fn insert_row_basic() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL, age I32 NOT NULL)")
        .unwrap();

    let schema = db.get_table("users").unwrap();
    let desc = Arc::new(RowDescriptor::from_table_schema(&schema));

    let row = RowBuilder::new(desc)
        .set_string_by_name("name", "Alice")
        .set_i32_by_name("age", 30)
        .build();

    let id = db.insert_row("users", row).unwrap();

    // Verify we can retrieve the row
    let (retrieved_id, retrieved_row) = db.get("users", id).unwrap().unwrap();
    assert_eq!(retrieved_id, id);
    assert_eq!(
        retrieved_row
            .get_by_name("name")
            .map(|v| format!("{:?}", v)),
        Some("String(\"Alice\")".to_string())
    );
    assert_eq!(
        retrieved_row.get_by_name("age").map(|v| format!("{:?}", v)),
        Some("I32(30)".to_string())
    );
}

#[test]
fn update_row_basic() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL, age I32 NOT NULL)")
        .unwrap();

    // Insert using builder API
    let id = db
        .insert_with("users", |b| {
            b.set_string_by_name("name", "Alice")
                .set_i32_by_name("age", 30)
                .build()
        })
        .unwrap();

    // Update using new API
    let schema = db.get_table("users").unwrap();
    let desc = Arc::new(RowDescriptor::from_table_schema(&schema));

    let new_row = RowBuilder::new(desc)
        .set_string_by_name("name", "Bob")
        .set_i32_by_name("age", 35)
        .build();

    let updated = db.update_row("users", id, new_row).unwrap();
    assert!(updated);

    // Verify the update
    let (_, retrieved_row) = db.get("users", id).unwrap().unwrap();
    assert_eq!(
        retrieved_row
            .get_by_name("name")
            .map(|v| format!("{:?}", v)),
        Some("String(\"Bob\")".to_string())
    );
    assert_eq!(
        retrieved_row.get_by_name("age").map(|v| format!("{:?}", v)),
        Some("I32(35)".to_string())
    );
}

#[test]
fn insert_row_with_ref() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    // Insert user using builder API
    let user_id = db
        .insert_with("users", |b| b.set_string_by_name("name", "Alice").build())
        .unwrap();

    // Insert post using new API
    let schema = db.get_table("posts").unwrap();
    let desc = Arc::new(RowDescriptor::from_table_schema(&schema));

    let post = RowBuilder::new(desc)
        .set_ref_by_name("author", user_id)
        .set_string_by_name("title", "Hello World")
        .build();

    let post_id = db.insert_row("posts", post).unwrap();

    // Verify we can retrieve the post with correct reference
    let (_, retrieved_post) = db.get("posts", post_id).unwrap().unwrap();
    assert_eq!(
        retrieved_post.get_by_name("author"),
        Some(RowValue::Ref(user_id))
    );
}

// ========== Tests Using Internal APIs ==========
// These tests use private methods or #[cfg(test)] methods and must remain as unit tests.

#[test]
fn table_rows_object_created() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();

    // Table rows object should exist (uses internal API)
    let rows_id = db.table_rows_object_id("users");
    assert!(rows_id.is_some());
}

#[test]
fn index_object_created() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    // Index object should exist (uses internal API)
    let key = IndexKey::new("posts", "author");
    let index_id = db.index_object_id(&key);
    assert!(index_id.is_some());
}

#[test]
fn table_rows_updates_on_insert() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();

    // Uses private read_table_rows method
    let table_rows = db.read_table_rows("users").unwrap();
    assert!(table_rows.is_empty());

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let table_rows = db.read_table_rows("users").unwrap();
    assert_eq!(table_rows.len(), 1);
    assert!(
        table_rows.contains(alice_id),
        "table_rows should contain inserted row ID"
    );
}

#[test]
fn table_rows_updates_on_delete() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    let id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Uses private read_table_rows method
    let table_rows = db.read_table_rows("users").unwrap();
    assert_eq!(table_rows.len(), 1);
    assert!(
        table_rows.contains(id),
        "table_rows should contain inserted row ID"
    );

    db.delete("users", id).unwrap();

    let table_rows = db.read_table_rows("users").unwrap();
    assert!(table_rows.is_empty());
    assert!(
        !table_rows.contains(id),
        "table_rows should not contain deleted row ID"
    );
}

// ========== Incremental Query Tests ==========

#[test]
fn incremental_query_basic() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)")
        .unwrap();

    // Create an incremental query before any data
    let query = db.incremental_query("SELECT * FROM users").unwrap();
    assert!(query.rows().is_empty());

    // Insert some data
    db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)")
        .unwrap();

    // Query should now return both rows
    let rows = query.rows();
    eprintln!("DEBUG: rows = {:?}", rows);
    assert_eq!(rows.len(), 2);
    let names: Vec<_> = rows
        .iter()
        .map(|(_id, row)| row.get_by_name("name"))
        .collect();
    assert!(names.contains(&Some(RowValue::String("Alice"))));
    assert!(names.contains(&Some(RowValue::String("Bob"))));
}

#[test]
fn incremental_query_with_filter() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)")
        .unwrap();

    // Create an incremental query with a WHERE clause
    let query = db
        .incremental_query("SELECT * FROM users WHERE active = true")
        .unwrap();

    // Insert some data
    db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Carol', true)")
        .unwrap();

    // Query should only return active users
    let rows = query.rows();
    assert_eq!(rows.len(), 2);
    let names: Vec<_> = rows
        .iter()
        .map(|(_id, row)| row.get_by_name("name"))
        .collect();
    assert!(names.contains(&Some(RowValue::String("Alice"))));
    assert!(names.contains(&Some(RowValue::String("Carol"))));
    assert!(!names.contains(&Some(RowValue::String("Bob"))));
}

#[test]
fn incremental_query_update_enters_filter() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)")
        .unwrap();

    // Create an incremental query for active users
    let query = db
        .incremental_query("SELECT * FROM users WHERE active = true")
        .unwrap();

    // Insert an inactive user
    let id = match db
        .execute("INSERT INTO users (name, active) VALUES ('Alice', false)")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Should not be in results
    assert!(query.rows().is_empty());

    // Activate the user
    db.update_with("users", id, |b| b.set_bool_by_name("active", true).build())
        .unwrap();

    // Should now appear in results
    let rows = query.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1.get_by_name("name"),
        Some(RowValue::String("Alice"))
    );
    assert_eq!(rows[0].1.get_by_name("active"), Some(RowValue::Bool(true)));
}

#[test]
fn incremental_query_update_leaves_filter() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)")
        .unwrap();

    // Create an incremental query for active users
    let query = db
        .incremental_query("SELECT * FROM users WHERE active = true")
        .unwrap();

    // Insert an active user
    let id = match db
        .execute("INSERT INTO users (name, active) VALUES ('Alice', true)")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Should be in results
    let rows = query.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1.get_by_name("name"),
        Some(RowValue::String("Alice"))
    );
    assert_eq!(rows[0].1.get_by_name("active"), Some(RowValue::Bool(true)));

    // Deactivate the user
    db.update_with("users", id, |b| b.set_bool_by_name("active", false).build())
        .unwrap();

    // Should no longer be in results
    assert!(query.rows().is_empty());
}

#[test]
fn incremental_query_delete() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();

    let query = db.incremental_query("SELECT * FROM users").unwrap();

    let id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let rows = query.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1.get_by_name("name"),
        Some(RowValue::String("Alice"))
    );
    assert_eq!(rows[0].0, id);

    db.delete("users", id).unwrap();

    assert!(query.rows().is_empty());
}

#[test]
fn incremental_query_subscribe() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();

    let query = db.incremental_query("SELECT * FROM users").unwrap();

    let delta_count = Arc::new(AtomicUsize::new(0));
    let delta_count_clone = delta_count.clone();

    let _listener_id = query.subscribe(Box::new(move |delta| {
        delta_count_clone.fetch_add(delta.len(), Ordering::SeqCst);
    }));

    // Insert triggers callback
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap();
    assert_eq!(delta_count.load(Ordering::SeqCst), 1);

    // Another insert
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();
    assert_eq!(delta_count.load(Ordering::SeqCst), 2);
}

#[test]
fn incremental_query_subscribe_rows() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();

    let query = db.incremental_query("SELECT * FROM users").unwrap();

    let delta_count = Arc::new(AtomicUsize::new(0));
    let delta_count_clone = delta_count.clone();

    let _listener_id = query.subscribe(Box::new(move |deltas| {
        delta_count_clone.fetch_add(deltas.len(), Ordering::SeqCst);
    }));

    // Initial state callback has 0 rows (no data yet)
    assert_eq!(delta_count.load(Ordering::SeqCst), 0);

    // Insert triggers callback with 1 delta (Added)
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap();
    assert_eq!(delta_count.load(Ordering::SeqCst), 1);

    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();
    assert_eq!(delta_count.load(Ordering::SeqCst), 2);
}

#[test]
fn incremental_query_join_basic() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    // Create JOIN query
    let query = db
        .incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id")
        .unwrap();
    assert!(query.rows().is_empty());

    // Insert a user
    let user_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Still empty - no posts yet
    assert!(query.rows().is_empty());

    // Insert a post by Alice
    db.insert_with("posts", |b| {
        b.set_ref_by_name("author", user_id)
            .set_string_by_name("title", "Hello World")
            .build()
    })
    .unwrap();

    // Now we should have one joined row
    let rows = query.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1.get_by_name("posts.title"),
        Some(RowValue::String("Hello World"))
    );
    assert_eq!(
        rows[0].1.get_by_name("users.name"),
        Some(RowValue::String("Alice"))
    );
}

#[test]
fn incremental_query_join_left_table_change() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    // Insert a user first
    let user_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Create JOIN query
    let query = db
        .incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id")
        .unwrap();

    // Insert multiple posts
    db.insert_with("posts", |b| {
        b.set_ref_by_name("author", user_id)
            .set_string_by_name("title", "Post 1")
            .build()
    })
    .unwrap();
    db.insert_with("posts", |b| {
        b.set_ref_by_name("author", user_id)
            .set_string_by_name("title", "Post 2")
            .build()
    })
    .unwrap();

    let rows = query.rows();
    assert_eq!(rows.len(), 2);
    // Both rows should have Alice as author
    for row in &rows {
        assert_eq!(
            row.1.get_by_name("users.name"),
            Some(RowValue::String("Alice"))
        );
    }
    let titles: Vec<_> = rows
        .iter()
        .filter_map(|r| {
            if let Some(RowValue::String(s)) = r.1.get_by_name("posts.title") {
                Some(s.to_string())
            } else {
                None
            }
        })
        .collect();
    assert!(titles.contains(&"Post 1".to_string()));
    assert!(titles.contains(&"Post 2".to_string()));
}

#[test]
fn incremental_query_join_right_table_change() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    // Insert a user
    let user_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Insert a post
    db.insert_with("posts", |b| {
        b.set_ref_by_name("author", user_id)
            .set_string_by_name("title", "Hello")
            .build()
    })
    .unwrap();

    // Create JOIN query after initial data
    let query = db
        .incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id")
        .unwrap();

    // Track changes via delta subscription
    let delta_count = Arc::new(AtomicUsize::new(0));
    let delta_count_clone = delta_count.clone();

    let _listener = query.subscribe(Box::new(move |delta| {
        delta_count_clone.fetch_add(delta.len(), Ordering::SeqCst);
    }));

    // Updating the right table (users) should trigger notification
    // because the joined row contains data from that table
    db.update_with("users", user_id, |b| {
        b.set_string_by_name("name", "Alice Updated").build()
    })
    .unwrap();

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
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();

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
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();

    // Create documents table with owner_id
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();

    // Create users
    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Create documents
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Alice's Doc")
            .set_ref_by_name("owner_id", alice_id)
            .build()
    })
    .unwrap();
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Bob's Doc")
            .set_ref_by_name("owner_id", bob_id)
            .build()
    })
    .unwrap();

    // Without policy: both users see all documents
    let all_docs = db.query("SELECT * FROM documents").unwrap();
    assert_eq!(all_docs.len(), 2);
    let all_titles: Vec<_> = all_docs
        .iter()
        .filter_map(|r| {
            if let Some(RowValue::String(s)) = r.1.get_by_name("title") {
                Some(s.to_string())
            } else {
                None
            }
        })
        .collect();
    assert!(all_titles.contains(&"Alice's Doc".to_string()));
    assert!(all_titles.contains(&"Bob's Doc".to_string()));

    // Add policy: owner can read their own documents
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer")
        .unwrap();

    // Alice can only see her document
    let alice_docs = db.query_as("SELECT * FROM documents", alice_id).unwrap();
    assert_eq!(alice_docs.len(), 1);
    assert_eq!(
        alice_docs[0].1.get_by_name("title"),
        Some(RowValue::String("Alice's Doc"))
    );

    // Bob can only see his document
    let bob_docs = db.query_as("SELECT * FROM documents", bob_id).unwrap();
    assert_eq!(bob_docs.len(), 1);
    assert_eq!(
        bob_docs[0].1.get_by_name("title"),
        Some(RowValue::String("Bob's Doc"))
    );
}

#[test]
fn select_all_as_with_inheritance() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    // Create users
    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Create folders
    let alice_folder = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Alice's Folder")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    // Create document in Alice's folder
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Secret Doc")
            .set_ref_by_name("folder_id", alice_folder)
            .build()
    })
    .unwrap();

    // Add policies
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    // Alice can see the document (via folder ownership)
    // Note: INHERITS policy expands to a JOIN, so column names may be qualified
    let alice_docs = db.query_as("SELECT * FROM documents", alice_id).unwrap();
    assert_eq!(alice_docs.len(), 1);
    // Try both qualified and unqualified names since INHERITS may add a JOIN
    let title = alice_docs[0]
        .1
        .get_by_name("title")
        .or_else(|| alice_docs[0].1.get_by_name("title"));
    assert_eq!(title, Some(RowValue::String("Secret Doc")));

    // Bob cannot see the document
    let bob_docs = db.query_as("SELECT * FROM documents", bob_id).unwrap();
    assert_eq!(bob_docs.len(), 0);
}

// ========== Write Policy Tests ==========

#[test]
fn insert_as_checks_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, author_id REFERENCES users NOT NULL)",
    )
    .unwrap();

    // Create users
    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Add INSERT policy: author_id must be viewer
    db.execute("CREATE POLICY ON documents FOR INSERT CHECK (@new.author_id = @viewer)")
        .unwrap();

    // Alice can insert doc with herself as author
    let result = db.insert_with_as(
        "documents",
        |b| {
            b.set_string_by_name("title", "Alice's Doc")
                .set_ref_by_name("author_id", alice_id)
                .build()
        },
        alice_id,
    );
    assert!(
        result.is_ok(),
        "owner should be able to insert: {:?}",
        result
    );

    // Alice cannot insert doc with Bob as author
    let result = db.insert_with_as(
        "documents",
        |b| {
            b.set_string_by_name("title", "Forged Doc")
                .set_ref_by_name("author_id", bob_id)
                .build()
        },
        alice_id,
    );
    assert!(
        matches!(result, Err(DatabaseError::PolicyDenied { .. })),
        "should deny insert with other as author: {:?}",
        result
    );
}

#[test]
fn update_as_checks_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();

    // Create users
    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Create a document owned by Alice
    let doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Original")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    // Add UPDATE policy: owner can update
    db.execute("CREATE POLICY ON documents FOR UPDATE WHERE owner_id = @viewer")
        .unwrap();

    // Alice can update
    let result = db.update_with_as(
        "documents",
        doc_id,
        |b| b.set_string_by_name("title", "Updated").build(),
        alice_id,
    );
    assert!(
        result.is_ok(),
        "owner should be able to update: {:?}",
        result
    );

    // Bob cannot update
    let result = db.update_with_as(
        "documents",
        doc_id,
        |b| b.set_string_by_name("title", "Hacked").build(),
        bob_id,
    );
    assert!(
        matches!(result, Err(DatabaseError::PolicyDenied { .. })),
        "non-owner should not be able to update: {:?}",
        result
    );
}

#[test]
fn update_as_checks_both_where_and_check() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Original")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    // Add UPDATE policy: owner can update, but cannot change owner
    db.execute("CREATE POLICY ON documents FOR UPDATE WHERE owner_id = @viewer CHECK (@new.owner_id = @old.owner_id)").unwrap();

    // Alice can update title
    let result = db.update_with_as(
        "documents",
        doc_id,
        |b| b.set_string_by_name("title", "New Title").build(),
        alice_id,
    );
    assert!(result.is_ok(), "should allow updating title: {:?}", result);

    // Alice cannot change owner
    let result = db.update_with_as(
        "documents",
        doc_id,
        |b| b.set_ref_by_name("owner_id", bob_id).build(),
        alice_id,
    );
    assert!(
        matches!(result, Err(DatabaseError::PolicyDenied { .. })),
        "should deny changing owner: {:?}",
        result
    );
}

#[test]
fn delete_as_checks_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "To Delete")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    // Add DELETE policy: owner can delete
    db.execute("CREATE POLICY ON documents FOR DELETE WHERE owner_id = @viewer")
        .unwrap();

    // Bob cannot delete
    let result = db.delete_as("documents", doc_id, bob_id);
    assert!(
        matches!(result, Err(DatabaseError::PolicyDenied { .. })),
        "non-owner should not be able to delete: {:?}",
        result
    );

    // Alice can delete
    let result = db.delete_as("documents", doc_id, alice_id);
    assert!(
        result.is_ok(),
        "owner should be able to delete: {:?}",
        result
    );
}

#[test]
fn delete_as_falls_back_to_update_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Deletable")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    // Only add UPDATE policy (no DELETE policy)
    db.execute("CREATE POLICY ON documents FOR UPDATE WHERE owner_id = @viewer")
        .unwrap();

    // Bob cannot delete (UPDATE policy check fails)
    let result = db.delete_as("documents", doc_id, bob_id);
    assert!(
        matches!(result, Err(DatabaseError::PolicyDenied { .. })),
        "non-owner should not be able to delete via UPDATE fallback: {:?}",
        result
    );

    // Alice can delete (UPDATE policy allows it)
    let result = db.delete_as("documents", doc_id, alice_id);
    assert!(
        result.is_ok(),
        "owner should be able to delete via UPDATE fallback: {:?}",
        result
    );
}

// ========== Incremental Query with Policy Tests ==========

#[test]
fn incremental_query_as_filters_by_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();

    // Create users
    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Create documents
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Alice's Doc")
            .set_ref_by_name("owner_id", alice_id)
            .build()
    })
    .unwrap();
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Bob's Doc")
            .set_ref_by_name("owner_id", bob_id)
            .build()
    })
    .unwrap();

    // Add SELECT policy: owner can read
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer")
        .unwrap();

    // Alice's incremental query should only see her document
    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();
    let alice_rows = alice_query.rows();
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(
        alice_rows[0].1.get_by_name("title"),
        Some(RowValue::String("Alice's Doc"))
    );

    // Bob's incremental query should only see his document
    let bob_query = db
        .incremental_query_as("SELECT * FROM documents", bob_id)
        .unwrap();
    let bob_rows = bob_query.rows();
    assert_eq!(bob_rows.len(), 1);
    assert_eq!(
        bob_rows[0].1.get_by_name("title"),
        Some(RowValue::String("Bob's Doc"))
    );
}

#[test]
fn incremental_query_as_updates_on_insert() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Add SELECT policy: owner can read
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer")
        .unwrap();

    // Create query before any documents
    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();
    assert!(alice_query.rows().is_empty());

    // Insert document for Alice
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Alice's Doc")
            .set_ref_by_name("owner_id", alice_id)
            .build()
    })
    .unwrap();

    // Alice should see it
    let alice_rows = alice_query.rows();
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(
        alice_rows[0].1.get_by_name("title"),
        Some(RowValue::String("Alice's Doc"))
    );

    // Insert document for Bob
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Bob's Doc")
            .set_ref_by_name("owner_id", bob_id)
            .build()
    })
    .unwrap();

    // Alice should still only see 1 document (her own)
    let alice_rows_after = alice_query.rows();
    assert_eq!(alice_rows_after.len(), 1);
    assert_eq!(
        alice_rows_after[0].1.get_by_name("title"),
        Some(RowValue::String("Alice's Doc"))
    );
}

#[test]
fn incremental_query_as_combines_with_where_clause() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL, published BOOL NOT NULL)").unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Add SELECT policy
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer")
        .unwrap();

    // Insert multiple documents for Alice
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Draft")
            .set_ref_by_name("owner_id", alice_id)
            .set_bool_by_name("published", false)
            .build()
    })
    .unwrap();
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Published")
            .set_ref_by_name("owner_id", alice_id)
            .set_bool_by_name("published", true)
            .build()
    })
    .unwrap();

    // Query with user WHERE clause combined with policy
    let query = db
        .incremental_query_as("SELECT * FROM documents WHERE published = true", alice_id)
        .unwrap();
    let rows = query.rows();

    // Should only see published documents owned by Alice
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1.get_by_name("title"),
        Some(RowValue::String("Published"))
    );
}

#[test]
fn incremental_query_as_no_policy_allows_all() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE items (name STRING NOT NULL)")
        .unwrap();

    db.insert_with("items", |b| b.set_string_by_name("name", "Item 1").build())
        .unwrap();
    db.insert_with("items", |b| b.set_string_by_name("name", "Item 2").build())
        .unwrap();

    // No policy - should see all items (with warning)
    let query = db
        .incremental_query_as("SELECT * FROM items", ObjectId::new(999))
        .unwrap();
    let rows = query.rows();
    assert_eq!(rows.len(), 2);
    let names: Vec<_> = rows
        .iter()
        .filter_map(|r| {
            if let Some(RowValue::String(s)) = r.1.get_by_name("name") {
                Some(s.to_string())
            } else {
                None
            }
        })
        .collect();
    assert!(names.contains(&"Item 1".to_string()));
    assert!(names.contains(&"Item 2".to_string()));
}

#[test]
fn incremental_query_as_or_policy() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL, public BOOL NOT NULL)").unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Policy: owner OR public
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer OR public = true")
        .unwrap();

    // Create documents
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Alice Private")
            .set_ref_by_name("owner_id", alice_id)
            .set_bool_by_name("public", false)
            .build()
    })
    .unwrap();
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Alice Public")
            .set_ref_by_name("owner_id", alice_id)
            .set_bool_by_name("public", true)
            .build()
    })
    .unwrap();
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Bob Private")
            .set_ref_by_name("owner_id", bob_id)
            .set_bool_by_name("public", false)
            .build()
    })
    .unwrap();

    // Alice can see: her private, her public (but not bob's private)
    // With "owner_id = @viewer OR public = true":
    // - Alice Private: owner=alice ✓
    // - Alice Public: owner=alice ✓ (also public)
    // - Bob Private: owner=bob, public=false ✗
    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();
    let alice_rows = alice_query.rows();
    assert_eq!(alice_rows.len(), 2);
    let alice_titles: Vec<_> = alice_rows
        .iter()
        .filter_map(|r| {
            if let Some(RowValue::String(s)) = r.1.get_by_name("title") {
                Some(s.to_string())
            } else {
                None
            }
        })
        .collect();
    assert!(alice_titles.contains(&"Alice Private".to_string()));
    assert!(alice_titles.contains(&"Alice Public".to_string()));
    assert!(!alice_titles.contains(&"Bob Private".to_string()));

    // Bob can see: his private, alice's public
    // - Alice Private: owner=alice, public=false ✗
    // - Alice Public: public=true ✓
    // - Bob Private: owner=bob ✓
    let bob_query = db
        .incremental_query_as("SELECT * FROM documents", bob_id)
        .unwrap();
    let bob_rows = bob_query.rows();
    assert_eq!(bob_rows.len(), 2);
    let bob_titles: Vec<_> = bob_rows
        .iter()
        .filter_map(|r| {
            if let Some(RowValue::String(s)) = r.1.get_by_name("title") {
                Some(s.to_string())
            } else {
                None
            }
        })
        .collect();
    assert!(bob_titles.contains(&"Alice Public".to_string()));
    assert!(bob_titles.contains(&"Bob Private".to_string()));
    assert!(!bob_titles.contains(&"Alice Private".to_string()));
}

#[test]
fn incremental_query_as_inherits_flattened_to_join() {
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    // Create users
    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Create folders
    let alice_folder = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Alice's Folder")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();
    let bob_folder = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Bob's Folder")
                .set_ref_by_name("owner_id", bob_id)
                .build()
        })
        .unwrap();

    // Create documents
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Alice's Doc")
            .set_ref_by_name("folder_id", alice_folder)
            .build()
    })
    .unwrap();
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Bob's Doc")
            .set_ref_by_name("folder_id", bob_folder)
            .build()
    })
    .unwrap();

    // Add policies:
    // - folders: owner can read
    // - documents: inherit from folder
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    // Alice can only see her document (via folder ownership)
    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();
    let alice_rows = alice_query.rows();
    assert_eq!(
        alice_rows.len(),
        1,
        "Alice should see 1 doc: {:?}",
        alice_rows
    );
    // After Projection, column names should be unqualified ("title" not "documents.title")
    assert_eq!(
        alice_rows[0].1.get_by_name("title"),
        Some(RowValue::String("Alice's Doc"))
    );

    // Bob can only see his document
    let bob_query = db
        .incremental_query_as("SELECT * FROM documents", bob_id)
        .unwrap();
    let bob_rows = bob_query.rows();
    assert_eq!(bob_rows.len(), 1, "Bob should see 1 doc: {:?}", bob_rows);
    assert_eq!(
        bob_rows[0].1.get_by_name("title"),
        Some(RowValue::String("Bob's Doc"))
    );
}

#[test]
fn incremental_query_as_inherits_incremental_updates() {
    use crate::sql::policy::clear_policy_warnings;
    use std::sync::atomic::{AtomicUsize, Ordering};

    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Create folder and policy before documents
    let alice_folder = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Alice's Folder")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    // Create query before any documents
    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();
    assert!(alice_query.rows().is_empty());

    // Track changes via subscription
    let change_count = Arc::new(AtomicUsize::new(0));
    let change_count_clone = change_count.clone();
    let _listener = alice_query.subscribe(Box::new(move |delta| {
        change_count_clone.fetch_add(delta.len(), Ordering::SeqCst);
    }));

    // Insert document into Alice's folder - should trigger update
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "New Doc")
            .set_ref_by_name("folder_id", alice_folder)
            .build()
    })
    .unwrap();

    // Query should now have one row
    let rows = alice_query.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1.get_by_name("title"),
        Some(RowValue::String("New Doc"))
    );
    // And we should have received a delta
    assert!(
        change_count.load(Ordering::SeqCst) > 0,
        "Should have received delta notification"
    );
}

#[test]
fn incremental_query_as_inherits_folder_ownership_change() {
    // Test that changing folder ownership propagates through INHERITS policy
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Create folder owned by Alice
    let folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Shared Folder")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    // Create document in that folder
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Important Doc")
            .set_ref_by_name("folder_id", folder_id)
            .build()
    })
    .unwrap();

    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    // Alice can see the document
    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();
    let alice_rows = alice_query.rows();
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(
        alice_rows[0].1.get_by_name("title"),
        Some(RowValue::String("Important Doc"))
    );

    // Bob cannot see the document
    let bob_query = db
        .incremental_query_as("SELECT * FROM documents", bob_id)
        .unwrap();
    assert_eq!(bob_query.rows().len(), 0);

    // Transfer folder ownership to Bob
    db.update_with("folders", folder_id, |b| {
        b.set_ref_by_name("owner_id", bob_id).build()
    })
    .unwrap();

    // Now Alice should NOT see the document (folder no longer hers)
    // NOTE: This tests incremental propagation through the JOIN
    let alice_rows_after = alice_query.rows();
    assert_eq!(
        alice_rows_after.len(),
        0,
        "Alice should no longer see doc after folder transfer"
    );

    // Bob should now see the document
    let bob_rows_after = bob_query.rows();
    assert_eq!(
        bob_rows_after.len(),
        1,
        "Bob should now see doc after folder transfer"
    );
    assert_eq!(
        bob_rows_after[0].1.get_by_name("title"),
        Some(RowValue::String("Important Doc"))
    );
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

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE workspaces (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's workspace with a folder and document
    let alice_workspace_id = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Alice's Workspace")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    let alice_folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Alice's Folder")
                .set_ref_by_name("workspace_id", alice_workspace_id)
                .build()
        })
        .unwrap();

    let alice_doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Alice's Doc")
                .set_ref_by_name("folder_id", alice_folder_id)
                .build()
        })
        .unwrap();

    // Bob's workspace with a folder and document
    let bob_workspace_id = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Bob's Workspace")
                .set_ref_by_name("owner_id", bob_id)
                .build()
        })
        .unwrap();

    let bob_folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Bob's Folder")
                .set_ref_by_name("workspace_id", bob_workspace_id)
                .build()
        })
        .unwrap();

    let _bob_doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Bob's Doc")
                .set_ref_by_name("folder_id", bob_folder_id)
                .build()
        })
        .unwrap();

    // Set up 2-hop chain: documents -> folders -> workspaces -> owner_id
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    // Alice should only see her document (via folder → workspace → owner)
    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .expect("Nested INHERITS chain should work");
    let alice_docs = alice_query.rows();

    assert_eq!(
        alice_docs.len(),
        1,
        "Alice should see 1 document through the chain"
    );
    assert_eq!(
        alice_docs[0].0, alice_doc_id,
        "Alice should see her own document"
    );
    assert_eq!(
        alice_docs[0].1.get_by_name("title"),
        Some(RowValue::String("Alice's Doc"))
    );

    // Bob should only see his document
    let bob_query = db
        .incremental_query_as("SELECT * FROM documents", bob_id)
        .expect("Nested INHERITS chain should work");
    let bob_docs = bob_query.rows();

    assert_eq!(
        bob_docs.len(),
        1,
        "Bob should see 1 document through the chain"
    );
    assert_eq!(
        bob_docs[0].1.get_by_name("title"),
        Some(RowValue::String("Bob's Doc"))
    );
}

#[test]
fn incremental_query_as_inherits_multiple_docs_same_folder() {
    // Test INHERITS with multiple documents in the same folder
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Alice's Folder")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    // Insert multiple documents
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Doc 1")
            .set_ref_by_name("folder_id", folder_id)
            .build()
    })
    .unwrap();
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Doc 2")
            .set_ref_by_name("folder_id", folder_id)
            .build()
    })
    .unwrap();
    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Doc 3")
            .set_ref_by_name("folder_id", folder_id)
            .build()
    })
    .unwrap();

    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();
    let rows = alice_query.rows();

    assert_eq!(
        rows.len(),
        3,
        "Alice should see all 3 documents in her folder"
    );
    let titles: Vec<_> = rows.iter().map(|r| r.1.get_by_name("title")).collect();
    assert!(titles.contains(&Some(RowValue::String("Doc 1"))));
    assert!(titles.contains(&Some(RowValue::String("Doc 2"))));
    assert!(titles.contains(&Some(RowValue::String("Doc 3"))));
}

#[test]
fn incremental_query_as_inherits_delete_propagates() {
    // Test that deleting a folder removes access to its documents
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Temp Folder")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    db.insert_with("documents", |b| {
        b.set_string_by_name("title", "Orphan Doc")
            .set_ref_by_name("folder_id", folder_id)
            .build()
    })
    .unwrap();

    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();

    // Initially Alice can see the document
    let initial_rows = alice_query.rows();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(
        initial_rows[0].1.get_by_name("title"),
        Some(RowValue::String("Orphan Doc"))
    );

    // Delete the folder
    db.delete("folders", folder_id).unwrap();

    // Now the document should not be visible (no matching folder in JOIN)
    let after_delete = alice_query.rows();
    assert_eq!(
        after_delete.len(),
        0,
        "Document should not be visible after folder deletion"
    );
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

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    // Self-referential: parent_id references the same table
    db.execute("CREATE TABLE folders (name STRING NOT NULL, parent_id REFERENCES folders, owner_id REFERENCES users)").unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Create folder hierarchy owned by Alice:
    // root (owned by Alice) -> child -> grandchild -> great_grandchild
    let root_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Root")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    let child_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Child")
                .set_ref_by_name("parent_id", root_id)
                .build()
        })
        .unwrap();

    let grandchild_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Grandchild")
                .set_ref_by_name("parent_id", child_id)
                .build()
        })
        .unwrap();

    let great_grandchild_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "GreatGrandchild")
                .set_ref_by_name("parent_id", grandchild_id)
                .build()
        })
        .unwrap();

    // Policy: owner OR inherit from parent (recursive!)
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer OR INHERITS SELECT FROM parent_id").unwrap();

    // Create incremental query - this should now work with RecursiveFilter
    let query = db
        .incremental_query_as("SELECT * FROM folders", alice_id)
        .expect("Self-referential INHERITS should work with RecursiveFilter");

    // Alice should see all 4 folders (root via owner_id, others via inheritance)
    let alice_rows = query.rows();
    assert_eq!(
        alice_rows.len(),
        4,
        "Alice should see all 4 folders through recursive inheritance"
    );

    // Verify specific folders are visible
    let folder_ids: Vec<_> = alice_rows.iter().map(|r| r.0).collect();
    assert!(
        folder_ids.contains(&root_id),
        "Root should be visible (owned by Alice)"
    );
    assert!(
        folder_ids.contains(&child_id),
        "Child should be visible (inherits from root)"
    );
    assert!(
        folder_ids.contains(&grandchild_id),
        "Grandchild should be visible (inherits from child)"
    );
    assert!(
        folder_ids.contains(&great_grandchild_id),
        "GreatGrandchild should be visible (inherits from grandchild)"
    );

    // Bob should see no folders (doesn't own any, no inheritance path)
    let bob_query = db
        .incremental_query_as("SELECT * FROM folders", bob_id)
        .expect("Query should work for Bob too");
    let bob_rows = bob_query.rows();
    assert_eq!(bob_rows.len(), 0, "Bob should see no folders");

    // Test incremental update: create a new folder under grandchild
    let new_folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "NewFolder")
                .set_ref_by_name("parent_id", grandchild_id)
                .build()
        })
        .unwrap();

    // Alice should now see 5 folders
    let alice_rows_after = query.rows();
    assert_eq!(
        alice_rows_after.len(),
        5,
        "Alice should now see 5 folders after insert"
    );
    let folder_ids_after: Vec<_> = alice_rows_after.iter().map(|r| r.0).collect();
    assert!(
        folder_ids_after.contains(&new_folder_id),
        "New folder should be visible via inheritance"
    );
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

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, parent_id REFERENCES folders, owner_id REFERENCES users)").unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Create root folder with owner
    let root_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Root")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    db.insert_with("folders", |b| {
        b.set_string_by_name("name", "Child")
            .set_ref_by_name("parent_id", root_id)
            .build()
    })
    .unwrap();

    // Pure INHERITS policy (no simple predicate fallback)
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM parent_id")
        .unwrap();

    // This should work but return no rows since there's no base case
    let query = db
        .incremental_query_as("SELECT * FROM folders", alice_id)
        .expect("Pure INHERITS should create a query (but return no rows)");

    let rows = query.rows();
    assert_eq!(
        rows.len(),
        0,
        "Pure INHERITS with no base predicate should return no rows"
    );
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

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE organizations (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE workspaces (name STRING NOT NULL, org_id REFERENCES organizations NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's hierarchy: org → workspace → folder → document
    let alice_org_id = db
        .insert_with("organizations", |b| {
            b.set_string_by_name("name", "Alice's Org")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    let alice_workspace_id = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Alice's Workspace")
                .set_ref_by_name("org_id", alice_org_id)
                .build()
        })
        .unwrap();

    let alice_folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Alice's Folder")
                .set_ref_by_name("workspace_id", alice_workspace_id)
                .build()
        })
        .unwrap();

    let alice_doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Alice's Doc")
                .set_ref_by_name("folder_id", alice_folder_id)
                .build()
        })
        .unwrap();

    // Bob's hierarchy: org → workspace → folder → document
    let bob_org_id = db
        .insert_with("organizations", |b| {
            b.set_string_by_name("name", "Bob's Org")
                .set_ref_by_name("owner_id", bob_id)
                .build()
        })
        .unwrap();

    let bob_workspace_id = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Bob's Workspace")
                .set_ref_by_name("org_id", bob_org_id)
                .build()
        })
        .unwrap();

    let bob_folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Bob's Folder")
                .set_ref_by_name("workspace_id", bob_workspace_id)
                .build()
        })
        .unwrap();

    let _bob_doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Bob's Doc")
                .set_ref_by_name("folder_id", bob_folder_id)
                .build()
        })
        .unwrap();

    // Set up 3-hop chain: documents -> folders -> workspaces -> organizations -> owner_id
    db.execute("CREATE POLICY ON organizations FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE INHERITS SELECT FROM org_id")
        .unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    // Alice should only see her document (via folder → workspace → org → owner)
    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .expect("3-hop INHERITS chain should work");
    let alice_docs = alice_query.rows();

    assert_eq!(
        alice_docs.len(),
        1,
        "Alice should see 1 document through 3-hop chain"
    );
    assert_eq!(
        alice_docs[0].0, alice_doc_id,
        "Alice should see her own document"
    );
    assert_eq!(
        alice_docs[0].1.get_by_name("title"),
        Some(RowValue::String("Alice's Doc"))
    );

    // Bob should only see his document
    let bob_query = db
        .incremental_query_as("SELECT * FROM documents", bob_id)
        .expect("3-hop INHERITS chain should work");
    let bob_docs = bob_query.rows();

    assert_eq!(
        bob_docs.len(),
        1,
        "Bob should see 1 document through 3-hop chain"
    );
    assert_eq!(
        bob_docs[0].1.get_by_name("title"),
        Some(RowValue::String("Bob's Doc"))
    );
}

#[test]
fn incremental_query_as_3_hop_chain_delta_from_org_update() {
    // Test delta propagation from the furthest table (organizations) in a 3-hop chain.
    // When an organization's owner changes, documents visibility should update.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE organizations (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE workspaces (name STRING NOT NULL, org_id REFERENCES organizations NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Start with Alice owning the org
    let org_id = db
        .insert_with("organizations", |b| {
            b.set_string_by_name("name", "Test Org")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    let workspace_id = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Test Workspace")
                .set_ref_by_name("org_id", org_id)
                .build()
        })
        .unwrap();

    let folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Test Folder")
                .set_ref_by_name("workspace_id", workspace_id)
                .build()
        })
        .unwrap();

    let doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Test Doc")
                .set_ref_by_name("folder_id", folder_id)
                .build()
        })
        .unwrap();

    // Set up 3-hop chain policies
    db.execute("CREATE POLICY ON organizations FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE INHERITS SELECT FROM org_id")
        .unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    // Create incremental queries
    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();
    let bob_query = db
        .incremental_query_as("SELECT * FROM documents", bob_id)
        .unwrap();

    // Initially, Alice sees the document, Bob doesn't
    assert_eq!(
        alice_query.rows().len(),
        1,
        "Alice should see doc initially"
    );
    assert_eq!(alice_query.rows()[0].0, doc_id);
    assert_eq!(
        bob_query.rows().len(),
        0,
        "Bob should not see doc initially"
    );

    // Transfer org ownership from Alice to Bob
    db.update_with("organizations", org_id, |b| {
        b.set_ref_by_name("owner_id", bob_id).build()
    })
    .unwrap();

    // Now Bob should see it, Alice shouldn't
    let alice_rows_after = alice_query.rows();
    let bob_rows_after = bob_query.rows();

    assert_eq!(
        alice_rows_after.len(),
        0,
        "Alice should not see doc after org transfer"
    );
    assert_eq!(
        bob_rows_after.len(),
        1,
        "Bob should see doc after org transfer"
    );
    assert_eq!(bob_rows_after[0].0, doc_id);
    assert_eq!(
        bob_rows_after[0].1.get_by_name("title"),
        Some(RowValue::String("Test Doc"))
    );
}

#[test]
fn incremental_query_as_3_hop_chain_delta_from_workspace_update() {
    // Test delta propagation when an intermediate table (workspace) changes.
    // Moving a workspace to a different org should update document visibility.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE organizations (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE workspaces (name STRING NOT NULL, org_id REFERENCES organizations NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's org
    let alice_org_id = db
        .insert_with("organizations", |b| {
            b.set_string_by_name("name", "Alice's Org")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    // Bob's org
    let bob_org_id = db
        .insert_with("organizations", |b| {
            b.set_string_by_name("name", "Bob's Org")
                .set_ref_by_name("owner_id", bob_id)
                .build()
        })
        .unwrap();

    // Workspace starts in Alice's org
    let workspace_id = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Movable Workspace")
                .set_ref_by_name("org_id", alice_org_id)
                .build()
        })
        .unwrap();

    let folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Test Folder")
                .set_ref_by_name("workspace_id", workspace_id)
                .build()
        })
        .unwrap();

    let doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Test Doc")
                .set_ref_by_name("folder_id", folder_id)
                .build()
        })
        .unwrap();

    // Set up 3-hop chain policies
    db.execute("CREATE POLICY ON organizations FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE INHERITS SELECT FROM org_id")
        .unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();
    let bob_query = db
        .incremental_query_as("SELECT * FROM documents", bob_id)
        .unwrap();

    // Initially, Alice sees the document, Bob doesn't
    assert_eq!(
        alice_query.rows().len(),
        1,
        "Alice should see doc initially"
    );
    assert_eq!(
        bob_query.rows().len(),
        0,
        "Bob should not see doc initially"
    );

    // Move workspace from Alice's org to Bob's org
    db.update_with("workspaces", workspace_id, |b| {
        b.set_ref_by_name("org_id", bob_org_id).build()
    })
    .unwrap();

    // Now Bob should see it, Alice shouldn't
    let alice_rows_after = alice_query.rows();
    let bob_rows_after = bob_query.rows();

    assert_eq!(
        alice_rows_after.len(),
        0,
        "Alice should not see doc after workspace move"
    );
    assert_eq!(
        bob_rows_after.len(),
        1,
        "Bob should see doc after workspace move"
    );
    assert_eq!(bob_rows_after[0].0, doc_id);
    assert_eq!(
        bob_rows_after[0].1.get_by_name("title"),
        Some(RowValue::String("Test Doc"))
    );
}

#[test]
fn incremental_query_as_3_hop_chain_delta_from_folder_update() {
    // Test delta propagation when the nearest joined table (folder) changes.
    // Moving a folder to a different workspace should update document visibility.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE organizations (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE workspaces (name STRING NOT NULL, org_id REFERENCES organizations NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's full hierarchy
    let alice_org_id = db
        .insert_with("organizations", |b| {
            b.set_string_by_name("name", "Alice's Org")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    let alice_workspace_id = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Alice's Workspace")
                .set_ref_by_name("org_id", alice_org_id)
                .build()
        })
        .unwrap();

    // Bob's full hierarchy
    let bob_org_id = db
        .insert_with("organizations", |b| {
            b.set_string_by_name("name", "Bob's Org")
                .set_ref_by_name("owner_id", bob_id)
                .build()
        })
        .unwrap();

    let bob_workspace_id = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Bob's Workspace")
                .set_ref_by_name("org_id", bob_org_id)
                .build()
        })
        .unwrap();

    // Folder starts in Alice's workspace
    let folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Movable Folder")
                .set_ref_by_name("workspace_id", alice_workspace_id)
                .build()
        })
        .unwrap();

    let doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Test Doc")
                .set_ref_by_name("folder_id", folder_id)
                .build()
        })
        .unwrap();

    // Set up 3-hop chain policies
    db.execute("CREATE POLICY ON organizations FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE INHERITS SELECT FROM org_id")
        .unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();
    let bob_query = db
        .incremental_query_as("SELECT * FROM documents", bob_id)
        .unwrap();

    // Initially, Alice sees the document, Bob doesn't
    assert_eq!(
        alice_query.rows().len(),
        1,
        "Alice should see doc initially"
    );
    assert_eq!(
        bob_query.rows().len(),
        0,
        "Bob should not see doc initially"
    );

    // Move folder from Alice's workspace to Bob's workspace
    db.update_with("folders", folder_id, |b| {
        b.set_ref_by_name("workspace_id", bob_workspace_id).build()
    })
    .unwrap();

    // Now Bob should see it, Alice shouldn't
    let alice_rows_after = alice_query.rows();
    let bob_rows_after = bob_query.rows();

    assert_eq!(
        alice_rows_after.len(),
        0,
        "Alice should not see doc after folder move"
    );
    assert_eq!(
        bob_rows_after.len(),
        1,
        "Bob should see doc after folder move"
    );
    assert_eq!(bob_rows_after[0].0, doc_id);
    assert_eq!(
        bob_rows_after[0].1.get_by_name("title"),
        Some(RowValue::String("Test Doc"))
    );
}

#[test]
fn incremental_query_as_3_hop_chain_new_document_insert() {
    // Test that inserting a new document in a 3-hop chain correctly updates visibility.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE organizations (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE workspaces (name STRING NOT NULL, org_id REFERENCES organizations NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's hierarchy (no documents yet)
    let alice_org_id = db
        .insert_with("organizations", |b| {
            b.set_string_by_name("name", "Alice's Org")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    let alice_workspace_id = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Alice's Workspace")
                .set_ref_by_name("org_id", alice_org_id)
                .build()
        })
        .unwrap();

    let alice_folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Alice's Folder")
                .set_ref_by_name("workspace_id", alice_workspace_id)
                .build()
        })
        .unwrap();

    // Set up 3-hop chain policies
    db.execute("CREATE POLICY ON organizations FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE INHERITS SELECT FROM org_id")
        .unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();
    let bob_query = db
        .incremental_query_as("SELECT * FROM documents", bob_id)
        .unwrap();

    // Initially, no documents
    assert_eq!(
        alice_query.rows().len(),
        0,
        "Alice should see 0 docs initially"
    );
    assert_eq!(bob_query.rows().len(), 0, "Bob should see 0 docs initially");

    // Insert a document in Alice's folder
    let new_doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "New Alice Doc")
                .set_ref_by_name("folder_id", alice_folder_id)
                .build()
        })
        .unwrap();

    // Alice should see the new document, Bob shouldn't
    let alice_rows_after = alice_query.rows();
    let bob_rows_after = bob_query.rows();

    assert_eq!(
        alice_rows_after.len(),
        1,
        "Alice should see new doc after insert"
    );
    assert_eq!(alice_rows_after[0].0, new_doc_id);
    assert_eq!(
        alice_rows_after[0].1.get_by_name("title"),
        Some(RowValue::String("New Alice Doc"))
    );
    assert_eq!(bob_rows_after.len(), 0, "Bob should still see 0 docs");
}

#[test]
fn incremental_query_as_3_hop_chain_with_filter() {
    // Test 3-hop chain with a WHERE clause filter on the source table.
    // This tests that filters are correctly applied in chain queries.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE organizations (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE workspaces (name STRING NOT NULL, org_id REFERENCES organizations NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)",
    )
    .unwrap();
    db.execute("CREATE TABLE documents (title STRING NOT NULL, archived BOOL NOT NULL, folder_id REFERENCES folders NOT NULL)").unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's hierarchy
    let alice_org_id = db
        .insert_with("organizations", |b| {
            b.set_string_by_name("name", "Alice's Org")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    let alice_workspace_id = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Alice's Workspace")
                .set_ref_by_name("org_id", alice_org_id)
                .build()
        })
        .unwrap();

    let alice_folder_id = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Alice's Folder")
                .set_ref_by_name("workspace_id", alice_workspace_id)
                .build()
        })
        .unwrap();

    // Create active and archived documents
    let active_doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Active Doc")
                .set_bool_by_name("archived", false)
                .set_ref_by_name("folder_id", alice_folder_id)
                .build()
        })
        .unwrap();

    let _archived_doc_id = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Archived Doc")
                .set_bool_by_name("archived", true)
                .set_ref_by_name("folder_id", alice_folder_id)
                .build()
        })
        .unwrap();

    // Set up 3-hop chain policies
    db.execute("CREATE POLICY ON organizations FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE INHERITS SELECT FROM org_id")
        .unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    // Query with a filter: only non-archived documents
    let alice_query = db
        .incremental_query_as("SELECT * FROM documents WHERE archived = false", alice_id)
        .unwrap();
    let rows = alice_query.rows();

    // Alice should only see the active document (filter applied)
    assert_eq!(rows.len(), 1, "Alice should see only 1 non-archived doc");
    assert_eq!(rows[0].0, active_doc_id);
    assert_eq!(
        rows[0].1.get_by_name("title"),
        Some(RowValue::String("Active Doc"))
    );
    assert_eq!(
        rows[0].1.get_by_name("archived"),
        Some(RowValue::Bool(false))
    );
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

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE workspaces (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();
    db.execute("CREATE TABLE folders (name STRING NOT NULL, owner_id REFERENCES users, workspace_id REFERENCES workspaces NOT NULL)").unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's workspace
    let alice_ws_id = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Alice's Workspace")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    // Bob's workspace
    let bob_ws_id = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Bob's Workspace")
                .set_ref_by_name("owner_id", bob_id)
                .build()
        })
        .unwrap();

    // Folder in Alice's workspace (no direct owner)
    let folder_in_alice_ws = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Folder in Alice WS")
                .set_ref_by_name("workspace_id", alice_ws_id)
                .build()
        })
        .unwrap();

    // Folder owned by Alice but in Bob's workspace
    let alice_folder_in_bob_ws = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Alice's Folder in Bob WS")
                .set_ref_by_name("owner_id", alice_id)
                .set_ref_by_name("workspace_id", bob_ws_id)
                .build()
        })
        .unwrap();

    // Folder owned by Bob in Bob's workspace
    let bob_folder = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Bob's Folder")
                .set_ref_by_name("owner_id", bob_id)
                .set_ref_by_name("workspace_id", bob_ws_id)
                .build()
        })
        .unwrap();

    // Documents in each folder
    let doc_in_alice_ws = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Doc in Alice WS")
                .set_ref_by_name("folder_id", folder_in_alice_ws)
                .build()
        })
        .unwrap();

    // This doc is in a folder Alice owns but in Bob's workspace.
    // Alice can see it via the OR condition: folders.owner_id = @viewer
    let doc_in_alice_folder = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Doc in Alice's Folder")
                .set_ref_by_name("folder_id", alice_folder_in_bob_ws)
                .build()
        })
        .unwrap();

    let _doc_in_bob_folder = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Doc in Bob's Folder")
                .set_ref_by_name("folder_id", bob_folder)
                .build()
        })
        .unwrap();

    // Set up policies
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE owner_id = @viewer OR INHERITS SELECT FROM workspace_id").unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    // Alice sees documents via both paths:
    // 1. doc → folder → workspace → owner_id = Alice (INHERITS path)
    // 2. doc → folder → owner_id = Alice (OR path at folder level)
    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .expect("Policy chain should work");
    let alice_docs = alice_query.rows();

    // Alice sees 2 documents:
    // - Doc in her workspace (via INHERITS to workspace owner)
    // - Doc in folder she owns (via OR condition on folder owner)
    assert_eq!(alice_docs.len(), 2, "Alice should see 2 documents");
    let alice_doc_ids: std::collections::HashSet<_> = alice_docs.iter().map(|r| r.0).collect();
    assert!(
        alice_doc_ids.contains(&doc_in_alice_ws),
        "Alice should see doc in her workspace"
    );
    assert!(
        alice_doc_ids.contains(&doc_in_alice_folder),
        "Alice should see doc in folder she owns"
    );
}

#[test]
fn policy_chain_multiple_viewers_concurrent() {
    // Test that multiple viewers have independent incremental queries
    // that update correctly when data changes.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE orgs (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE projects (name STRING NOT NULL, org_id REFERENCES orgs NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let charlie_id = match db
        .execute("INSERT INTO users (name) VALUES ('Charlie')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Each user has their own org
    let alice_org = db
        .insert_with("orgs", |b| {
            b.set_string_by_name("name", "Alice Org")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    let bob_org = db
        .insert_with("orgs", |b| {
            b.set_string_by_name("name", "Bob Org")
                .set_ref_by_name("owner_id", bob_id)
                .build()
        })
        .unwrap();

    // Projects in each org
    let alice_project = db
        .insert_with("projects", |b| {
            b.set_string_by_name("name", "Alice Project")
                .set_ref_by_name("org_id", alice_org)
                .build()
        })
        .unwrap();

    let bob_project = db
        .insert_with("projects", |b| {
            b.set_string_by_name("name", "Bob Project")
                .set_ref_by_name("org_id", bob_org)
                .build()
        })
        .unwrap();

    // Set up 2-hop chain policies
    db.execute("CREATE POLICY ON orgs FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON projects FOR SELECT WHERE INHERITS SELECT FROM org_id")
        .unwrap();

    // Create queries for all three viewers
    let alice_query = db
        .incremental_query_as("SELECT * FROM projects", alice_id)
        .unwrap();
    let bob_query = db
        .incremental_query_as("SELECT * FROM projects", bob_id)
        .unwrap();
    let charlie_query = db
        .incremental_query_as("SELECT * FROM projects", charlie_id)
        .unwrap();

    // Initially: Alice sees 1, Bob sees 1, Charlie sees 0
    assert_eq!(alice_query.rows().len(), 1, "Alice should see her project");
    assert_eq!(alice_query.rows()[0].0, alice_project);
    assert_eq!(bob_query.rows().len(), 1, "Bob should see his project");
    assert_eq!(bob_query.rows()[0].0, bob_project);
    assert_eq!(
        charlie_query.rows().len(),
        0,
        "Charlie should see no projects"
    );

    // Transfer Alice's org to Charlie
    db.update_with("orgs", alice_org, |b| {
        b.set_ref_by_name("owner_id", charlie_id).build()
    })
    .unwrap();

    // Now: Alice sees 0, Bob sees 1, Charlie sees 1
    assert_eq!(
        alice_query.rows().len(),
        0,
        "Alice should no longer see her project"
    );
    assert_eq!(
        bob_query.rows().len(),
        1,
        "Bob should still see his project"
    );
    assert_eq!(
        charlie_query.rows().len(),
        1,
        "Charlie should now see Alice's project"
    );
    assert_eq!(charlie_query.rows()[0].0, alice_project);

    // Add a new project to Bob's org
    let new_bob_project = db
        .insert_with("projects", |b| {
            b.set_string_by_name("name", "New Bob Project")
                .set_ref_by_name("org_id", bob_org)
                .build()
        })
        .unwrap();

    // Bob should now see 2 projects
    let bob_rows = bob_query.rows();
    assert_eq!(bob_rows.len(), 2, "Bob should now see 2 projects");
    let bob_project_ids: Vec<_> = bob_rows.iter().map(|r| r.0).collect();
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

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE workspaces (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's workspace
    let alice_ws = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Alice WS")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    // Set up policies before creating folder/docs
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    // Create query before any folders exist
    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();
    assert_eq!(alice_query.rows().len(), 0, "No documents yet");

    // Now create a folder
    let folder = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "New Folder")
                .set_ref_by_name("workspace_id", alice_ws)
                .build()
        })
        .unwrap();

    // Still no documents
    assert_eq!(alice_query.rows().len(), 0, "Still no documents");

    // Add a document to the new folder
    let doc = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "New Doc")
                .set_ref_by_name("folder_id", folder)
                .build()
        })
        .unwrap();

    // Now Alice should see the document
    let rows = alice_query.rows();
    assert_eq!(rows.len(), 1, "Alice should see the new document");
    assert_eq!(rows[0].0, doc);
    assert_eq!(
        rows[0].1.get_by_name("title"),
        Some(RowValue::String("New Doc"))
    );
}

#[test]
fn policy_chain_delete_intermediate_row() {
    // Test that deleting an intermediate row (folder) makes its children invisible.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE workspaces (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE folders (name STRING NOT NULL, workspace_id REFERENCES workspaces NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, folder_id REFERENCES folders NOT NULL)",
    )
    .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let alice_ws = db
        .insert_with("workspaces", |b| {
            b.set_string_by_name("name", "Alice WS")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    let folder = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Folder")
                .set_ref_by_name("workspace_id", alice_ws)
                .build()
        })
        .unwrap();

    let doc = db
        .insert_with("documents", |b| {
            b.set_string_by_name("title", "Doc")
                .set_ref_by_name("folder_id", folder)
                .build()
        })
        .unwrap();

    // Set up policies
    db.execute("CREATE POLICY ON workspaces FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM workspace_id")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    let alice_query = db
        .incremental_query_as("SELECT * FROM documents", alice_id)
        .unwrap();

    // Initially Alice sees the document
    assert_eq!(
        alice_query.rows().len(),
        1,
        "Alice should see doc initially"
    );
    assert_eq!(alice_query.rows()[0].0, doc);

    // Delete the folder (this should cascade to make doc invisible)
    db.delete("folders", folder).unwrap();

    // Alice should no longer see the document
    // Note: The document still exists but its folder reference is now dangling
    // The join fails, so it's not visible
    assert_eq!(
        alice_query.rows().len(),
        0,
        "Alice should not see doc after folder deletion"
    );
}

#[test]
fn policy_chain_4_hop_deep() {
    // Test a 4-hop INHERITS chain:
    // files → folders → projects → orgs → owner_id
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE orgs (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE projects (name STRING NOT NULL, org_id REFERENCES orgs NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE folders (name STRING NOT NULL, project_id REFERENCES projects NOT NULL)",
    )
    .unwrap();
    db.execute("CREATE TABLE files (name STRING NOT NULL, folder_id REFERENCES folders NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's hierarchy
    let alice_org = db
        .insert_with("orgs", |b| {
            b.set_string_by_name("name", "Alice Org")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    let alice_project = db
        .insert_with("projects", |b| {
            b.set_string_by_name("name", "Alice Project")
                .set_ref_by_name("org_id", alice_org)
                .build()
        })
        .unwrap();

    let alice_folder = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Alice Folder")
                .set_ref_by_name("project_id", alice_project)
                .build()
        })
        .unwrap();

    let alice_file = db
        .insert_with("files", |b| {
            b.set_string_by_name("name", "Alice File")
                .set_ref_by_name("folder_id", alice_folder)
                .build()
        })
        .unwrap();

    // Bob's hierarchy
    let bob_org = db
        .insert_with("orgs", |b| {
            b.set_string_by_name("name", "Bob Org")
                .set_ref_by_name("owner_id", bob_id)
                .build()
        })
        .unwrap();

    let bob_project = db
        .insert_with("projects", |b| {
            b.set_string_by_name("name", "Bob Project")
                .set_ref_by_name("org_id", bob_org)
                .build()
        })
        .unwrap();

    let bob_folder = db
        .insert_with("folders", |b| {
            b.set_string_by_name("name", "Bob Folder")
                .set_ref_by_name("project_id", bob_project)
                .build()
        })
        .unwrap();

    let _bob_file = db
        .insert_with("files", |b| {
            b.set_string_by_name("name", "Bob File")
                .set_ref_by_name("folder_id", bob_folder)
                .build()
        })
        .unwrap();

    // Set up 4-hop chain policies
    db.execute("CREATE POLICY ON orgs FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON projects FOR SELECT WHERE INHERITS SELECT FROM org_id")
        .unwrap();
    db.execute("CREATE POLICY ON folders FOR SELECT WHERE INHERITS SELECT FROM project_id")
        .unwrap();
    db.execute("CREATE POLICY ON files FOR SELECT WHERE INHERITS SELECT FROM folder_id")
        .unwrap();

    // Alice should only see her file
    let alice_query = db
        .incremental_query_as("SELECT * FROM files", alice_id)
        .expect("4-hop chain should work");
    let alice_files = alice_query.rows();

    assert_eq!(alice_files.len(), 1, "Alice should see 1 file");
    assert_eq!(alice_files[0].0, alice_file);
    assert_eq!(
        alice_files[0].1.get_by_name("name"),
        Some(RowValue::String("Alice File"))
    );

    // Bob should only see his file
    let bob_query = db
        .incremental_query_as("SELECT * FROM files", bob_id)
        .unwrap();
    assert_eq!(bob_query.rows().len(), 1, "Bob should see 1 file");

    // Transfer org from Alice to Bob
    db.update_with("orgs", alice_org, |b| {
        b.set_ref_by_name("owner_id", bob_id).build()
    })
    .unwrap();

    // Alice should see nothing now
    assert_eq!(
        alice_query.rows().len(),
        0,
        "Alice should see no files after org transfer"
    );

    // Bob should see both files now
    let bob_files = bob_query.rows();
    assert_eq!(
        bob_files.len(),
        2,
        "Bob should see 2 files after org transfer"
    );
    let bob_file_names: Vec<_> = bob_files.iter().map(|r| r.1.get_by_name("name")).collect();
    assert!(bob_file_names.contains(&Some(RowValue::String("Alice File"))));
    assert!(bob_file_names.contains(&Some(RowValue::String("Bob File"))));
}

#[test]
fn policy_chain_update_at_each_level() {
    // Test updates at every level of a 3-hop chain trigger correct propagation.
    use crate::sql::policy::clear_policy_warnings;
    clear_policy_warnings();

    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE orgs (name STRING NOT NULL, owner_id REFERENCES users NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE teams (name STRING NOT NULL, org_id REFERENCES orgs NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE tasks (title STRING NOT NULL, team_id REFERENCES teams NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Alice's hierarchy
    let alice_org = db
        .insert_with("orgs", |b| {
            b.set_string_by_name("name", "Alice Org")
                .set_ref_by_name("owner_id", alice_id)
                .build()
        })
        .unwrap();

    // Bob's hierarchy
    let bob_org = db
        .insert_with("orgs", |b| {
            b.set_string_by_name("name", "Bob Org")
                .set_ref_by_name("owner_id", bob_id)
                .build()
        })
        .unwrap();

    // Team starts in Alice's org
    let team = db
        .insert_with("teams", |b| {
            b.set_string_by_name("name", "The Team")
                .set_ref_by_name("org_id", alice_org)
                .build()
        })
        .unwrap();

    // Task in the team
    let task = db
        .insert_with("tasks", |b| {
            b.set_string_by_name("title", "The Task")
                .set_ref_by_name("team_id", team)
                .build()
        })
        .unwrap();

    // Set up policies
    db.execute("CREATE POLICY ON orgs FOR SELECT WHERE owner_id = @viewer")
        .unwrap();
    db.execute("CREATE POLICY ON teams FOR SELECT WHERE INHERITS SELECT FROM org_id")
        .unwrap();
    db.execute("CREATE POLICY ON tasks FOR SELECT WHERE INHERITS SELECT FROM team_id")
        .unwrap();

    let alice_query = db
        .incremental_query_as("SELECT * FROM tasks", alice_id)
        .unwrap();
    let bob_query = db
        .incremental_query_as("SELECT * FROM tasks", bob_id)
        .unwrap();

    // Initially Alice sees the task
    assert_eq!(alice_query.rows().len(), 1);
    assert_eq!(alice_query.rows()[0].0, task);
    assert_eq!(bob_query.rows().len(), 0);

    // Update 1: Move team to Bob's org
    db.update_with("teams", team, |b| {
        b.set_ref_by_name("org_id", bob_org).build()
    })
    .unwrap();

    assert_eq!(
        alice_query.rows().len(),
        0,
        "Alice should not see task after team moved"
    );
    assert_eq!(
        bob_query.rows().len(),
        1,
        "Bob should see task after team moved"
    );
    assert_eq!(bob_query.rows()[0].0, task);

    // Update 2: Move team back to Alice's org
    db.update_with("teams", team, |b| {
        b.set_ref_by_name("org_id", alice_org).build()
    })
    .unwrap();

    assert_eq!(alice_query.rows().len(), 1, "Alice should see task again");
    assert_eq!(bob_query.rows().len(), 0, "Bob should not see task anymore");

    // Update 3: Transfer org ownership
    db.update_with("orgs", alice_org, |b| {
        b.set_ref_by_name("owner_id", bob_id).build()
    })
    .unwrap();

    assert_eq!(
        alice_query.rows().len(),
        0,
        "Alice should not see task after org transfer"
    );
    assert_eq!(
        bob_query.rows().len(),
        1,
        "Bob should see task after org transfer"
    );

    // Update 4: Update the task itself (should not change visibility)
    db.update_with("tasks", task, |b| {
        b.set_string_by_name("title", "Updated Task").build()
    })
    .unwrap();

    assert_eq!(bob_query.rows().len(), 1);
    assert_eq!(
        bob_query.rows()[0].1.get_by_name("title"),
        Some(RowValue::String("Updated Task"))
    );
}

// ========== Reverse JOIN Tests ==========
// These test JOINs where the JOIN table has the Ref column pointing to the FROM table
// (opposite of the normal case where FROM table has the Ref)

#[test]
fn incremental_query_reverse_join_basic() {
    // Schema: Issues and IssueAssignees (junction table)
    // IssueAssignees.issue references Issues
    // Query: SELECT Issues.* FROM Issues JOIN IssueAssignees ON IssueAssignees.issue = Issues.id
    let db = Database::in_memory();
    db.execute("CREATE TABLE Issues (title STRING NOT NULL, priority STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE Users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE IssueAssignees (issue REFERENCES Issues NOT NULL, user REFERENCES Users NOT NULL)").unwrap();

    // Create user
    let alice_id = match db
        .execute("INSERT INTO Users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Create issues
    let issue1_id = match db
        .execute("INSERT INTO Issues (title, priority) VALUES ('Bug 1', 'high')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    // issue2 is created but unassigned - we don't need its ID
    let _issue2_id = match db
        .execute("INSERT INTO Issues (title, priority) VALUES ('Bug 2', 'low')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Assign Alice to issue1 only (issue2 is unassigned)
    db.insert_with("IssueAssignees", |b| {
        b.set_ref_by_name("issue", issue1_id)
            .set_ref_by_name("user", alice_id)
            .build()
    })
    .unwrap();

    // Create reverse JOIN query: find issues assigned to Alice
    let query = db.incremental_query(
        "SELECT Issues.* FROM Issues JOIN IssueAssignees ON IssueAssignees.issue = Issues.id WHERE IssueAssignees.user = ?"
            .replace("?", &format!("'{}'", alice_id))
            .as_str()
    ).unwrap();

    // Should return issue1 only (assigned to Alice)
    let rows = query.rows();
    assert_eq!(rows.len(), 1, "Should find 1 issue assigned to Alice");
    // The output should be Issues columns: id, title, priority
    assert_eq!(
        rows[0].1.descriptor.columns.len(),
        3,
        "Should have 3 columns from Issues (id, title, priority)"
    );
    assert_eq!(
        rows[0].1.get_by_name("Issues.id"),
        Some(RowValue::Ref(issue1_id))
    );
    assert_eq!(
        rows[0].1.get_by_name("Issues.title"),
        Some(RowValue::String("Bug 1"))
    );
    assert_eq!(
        rows[0].1.get_by_name("Issues.priority"),
        Some(RowValue::String("high"))
    );
}

#[test]
fn incremental_query_reverse_join_no_filter() {
    // Same schema but no WHERE filter - just the JOIN
    let db = Database::in_memory();
    db.execute("CREATE TABLE Issues (title STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE Users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE IssueAssignees (issue REFERENCES Issues NOT NULL, user REFERENCES Users NOT NULL)").unwrap();

    let alice_id = match db
        .execute("INSERT INTO Users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let issue1_id = match db
        .execute("INSERT INTO Issues (title) VALUES ('Bug 1')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let _issue2_id = match db
        .execute("INSERT INTO Issues (title) VALUES ('Bug 2')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Assign Alice to issue1
    db.insert_with("IssueAssignees", |b| {
        b.set_ref_by_name("issue", issue1_id)
            .set_ref_by_name("user", alice_id)
            .build()
    })
    .unwrap();

    // Query without WHERE - should return all issues that have ANY assignee
    let query = db
        .incremental_query(
            "SELECT Issues.* FROM Issues JOIN IssueAssignees ON IssueAssignees.issue = Issues.id",
        )
        .unwrap();

    let rows = query.rows();
    assert_eq!(
        rows.len(),
        1,
        "Should find 1 issue (the one with an assignee)"
    );
    assert_eq!(
        rows[0].1.get_by_name("Issues.title"),
        Some(RowValue::String("Bug 1"))
    );
}

#[test]
fn incremental_query_reverse_join_with_from_table_filter() {
    // Filter on the FROM table (Issues) in a reverse join
    let db = Database::in_memory();
    db.execute("CREATE TABLE Issues (title STRING NOT NULL, priority STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE Users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE IssueAssignees (issue REFERENCES Issues NOT NULL, user REFERENCES Users NOT NULL)").unwrap();

    let alice_id = match db
        .execute("INSERT INTO Users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let issue1_id = match db
        .execute("INSERT INTO Issues (title, priority) VALUES ('Bug 1', 'high')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let issue2_id = match db
        .execute("INSERT INTO Issues (title, priority) VALUES ('Bug 2', 'low')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Assign Alice to both issues
    db.insert_with("IssueAssignees", |b| {
        b.set_ref_by_name("issue", issue1_id)
            .set_ref_by_name("user", alice_id)
            .build()
    })
    .unwrap();
    db.insert_with("IssueAssignees", |b| {
        b.set_ref_by_name("issue", issue2_id)
            .set_ref_by_name("user", alice_id)
            .build()
    })
    .unwrap();

    // Filter by priority on Issues table
    let query = db.incremental_query(
        "SELECT Issues.* FROM Issues JOIN IssueAssignees ON IssueAssignees.issue = Issues.id WHERE Issues.priority = 'low'"
    ).unwrap();

    let rows = query.rows();
    assert_eq!(rows.len(), 1, "Should find 1 low priority issue");
    assert_eq!(
        rows[0].1.get_by_name("Issues.title"),
        Some(RowValue::String("Bug 2"))
    );
    assert_eq!(
        rows[0].1.get_by_name("Issues.priority"),
        Some(RowValue::String("low"))
    );
}

#[test]
fn incremental_query_reverse_join_combined_filters() {
    // Filter on both FROM table and JOIN table
    let db = Database::in_memory();
    db.execute("CREATE TABLE Issues (title STRING NOT NULL, priority STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE Users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE IssueAssignees (issue REFERENCES Issues NOT NULL, user REFERENCES Users NOT NULL)").unwrap();

    let alice_id = match db
        .execute("INSERT INTO Users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO Users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let issue1_id = match db
        .execute("INSERT INTO Issues (title, priority) VALUES ('Bug 1', 'high')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let issue2_id = match db
        .execute("INSERT INTO Issues (title, priority) VALUES ('Bug 2', 'low')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };
    let issue3_id = match db
        .execute("INSERT INTO Issues (title, priority) VALUES ('Bug 3', 'low')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Alice assigned to issue1 (high) and issue2 (low)
    db.insert_with("IssueAssignees", |b| {
        b.set_ref_by_name("issue", issue1_id)
            .set_ref_by_name("user", alice_id)
            .build()
    })
    .unwrap();
    db.insert_with("IssueAssignees", |b| {
        b.set_ref_by_name("issue", issue2_id)
            .set_ref_by_name("user", alice_id)
            .build()
    })
    .unwrap();
    // Bob assigned to issue3 (low)
    db.insert_with("IssueAssignees", |b| {
        b.set_ref_by_name("issue", issue3_id)
            .set_ref_by_name("user", bob_id)
            .build()
    })
    .unwrap();

    // Find low priority issues assigned to Alice
    let query = db.incremental_query(
        &format!(
            "SELECT Issues.* FROM Issues JOIN IssueAssignees ON IssueAssignees.issue = Issues.id WHERE Issues.priority = 'low' AND IssueAssignees.user = '{}'",
            alice_id
        )
    ).unwrap();

    let rows = query.rows();
    assert_eq!(
        rows.len(),
        1,
        "Should find 1 low priority issue assigned to Alice"
    );
    assert_eq!(
        rows[0].1.get_by_name("Issues.title"),
        Some(RowValue::String("Bug 2"))
    );
}

#[test]
fn incremental_query_reverse_join_with_alias() {
    // Test that table aliases work in reverse JOINs
    let db = Database::in_memory();
    db.execute("CREATE TABLE Issues (title STRING NOT NULL, priority STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE Users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE IssueAssignees (issue REFERENCES Issues NOT NULL, user REFERENCES Users NOT NULL)").unwrap();

    let alice_id = match db
        .execute("INSERT INTO Users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let issue1_id = match db
        .execute("INSERT INTO Issues (title, priority) VALUES ('Bug 1', 'high')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Assign Alice to issue1
    db.insert_with("IssueAssignees", |b| {
        b.set_ref_by_name("issue", issue1_id)
            .set_ref_by_name("user", alice_id)
            .build()
    })
    .unwrap();

    // Query with alias "i" for Issues table (matches app pattern)
    let query = db.incremental_query(
        &format!(
            "SELECT i.* FROM Issues i JOIN IssueAssignees ON IssueAssignees.issue = i.id WHERE IssueAssignees.user = '{}'",
            alice_id
        )
    ).unwrap();

    let rows = query.rows();
    assert_eq!(rows.len(), 1, "Should find 1 issue assigned to Alice");
    assert_eq!(
        rows[0].1.get_by_name("Issues.title"),
        Some(RowValue::String("Bug 1"))
    );
    assert_eq!(
        rows[0].1.get_by_name("Issues.priority"),
        Some(RowValue::String("high"))
    );
}

#[test]
fn incremental_query_reverse_join_subscribe() {
    // Test that subscribe works for reverse JOINs - this mirrors what the WASM layer does
    use std::sync::atomic::{AtomicUsize, Ordering};

    let db = Database::in_memory();
    db.execute("CREATE TABLE Issues (title STRING NOT NULL, priority STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE Users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE IssueAssignees (issue REFERENCES Issues NOT NULL, user REFERENCES Users NOT NULL)").unwrap();

    let alice_id = match db
        .execute("INSERT INTO Users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let issue1_id = match db
        .execute("INSERT INTO Issues (title, priority) VALUES ('Bug 1', 'high')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Assign Alice to issue1
    db.insert_with("IssueAssignees", |b| {
        b.set_ref_by_name("issue", issue1_id)
            .set_ref_by_name("user", alice_id)
            .build()
    })
    .unwrap();

    // Create query with alias
    let query = db.incremental_query(
        &format!(
            "SELECT i.* FROM Issues i JOIN IssueAssignees ON IssueAssignees.issue = i.id WHERE IssueAssignees.user = '{}'",
            alice_id
        )
    ).unwrap();

    // Subscribe with delta callback (like WASM does)
    let callback_count = Arc::new(AtomicUsize::new(0));
    let callback_count_clone = callback_count.clone();
    let initial_count = Arc::new(AtomicUsize::new(0));
    let initial_count_clone = initial_count.clone();

    let _listener = query.subscribe(Box::new(move |delta| {
        callback_count_clone.fetch_add(1, Ordering::SeqCst);
        initial_count_clone.fetch_add(delta.len(), Ordering::SeqCst);
    }));

    // Callback should have been called immediately with initial data
    assert_eq!(
        callback_count.load(Ordering::SeqCst),
        1,
        "Callback should be called once on subscribe"
    );
    assert_eq!(
        initial_count.load(Ordering::SeqCst),
        1,
        "Initial delta should contain 1 row"
    );
}

// ========== Migration Execution Tests ==========

#[test]
fn migration_rename_column() {
    use crate::sql::lens::LensGenerationOptions;
    use crate::sql::schema::{ColumnDef, ColumnType, TableSchema};

    let db = Database::in_memory();

    // Create table with 'title' column
    db.execute("CREATE TABLE documents (title STRING NOT NULL)")
        .unwrap();

    // Insert a row
    let schema = db.get_table("documents").unwrap();
    let desc = Arc::new(RowDescriptor::from_table_schema(&schema));
    let row = RowBuilder::new(desc)
        .set_string_by_name("title", "My Document")
        .build();
    let row_id = db.insert_row("documents", row).unwrap();

    // Create new schema with 'name' instead of 'title'
    let new_schema = TableSchema::new(
        "documents",
        vec![ColumnDef::required("name", ColumnType::String)],
    );

    // Execute migration with confirmed rename
    let options = LensGenerationOptions {
        confirmed_renames: vec![("title".into(), "name".into())],
    };
    let result = db
        .execute_migration("documents", new_schema.clone(), options)
        .unwrap();

    // Verify migration results
    assert_eq!(result.migrated_count, 1);
    assert_eq!(result.invisible_count, 0);
    assert!(result.warnings.is_empty());

    // Verify the row has the new column name
    let (_, migrated_row) = db.get("documents", row_id).unwrap().unwrap();
    assert_eq!(
        migrated_row.get_by_name("name"),
        Some(RowValue::String("My Document"))
    );
    // Old column should not exist
    assert_eq!(migrated_row.get_by_name("title"), None);

    // Verify the schema was updated
    let updated_schema = db.get_table("documents").unwrap();
    assert!(updated_schema.column("name").is_some());
    assert!(updated_schema.column("title").is_none());
}

#[test]
fn migration_add_column() {
    use crate::sql::lens::LensGenerationOptions;
    use crate::sql::schema::{ColumnDef, ColumnType, TableSchema};

    let db = Database::in_memory();

    // Create table with 'name' column
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();

    // Insert a row
    let schema = db.get_table("users").unwrap();
    let desc = Arc::new(RowDescriptor::from_table_schema(&schema));
    let row = RowBuilder::new(desc)
        .set_string_by_name("name", "Alice")
        .build();
    let row_id = db.insert_row("users", row).unwrap();

    // Create new schema with added 'email' column (nullable)
    let new_schema = TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::optional("email", ColumnType::String),
        ],
    );

    // Execute migration
    let result = db
        .execute_migration("users", new_schema, LensGenerationOptions::default())
        .unwrap();

    // Verify migration results
    assert_eq!(result.migrated_count, 1);
    assert_eq!(result.invisible_count, 0);

    // Verify the row has the new column (with NULL default)
    let (_, migrated_row) = db.get("users", row_id).unwrap().unwrap();
    assert_eq!(
        migrated_row.get_by_name("name"),
        Some(RowValue::String("Alice"))
    );
    assert_eq!(migrated_row.get_by_name("email"), Some(RowValue::Null));
}

#[test]
fn migration_preview() {
    use crate::sql::lens::{ColumnTransform, LensGenerationOptions};
    use crate::sql::schema::{ColumnDef, ColumnType, TableSchema};

    let db = Database::in_memory();

    // Create table
    db.execute("CREATE TABLE items (title STRING NOT NULL)")
        .unwrap();

    // Create new schema
    let new_schema = TableSchema::new(
        "items",
        vec![ColumnDef::required("name", ColumnType::String)],
    );

    // Preview without executing
    let options = LensGenerationOptions {
        confirmed_renames: vec![("title".into(), "name".into())],
    };
    let (lens, warnings) = db
        .preview_migration("items", &new_schema, &options)
        .unwrap();

    // Verify lens was generated correctly
    assert_eq!(lens.forward.len(), 1);
    assert!(matches!(
        &lens.forward[0],
        ColumnTransform::Rename { from, to } if from == "title" && to == "name"
    ));
    assert!(warnings.is_empty());
}

#[test]
fn migration_descriptor_chain() {
    use crate::sql::lens::LensGenerationOptions;
    use crate::sql::schema::{ColumnDef, ColumnType, TableSchema};

    let db = Database::in_memory();

    // Create table
    db.execute("CREATE TABLE notes (text STRING NOT NULL)")
        .unwrap();

    // Get initial descriptor
    let desc_v1 = db.get_descriptor("notes").unwrap();
    let id_v1 = db.get_descriptor_id("notes").unwrap();
    assert!(desc_v1.parent_descriptors.is_empty());

    // Migrate: add optional column
    let new_schema = TableSchema::new(
        "notes",
        vec![
            ColumnDef::required("text", ColumnType::String),
            ColumnDef::optional("color", ColumnType::String),
        ],
    );

    db.execute_migration("notes", new_schema, LensGenerationOptions::default())
        .unwrap();

    // Get new descriptor - should have parent pointer
    let desc_v2 = db.get_descriptor("notes").unwrap();
    assert_eq!(desc_v2.parent_descriptors.len(), 1);
    assert_eq!(desc_v2.parent_descriptors[0], id_v1);
    assert_eq!(desc_v2.lenses.len(), 1);
}

#[test]
fn incremental_query_two_reverse_joins_combined_filters() {
    // Test that queries with TWO reverse joins work correctly
    // This reproduces the exact scenario that fails in CI but passes locally
    // Now includes a forward join to Projects, matching the TypeScript-generated query
    let db = Database::in_memory();
    db.execute("CREATE TABLE Projects (name STRING NOT NULL, color STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE Issues (title STRING NOT NULL, priority STRING NOT NULL, project REFERENCES Projects NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE Users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE Labels (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE IssueAssignees (issue REFERENCES Issues NOT NULL, user REFERENCES Users NOT NULL)").unwrap();
    db.execute(
        "CREATE TABLE IssueLabels (issue REFERENCES Issues NOT NULL, label REFERENCES Labels NOT NULL)",
    )
    .unwrap();

    let project_id = match db
        .execute("INSERT INTO Projects (name, color) VALUES ('Test Project', '#00ff00')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let alice_id = match db
        .execute("INSERT INTO Users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let bug_label_id = match db
        .execute("INSERT INTO Labels (name) VALUES ('Bug')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    // Insert issue with project reference
    let issue1_id = db
        .insert_with("Issues", |b| {
            b.set_string_by_name("title", "Test Issue")
                .set_string_by_name("priority", "high")
                .set_ref_by_name("project", project_id)
                .build()
        })
        .unwrap();

    // Create junction table entries
    db.insert_with("IssueLabels", |b| {
        b.set_ref_by_name("issue", issue1_id)
            .set_ref_by_name("label", bug_label_id)
            .build()
    })
    .unwrap();

    db.insert_with("IssueAssignees", |b| {
        b.set_ref_by_name("issue", issue1_id)
            .set_ref_by_name("user", alice_id)
            .build()
    })
    .unwrap();

    // Test with the EXACT TypeScript-generated SQL pattern including aliases
    // This matches what packages/jazz-schema/src/runtime.ts buildQuery() produces
    let sql_ts_style = format!(
        "SELECT i.id, i.title, i.priority, i.project, \
         Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label FROM IssueLabels i_inner WHERE i_inner.issue = i.id) as IssueLabels, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user FROM IssueAssignees i_inner WHERE i_inner.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         WHERE i.priority = 'high' AND IssueLabels.label = '{}' AND IssueAssignees.user = '{}'",
        bug_label_id, alice_id
    );
    eprintln!("TypeScript-style SQL: {}", sql_ts_style);

    // Also test without aliases (simpler case that should also work)
    let sql = format!(
        "SELECT Issues.id, Issues.title, Issues.priority, Issues.project, \
         Projects as project \
         FROM Issues \
         JOIN Projects ON Issues.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = Issues.id \
         JOIN IssueAssignees ON IssueAssignees.issue = Issues.id \
         WHERE Issues.priority = 'high' AND IssueLabels.label = '{}' AND IssueAssignees.user = '{}'",
        bug_label_id, alice_id
    );
    eprintln!("SQL: {}", sql);

    // First, let's verify the simpler queries work
    let query_issues_only = db.incremental_query("SELECT Issues.* FROM Issues").unwrap();
    eprintln!("Issues only: {} rows", query_issues_only.rows().len());

    // Forward join only
    let query_forward_join = db
        .incremental_query(
            "SELECT Issues.*, Projects FROM Issues \
         JOIN Projects ON Issues.project = Projects.id",
        )
        .unwrap();
    eprintln!(
        "Forward join (Projects): {} rows",
        query_forward_join.rows().len()
    );

    let query_one_join = db
        .incremental_query(&format!(
            "SELECT Issues.* FROM Issues \
             JOIN IssueLabels ON IssueLabels.issue = Issues.id \
             WHERE IssueLabels.label = '{}'",
            bug_label_id
        ))
        .unwrap();
    eprintln!(
        "One reverse join (IssueLabels): {} rows",
        query_one_join.rows().len()
    );

    let query_other_join = db
        .incremental_query(&format!(
            "SELECT Issues.* FROM Issues \
             JOIN IssueAssignees ON IssueAssignees.issue = Issues.id \
             WHERE IssueAssignees.user = '{}'",
            alice_id
        ))
        .unwrap();
    eprintln!(
        "One reverse join (IssueAssignees): {} rows",
        query_other_join.rows().len()
    );

    // Query with two reverse joins and combined filters
    let query = db.incremental_query(&sql).unwrap();

    let rows = query.rows();
    eprintln!("Two joins query: {} rows", rows.len());
    assert_eq!(
        rows.len(),
        1,
        "Should find 1 issue with high priority, Bug label, and assigned to Alice. Got {} rows instead.",
        rows.len()
    );
    // Print column names to debug
    eprintln!("Row columns:");
    for (i, col) in rows[0].1.descriptor.columns.iter().enumerate() {
        eprintln!("  {}: {:?} = {:?}", i, col.name, rows[0].1.get(i));
    }
    // The column name might be "title" without the alias prefix
    let title = rows[0]
        .1
        .get_by_name("title")
        .or_else(|| rows[0].1.get_by_name("i.title"))
        .or_else(|| rows[0].1.get_by_name("Issues.title"));
    assert_eq!(
        title,
        Some(RowValue::String("Test Issue")),
        "Expected to find title column with value 'Test Issue'"
    );

    // Also test with subscribe to match TypeScript behavior
    // The initial delta should contain all existing matching rows
    use std::sync::{Arc, Mutex};
    let delta_count = Arc::new(Mutex::new(0usize));
    let delta_count_clone = Arc::clone(&delta_count);
    let _listener = query.subscribe(Box::new(move |delta_batch| {
        eprintln!("Delta callback with {} deltas", delta_batch.len());
        for delta in delta_batch.iter() {
            eprintln!("  Delta: {:?}", delta);
        }
        *delta_count_clone.lock().unwrap() = delta_batch.len();
    }));
    // Subscribe callback should be called synchronously with existing data
    assert_eq!(
        *delta_count.lock().unwrap(),
        1,
        "Initial delta should contain 1 row for the matching issue"
    );

    // Also test the TypeScript-style SQL with aliases
    let query_ts = db.incremental_query(&sql_ts_style).unwrap();
    let rows_ts = query_ts.rows();
    eprintln!("TypeScript-style query: {} rows", rows_ts.len());
    assert_eq!(
        rows_ts.len(),
        1,
        "TypeScript-style SQL should find 1 issue. Got {} rows instead.",
        rows_ts.len()
    );
}

#[test]
fn incremental_query_two_reverse_joins_combined_filters_run_100_times() {
    // Run the same test 100 times to check for non-determinism
    for i in 0..100 {
        let db = Database::in_memory();
        db.execute("CREATE TABLE Issues (title STRING NOT NULL, priority STRING NOT NULL)")
            .unwrap();
        db.execute("CREATE TABLE Users (name STRING NOT NULL)")
            .unwrap();
        db.execute("CREATE TABLE Labels (name STRING NOT NULL)")
            .unwrap();
        db.execute("CREATE TABLE IssueAssignees (issue REFERENCES Issues NOT NULL, user REFERENCES Users NOT NULL)").unwrap();
        db.execute(
            "CREATE TABLE IssueLabels (issue REFERENCES Issues NOT NULL, label REFERENCES Labels NOT NULL)",
        )
        .unwrap();

        let alice_id = match db
            .execute("INSERT INTO Users (name) VALUES ('Alice')")
            .unwrap()
        {
            ExecuteResult::Inserted { row_id: id, .. } => id,
            _ => panic!("expected Inserted"),
        };

        let bug_label_id = match db
            .execute("INSERT INTO Labels (name) VALUES ('Bug')")
            .unwrap()
        {
            ExecuteResult::Inserted { row_id: id, .. } => id,
            _ => panic!("expected Inserted"),
        };

        let issue1_id = match db
            .execute("INSERT INTO Issues (title, priority) VALUES ('Test Issue', 'high')")
            .unwrap()
        {
            ExecuteResult::Inserted { row_id: id, .. } => id,
            _ => panic!("expected Inserted"),
        };

        db.insert_with("IssueLabels", |b| {
            b.set_ref_by_name("issue", issue1_id)
                .set_ref_by_name("label", bug_label_id)
                .build()
        })
        .unwrap();

        db.insert_with("IssueAssignees", |b| {
            b.set_ref_by_name("issue", issue1_id)
                .set_ref_by_name("user", alice_id)
                .build()
        })
        .unwrap();

        let sql = format!(
            "SELECT Issues.* FROM Issues \
             JOIN IssueLabels ON IssueLabels.issue = Issues.id \
             JOIN IssueAssignees ON IssueAssignees.issue = Issues.id \
             WHERE Issues.priority = 'high' AND IssueLabels.label = '{}' AND IssueAssignees.user = '{}'",
            bug_label_id, alice_id
        );

        // For debugging on first iteration only
        if i == 0 {
            eprintln!("=== Iteration 0 Debug ===");
            eprintln!("SQL: {}", sql);

            // Test simpler queries first
            let q_issues = db.incremental_query("SELECT Issues.* FROM Issues").unwrap();
            eprintln!("Issues only: {} rows", q_issues.rows().len());

            let q_one = db.incremental_query(&format!(
                "SELECT Issues.* FROM Issues JOIN IssueLabels ON IssueLabels.issue = Issues.id WHERE IssueLabels.label = '{}'",
                bug_label_id
            )).unwrap();
            eprintln!("One join: {} rows", q_one.rows().len());
            // Print row columns from one join
            if let Some(row) = q_one.rows().first() {
                eprintln!("One join row columns:");
                for (i, col) in row.1.descriptor.columns.iter().enumerate() {
                    eprintln!("  {}: {} = {:?}", i, col.name, row.1.get(i));
                }
            }
        }

        let query = db.incremental_query(&sql).unwrap();

        let rows = query.rows();
        assert_eq!(
            rows.len(),
            1,
            "Iteration {}: Should find 1 issue. Got {} rows instead.",
            i,
            rows.len()
        );
    }
}

#[test]
fn incremental_query_exact_typescript_sql_pattern() {
    // Test the exact SQL pattern generated by TypeScript, which has 3 JOINs:
    // 1. Forward join: Projects ON i.project = Projects.id
    // 2. Reverse join: IssueLabels ON IssueLabels.issue = i.id
    // 3. Reverse join: IssueAssignees ON IssueAssignees.issue = i.id
    let db = Database::in_memory();

    // Create tables matching the TypeScript schema
    db.execute("CREATE TABLE Projects (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE Users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE Labels (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE Issues (title STRING NOT NULL, priority STRING NOT NULL, project REFERENCES Projects NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE IssueLabels (issue REFERENCES Issues NOT NULL, label REFERENCES Labels NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE IssueAssignees (issue REFERENCES Issues NOT NULL, user REFERENCES Users NOT NULL)")
        .unwrap();

    // Insert test data
    let user_id = match db
        .execute("INSERT INTO Users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let project_id = match db
        .execute("INSERT INTO Projects (name) VALUES ('Test Project')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let label_id = match db
        .execute("INSERT INTO Labels (name) VALUES ('Bug')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let issue_id = match db
        .execute(&format!(
            "INSERT INTO Issues (title, priority, project) VALUES ('Test Issue', 'high', '{}')",
            project_id
        ))
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    db.insert_with("IssueLabels", |b| {
        b.set_ref_by_name("issue", issue_id)
            .set_ref_by_name("label", label_id)
            .build()
    })
    .unwrap();

    db.insert_with("IssueAssignees", |b| {
        b.set_ref_by_name("issue", issue_id)
            .set_ref_by_name("user", user_id)
            .build()
    })
    .unwrap();

    eprintln!("=== IDs ===");
    eprintln!("user_id: {}", user_id);
    eprintln!("project_id: {}", project_id);
    eprintln!("label_id: {}", label_id);
    eprintln!("issue_id: {}", issue_id);

    // Build the exact SQL pattern from TypeScript (copied from debug output)
    // Key difference from previous test: this uses "Projects as project" syntax
    // for including full rows, and ARRAY subqueries for reverse refs
    let sql = format!(
        "SELECT i.id, i.title, i.priority, i.project, \
         Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         WHERE i.priority = 'high' AND IssueLabels.label = '{}' AND IssueAssignees.user = '{}'",
        label_id, user_id
    );

    eprintln!("=== SQL ===");
    eprintln!("{}", sql);

    // Test progressively more complex queries to find which part fails

    // Step 1: Simple query
    let q1 = db.incremental_query("SELECT Issues.* FROM Issues").unwrap();
    eprintln!("Step 1 (Issues only): {} rows", q1.rows().len());

    // Step 2: With alias
    let q2 = db.incremental_query("SELECT i.* FROM Issues i").unwrap();
    eprintln!("Step 2 (with alias): {} rows", q2.rows().len());

    // Step 3: With forward join (Projects)
    let q3 = db
        .incremental_query("SELECT i.* FROM Issues i JOIN Projects ON i.project = Projects.id")
        .unwrap();
    eprintln!("Step 3 (+ Projects join): {} rows", q3.rows().len());

    // Step 4: With one reverse join (IssueLabels) and filter
    let q4 = db.incremental_query(&format!(
        "SELECT i.* FROM Issues i JOIN IssueLabels ON IssueLabels.issue = i.id WHERE IssueLabels.label = '{}'",
        label_id
    )).unwrap();
    eprintln!("Step 4 (+ IssueLabels filter): {} rows", q4.rows().len());

    // Step 5: With forward join + one reverse join filter
    let q5 = db
        .incremental_query(&format!(
            "SELECT i.* FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         WHERE IssueLabels.label = '{}'",
            label_id
        ))
        .unwrap();
    eprintln!("Step 5 (Projects + IssueLabels): {} rows", q5.rows().len());

    // Step 6: With two reverse joins but no forward join
    let q6 = db
        .incremental_query(&format!(
            "SELECT i.* FROM Issues i \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         WHERE IssueLabels.label = '{}' AND IssueAssignees.user = '{}'",
            label_id, user_id
        ))
        .unwrap();
    eprintln!(
        "Step 6 (two reverse joins, no forward): {} rows",
        q6.rows().len()
    );

    // Step 7: With all three joins
    let q7 = db
        .incremental_query(&format!(
            "SELECT i.* FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         WHERE IssueLabels.label = '{}' AND IssueAssignees.user = '{}'",
            label_id, user_id
        ))
        .unwrap();
    eprintln!("Step 7 (all three joins): {} rows", q7.rows().len());

    // Step 8: Adding "Projects as project" syntax
    let q8 = db
        .incremental_query(&format!(
            "SELECT i.*, Projects as project FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         WHERE IssueLabels.label = '{}' AND IssueAssignees.user = '{}'",
            label_id, user_id
        ))
        .unwrap();
    eprintln!("Step 8 (+ Projects as project): {} rows", q8.rows().len());

    // Step 9: Adding just one ARRAY subquery (no nested "as label")
    let q9 = db
        .incremental_query(&format!(
            "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label \
               FROM IssueLabels i_inner WHERE i_inner.issue = i.id) as IssueLabels \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         WHERE IssueLabels.label = '{}' AND IssueAssignees.user = '{}'",
            label_id, user_id
        ))
        .unwrap();
    eprintln!("Step 9 (+ simple ARRAY): {} rows", q9.rows().len());

    // Step 10: Adding ARRAY subquery with nested join
    let q10 = db.incremental_query(&format!(
        "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         WHERE IssueLabels.label = '{}' AND IssueAssignees.user = '{}'",
        label_id, user_id
    )).unwrap();
    eprintln!("Step 10 (+ ARRAY with join): {} rows", q10.rows().len());

    // Step 11: Two ARRAY subqueries (this fails!)
    let q11 = db.incremental_query(&format!(
        "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         WHERE IssueLabels.label = '{}' AND IssueAssignees.user = '{}'",
        label_id, user_id
    )).unwrap();
    eprintln!("Step 11 (+ two ARRAYs): {} rows", q11.rows().len());

    // Step 11a: Two ARRAY subqueries WITHOUT the filter joins (just to test ARRAY)
    let q11a = db.incremental_query(
        "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id"
    ).unwrap();
    eprintln!(
        "Step 11a (two ARRAYs, no filter joins): {} rows",
        q11a.rows().len()
    );

    // Step 11b: Two ARRAY subqueries WITH filter joins but NO WHERE clause
    let q11b = db.incremental_query(
        "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id"
    ).unwrap();
    eprintln!(
        "Step 11b (two ARRAYs + joins, no WHERE): {} rows",
        q11b.rows().len()
    );

    // Step 11c: Two ARRAY subqueries WITH filter joins and only label filter
    let q11c = db.incremental_query(&format!(
        "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         WHERE IssueLabels.label = '{}'",
        label_id
    )).unwrap();
    eprintln!(
        "Step 11c (two ARRAYs + only label filter): {} rows",
        q11c.rows().len()
    );

    // Step 11d: Two ARRAY subqueries WITH filter joins and only user filter
    let q11d = db.incremental_query(&format!(
        "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         WHERE IssueAssignees.user = '{}'",
        user_id
    )).unwrap();
    eprintln!(
        "Step 11d (two ARRAYs + only user filter): {} rows",
        q11d.rows().len()
    );

    // Step 11e: Two ARRAYs + only one filter join (IssueLabels)
    let q11e = db.incremental_query(
        "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id"
    ).unwrap();
    eprintln!(
        "Step 11e (two ARRAYs + only IssueLabels join): {} rows",
        q11e.rows().len()
    );

    // Step 11f: Two ARRAYs + only one filter join (IssueAssignees)
    let q11f = db.incremental_query(
        "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id"
    ).unwrap();
    eprintln!(
        "Step 11f (two ARRAYs + only IssueAssignees join): {} rows",
        q11f.rows().len()
    );

    // Step 11g: Swap ARRAY order - IssueAssignees first, IssueLabels second
    let q11g = db.incremental_query(
        "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id"
    ).unwrap();
    eprintln!(
        "Step 11g (SWAPPED ARRAY order + IssueAssignees join): {} rows",
        q11g.rows().len()
    );

    // Step 11h: Swap ARRAY order - IssueAssignees first, IssueLabels second + IssueLabels join
    let q11h = db.incremental_query(
        "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id"
    ).unwrap();
    eprintln!(
        "Step 11h (SWAPPED ARRAY order + IssueLabels join): {} rows",
        q11h.rows().len()
    );

    // Step 11i: Swap JOIN order but keep ARRAY order same
    let q11i = db.incremental_query(
        "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id"
    ).unwrap();
    eprintln!(
        "Step 11i (two joins, IssueAssignees first): {} rows",
        q11i.rows().len()
    );

    // Step 12: Same as full SQL but with i.* instead of specific columns
    let q12 = db.incremental_query(&format!(
        "SELECT i.*, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         WHERE i.priority = 'high' AND IssueLabels.label = '{}' AND IssueAssignees.user = '{}'",
        label_id, user_id
    )).unwrap();
    eprintln!("Step 12 (i.* + priority filter): {} rows", q12.rows().len());

    // Step 13: Full SQL but with explicit columns
    let q13 = db.incremental_query(&format!(
        "SELECT i.id, i.title, i.priority, i.project, Projects as project, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.label, Labels as label \
               FROM IssueLabels i_inner JOIN Labels ON i_inner.label = Labels.id WHERE i_inner.issue = i.id) as IssueLabels, \
         ARRAY(SELECT i_inner.id, i_inner.issue, i_inner.user, Users as user \
               FROM IssueAssignees i_inner JOIN Users ON i_inner.user = Users.id WHERE i_inner.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         JOIN IssueAssignees ON IssueAssignees.issue = i.id \
         WHERE i.priority = 'high' AND IssueLabels.label = '{}' AND IssueAssignees.user = '{}'",
        label_id, user_id
    )).unwrap();
    eprintln!("Step 13 (explicit columns): {} rows", q13.rows().len());

    // Test rows() first
    let query = db.incremental_query(&sql).unwrap();
    let rows = query.rows();

    eprintln!("=== Query Result ===");
    eprintln!("rows.len() = {}", rows.len());
    for (i, row) in rows.iter().enumerate() {
        eprintln!("  row {}: id={}", i, row.0);
    }

    assert_eq!(
        rows.len(),
        1,
        "TypeScript SQL pattern should return 1 row. Got {} rows instead.",
        rows.len()
    );

    // Test subscribe() as well (this is what TypeScript uses)
    use std::sync::{Arc, Mutex};
    let delta_count = Arc::new(Mutex::new(0usize));
    let delta_count_clone = Arc::clone(&delta_count);
    let _listener = query.subscribe(Box::new(move |delta_batch| {
        eprintln!("Delta callback: {} deltas", delta_batch.len());
        *delta_count_clone.lock().unwrap() = delta_batch.len();
    }));

    assert_eq!(
        *delta_count.lock().unwrap(),
        1,
        "Initial delta should contain 1 row"
    );
}

#[test]
fn build_lens_context_for_table_after_migration() {
    // GCO-1096: Test that lens context is properly built after migration
    use crate::sql::lens::LensGenerationOptions;
    use crate::sql::schema::{ColumnDef, ColumnType, TableSchema};

    let db = Database::in_memory();

    // Create table with 'title' column
    db.execute("CREATE TABLE documents (title STRING NOT NULL)")
        .unwrap();

    // Insert a row
    let schema = db.get_table("documents").unwrap();
    let desc = Arc::new(RowDescriptor::from_table_schema(&schema));
    let row = RowBuilder::new(desc)
        .set_string_by_name("title", "My Document")
        .build();
    let _row_id = db.insert_row("documents", row).unwrap();

    // Get initial descriptor ID
    let desc_v1_id = db.get_descriptor_id("documents").unwrap();

    // Verify lens context is empty before migration (no parents)
    let ctx_before = db.state().build_lens_context_for_table("documents");
    assert!(
        ctx_before.get_lens(&desc_v1_id, &desc_v1_id).is_none(),
        "No lens should exist for same schema version"
    );

    // Create new schema with 'name' instead of 'title'
    let new_schema = TableSchema::new(
        "documents",
        vec![ColumnDef::required("name", ColumnType::String)],
    );

    // Execute migration with confirmed rename
    let options = LensGenerationOptions {
        confirmed_renames: vec![("title".into(), "name".into())],
    };
    db.execute_migration("documents", new_schema.clone(), options)
        .unwrap();

    // Get new descriptor ID
    let desc_v2_id = db.get_descriptor_id("documents").unwrap();
    assert_ne!(
        desc_v1_id, desc_v2_id,
        "New descriptor should have different ID"
    );

    // Verify lens context now has the lens from v1 → v2
    let ctx_after = db.state().build_lens_context_for_table("documents");
    let lens = ctx_after.get_lens(&desc_v1_id, &desc_v2_id);
    assert!(
        lens.is_some(),
        "Lens should exist from old schema to new schema"
    );

    // Verify the descriptor parent chain is correct
    let desc_v2 = db.get_descriptor("documents").unwrap();
    assert_eq!(desc_v2.parent_descriptors.len(), 1);
    assert_eq!(desc_v2.parent_descriptors[0], desc_v1_id);

    // Verify we can also load row descriptors by ID
    let row_desc_v1 = db.state().load_row_descriptor_by_id(desc_v1_id);
    let row_desc_v2 = db.state().load_row_descriptor_by_id(desc_v2_id);

    assert!(
        row_desc_v1.is_some(),
        "Should be able to load v1 row descriptor"
    );
    assert!(
        row_desc_v2.is_some(),
        "Should be able to load v2 row descriptor"
    );

    // v1 should have 'title', v2 should have 'name'
    assert!(
        row_desc_v1.unwrap().column("title").is_some(),
        "v1 descriptor should have 'title' column"
    );
    assert!(
        row_desc_v2.unwrap().column("name").is_some(),
        "v2 descriptor should have 'name' column"
    );
}
