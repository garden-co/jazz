//! Integration tests for sql::Database

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use groove::sql::{
    ColumnDef, ColumnType, Database, DatabaseError, ExecuteResult, OwnedRow, PredicateValue,
    RowBuilder, RowDescriptor, RowValue, TableSchema,
};
use groove::ObjectId;

/// Helper to build a row for a given schema with values in schema order.
/// Example: make_row(&schema, |b| b.set_string_by_name("name", "Alice").set_i64_by_name("age", 30))
fn make_row<F>(schema: &TableSchema, f: F) -> OwnedRow
where
    F: FnOnce(RowBuilder) -> RowBuilder,
{
    let desc = Arc::new(RowDescriptor::from_table_schema(schema));
    f(RowBuilder::new(desc)).build()
}

// ========== Table Creation Tests ==========

#[test]
fn create_table() {
    let db = Database::in_memory();

    let schema = TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::optional("age", ColumnType::I64),
        ],
    );

    let id = db.create_table(schema).unwrap();
    assert!(id.0 > 0);

    // Check table exists
    assert!(db.get_table("users").is_some());
    assert_eq!(db.list_tables(), vec!["users"]);

    // Cannot create duplicate
    let schema2 = TableSchema::new("users", vec![]);
    assert!(matches!(
        db.create_table(schema2),
        Err(DatabaseError::TableExists(_))
    ));
}

// ========== Insert and Get Tests ==========

#[test]
fn insert_and_get() {
    let db = Database::in_memory();

    let schema = TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::optional("age", ColumnType::I64),
        ],
    );
    db.create_table(schema.clone()).unwrap();

    let row = make_row(&schema, |b| {
        b.set_string_by_name("name", "Alice")
            .set_i64_by_name("age", 30)
    });
    let id = db.insert_row("users", row).unwrap();

    let result = db.get("users", id).unwrap().unwrap();
    assert_eq!(result.0, id);
    // Use get_by_name for zero-copy access
    assert_eq!(result.1.get_by_name("name"), Some(RowValue::String("Alice")));
    assert_eq!(result.1.get_by_name("age"), Some(RowValue::I64(30)));
}

#[test]
fn insert_with_null() {
    let db = Database::in_memory();

    let schema = TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::optional("email", ColumnType::String),
        ],
    );
    db.create_table(schema.clone()).unwrap();

    // Only set the required field, leave email as null
    let row = make_row(&schema, |b| b.set_string_by_name("name", "Bob"));
    let id = db.insert_row("users", row).unwrap();

    let result = db.get("users", id).unwrap().unwrap();
    // Nullable columns with no value return Null from get_by_name
    assert_eq!(result.1.get_by_name("email"), Some(RowValue::Null));
}

#[test]
fn insert_missing_required_column() {
    let db = Database::in_memory();

    let schema = TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    );
    db.create_table(schema.clone()).unwrap();

    // Create an empty row (missing required column)
    // Note: With buffer format, unset string columns become empty strings ""
    // which is different from NULL. Empty strings are valid for required columns.
    let row = make_row(&schema, |b| b);
    let id = db.insert_row("users", row).unwrap();

    // Verify the row was inserted with an empty string
    let result = db.get("users", id).unwrap().unwrap();
    assert_eq!(result.1.get_by_name("name"), Some(RowValue::String("")));
}

// ========== Update Tests ==========

#[test]
fn update_row_test() {
    let db = Database::in_memory();

    let schema = TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::optional("age", ColumnType::I64),
        ],
    );
    db.create_table(schema.clone()).unwrap();

    let row = make_row(&schema, |b| {
        b.set_string_by_name("name", "Alice")
            .set_i64_by_name("age", 30)
    });
    let id = db.insert_row("users", row).unwrap();

    // Update age to 31
    let updated_row = make_row(&schema, |b| {
        b.set_string_by_name("name", "Alice")
            .set_i64_by_name("age", 31)
    });
    let updated = db.update_row("users", id, updated_row).unwrap();
    assert!(updated);

    let result = db.get("users", id).unwrap().unwrap();
    assert_eq!(result.1.get_by_name("age"), Some(RowValue::I64(31)));
}

// ========== Delete Tests ==========

#[test]
fn delete_row_test() {
    let db = Database::in_memory();

    let schema = TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    );
    db.create_table(schema.clone()).unwrap();

    let row = make_row(&schema, |b| b.set_string_by_name("name", "Alice"));
    let id = db.insert_row("users", row).unwrap();

    assert!(db.get("users", id).unwrap().is_some());

    let deleted = db.delete("users", id).unwrap();
    assert!(deleted);

    // Row should no longer exist
    assert!(db.get("users", id).unwrap().is_none());
}

// ========== Select Tests ==========

#[test]
fn select_all() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("active", ColumnType::Bool),
        ],
    ))
    .unwrap();

    db.insert_with("users", |b| b
        .set_string_by_name("name", "Alice")
        .set_bool_by_name("active", true)
        .build()).unwrap();
    db.insert_with("users", |b| b
        .set_string_by_name("name", "Bob")
        .set_bool_by_name("active", false)
        .build()).unwrap();
    db.insert_with("users", |b| b
        .set_string_by_name("name", "Carol")
        .set_bool_by_name("active", true)
        .build()).unwrap();

    let rows = db.select_all("users").unwrap();
    assert_eq!(rows.len(), 3);

    // Verify specific row properties
    let names: Vec<_> = rows.iter().map(|r| r.1.get_by_name("name")).collect();
    assert!(names.contains(&Some(RowValue::String("Alice"))));
    assert!(names.contains(&Some(RowValue::String("Bob"))));
    assert!(names.contains(&Some(RowValue::String("Carol"))));
}

#[test]
fn select_where() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("active", ColumnType::Bool),
        ],
    ))
    .unwrap();

    db.insert_with("users", |b| b
        .set_string_by_name("name", "Alice")
        .set_bool_by_name("active", true)
        .build()).unwrap();
    db.insert_with("users", |b| b
        .set_string_by_name("name", "Bob")
        .set_bool_by_name("active", false)
        .build()).unwrap();
    db.insert_with("users", |b| b
        .set_string_by_name("name", "Carol")
        .set_bool_by_name("active", true)
        .build()).unwrap();

    let active = db.select_where("users", "active", &PredicateValue::Bool(true)).unwrap();
    assert_eq!(active.len(), 2);
    // Verify active users are Alice and Carol
    let active_names: Vec<_> = active.iter().map(|r| r.1.get_by_name("name")).collect();
    assert!(active_names.contains(&Some(RowValue::String("Alice"))));
    assert!(active_names.contains(&Some(RowValue::String("Carol"))));

    let inactive = db
        .select_where("users", "active", &PredicateValue::Bool(false))
        .unwrap();
    assert_eq!(inactive.len(), 1);
    assert_eq!(inactive[0].1.get_by_name("name"), Some(RowValue::String("Bob")));
}

// ========== SQL Execute Tests ==========

#[test]
fn execute_create_table() {
    let db = Database::in_memory();

    let result = db
        .execute("CREATE TABLE users (name STRING NOT NULL, age I64)")
        .unwrap();
    assert!(matches!(result, ExecuteResult::Created(_)));

    assert!(db.get_table("users").is_some());
}

#[test]
fn execute_insert() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL, age I64)")
        .unwrap();

    let result = db
        .execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        .unwrap();
    match result {
        ExecuteResult::Inserted(id) => {
            let row = db.get("users", id).unwrap().unwrap();
            assert_eq!(row.1.get_by_name("name"), Some(RowValue::String("Alice")));
            // age is optional (no NOT NULL), so it's stored as nullable
            assert_eq!(row.1.get_by_name("age"), Some(RowValue::I64(30)));
        }
        _ => panic!("expected Inserted"),
    }
}

#[test]
fn execute_select() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)")
        .unwrap();

    let result = db.execute("SELECT * FROM users").unwrap();
    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 2);
            // Verify both users are present
            let names: Vec<_> = rows.iter().map(|r| r.1.get_by_name("name")).collect();
            assert!(names.contains(&Some(RowValue::String("Alice"))));
            assert!(names.contains(&Some(RowValue::String("Bob"))));
        }
        _ => panic!("expected Selected"),
    }

    let result = db
        .execute("SELECT * FROM users WHERE active = true")
        .unwrap();
    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1.get_by_name("name"), Some(RowValue::String("Alice")));
        }
        _ => panic!("expected Selected"),
    }
}

#[test]
fn execute_update() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL, age I64)")
        .unwrap();
    let id = match db
        .execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    let result = db
        .execute(&format!(
            "UPDATE users SET age = 31 WHERE id = '{}'",
            id
        ))
        .unwrap();
    match result {
        ExecuteResult::Updated(count) => {
            assert_eq!(count, 1);
        }
        _ => panic!("expected Updated"),
    }

    let row = db.get("users", id).unwrap().unwrap();
    // age is optional (no NOT NULL), so it's stored as nullable
    assert_eq!(row.1.get_by_name("age"), Some(RowValue::I64(31)));
}

#[test]
fn execute_delete() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name, active) VALUES ('Alice', true)")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Carol', true)")
        .unwrap();

    // Should have 3 users
    assert_eq!(db.select_all("users").unwrap().len(), 3);

    // Delete one user by ID
    let result = db
        .execute(&format!("DELETE FROM users WHERE id = '{}'", alice_id))
        .unwrap();
    match result {
        ExecuteResult::Deleted(count) => {
            assert_eq!(count, 1);
        }
        _ => panic!("expected Deleted"),
    }

    // Should have 2 users now
    let remaining = db.select_all("users").unwrap();
    assert_eq!(remaining.len(), 2);
    let names: Vec<_> = remaining.iter().map(|r| r.1.get_by_name("name")).collect();
    assert!(names.contains(&Some(RowValue::String("Bob"))));
    assert!(names.contains(&Some(RowValue::String("Carol"))));
    assert!(!names.contains(&Some(RowValue::String("Alice"))));
}

#[test]
fn execute_delete_by_condition() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Carol', true)")
        .unwrap();

    // Delete all inactive users
    let result = db
        .execute("DELETE FROM users WHERE active = false")
        .unwrap();
    match result {
        ExecuteResult::Deleted(count) => {
            assert_eq!(count, 1);
        }
        _ => panic!("expected Deleted"),
    }

    // Only active users remain
    let remaining = db.select_all("users").unwrap();
    assert_eq!(remaining.len(), 2);
    let names: Vec<_> = remaining.iter().map(|r| r.1.get_by_name("name")).collect();
    assert!(names.contains(&Some(RowValue::String("Alice"))));
    assert!(names.contains(&Some(RowValue::String("Carol"))));
}

// ========== References and Indexes Tests ==========

#[test]
fn create_table_with_ref_requires_target_table() {
    let db = Database::in_memory();

    // Cannot create posts table before users table exists
    let result = db.create_table(TableSchema::new(
        "posts",
        vec![
            ColumnDef::required("author", ColumnType::Ref("users".into())),
            ColumnDef::required("title", ColumnType::String),
        ],
    ));
    assert!(matches!(result, Err(DatabaseError::TableNotFound(_))));

    // Create users first
    db.create_table(TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    ))
    .unwrap();

    // Now posts works
    db.create_table(TableSchema::new(
        "posts",
        vec![
            ColumnDef::required("author", ColumnType::Ref("users".into())),
            ColumnDef::required("title", ColumnType::String),
        ],
    ))
    .unwrap();
}

#[test]
fn insert_validates_ref() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    ))
    .unwrap();

    db.create_table(TableSchema::new(
        "posts",
        vec![
            ColumnDef::required("author", ColumnType::Ref("users".into())),
            ColumnDef::required("title", ColumnType::String),
        ],
    ))
    .unwrap();

    // Insert with non-existent user fails
    let result = db.insert_with("posts", |b| b
        .set_ref_by_name("author", ObjectId::new(0x12345))  // fake user ID
        .set_string_by_name("title", "Hello")
        .build());
    assert!(matches!(result, Err(DatabaseError::InvalidReference { .. })));

    // Create a user
    let user_id = db.insert_with("users", |b| b
        .set_string_by_name("name", "Alice")
        .build()).unwrap();

    // Now insert post with valid ref works
    let post_id = db.insert_with("posts", |b| b
        .set_ref_by_name("author", user_id)
        .set_string_by_name("title", "Hello")
        .build()).unwrap();

    let post = db.get("posts", post_id).unwrap().unwrap();
    assert_eq!(post.1.get_by_name("author"), Some(RowValue::Ref(user_id)));
}

#[test]
fn find_referencing_uses_index() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    ))
    .unwrap();

    db.create_table(TableSchema::new(
        "posts",
        vec![
            ColumnDef::required("author", ColumnType::Ref("users".into())),
            ColumnDef::required("title", ColumnType::String),
        ],
    ))
    .unwrap();

    let alice_id = db.insert_with("users", |b| b
        .set_string_by_name("name", "Alice")
        .build()).unwrap();
    let bob_id = db.insert_with("users", |b| b
        .set_string_by_name("name", "Bob")
        .build()).unwrap();

    // Create posts by Alice
    db.insert_with("posts", |b| b
        .set_ref_by_name("author", alice_id)
        .set_string_by_name("title", "Post 1")
        .build()).unwrap();
    db.insert_with("posts", |b| b
        .set_ref_by_name("author", alice_id)
        .set_string_by_name("title", "Post 2")
        .build()).unwrap();

    // Create post by Bob
    db.insert_with("posts", |b| b
        .set_ref_by_name("author", bob_id)
        .set_string_by_name("title", "Bob's Post")
        .build()).unwrap();

    // Find all posts by Alice
    let alice_posts = db.find_referencing("posts", "author", alice_id).unwrap();
    assert_eq!(alice_posts.len(), 2);
    let alice_titles: Vec<_> = alice_posts.iter().map(|r| r.1.get_by_name("title")).collect();
    assert!(alice_titles.contains(&Some(RowValue::String("Post 1"))));
    assert!(alice_titles.contains(&Some(RowValue::String("Post 2"))));

    // Find all posts by Bob
    let bob_posts = db.find_referencing("posts", "author", bob_id).unwrap();
    assert_eq!(bob_posts.len(), 1);
    assert_eq!(bob_posts[0].1.get_by_name("title"), Some(RowValue::String("Bob's Post")));
}

#[test]
fn update_maintains_index() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    ))
    .unwrap();

    db.create_table(TableSchema::new(
        "posts",
        vec![
            ColumnDef::required("author", ColumnType::Ref("users".into())),
            ColumnDef::required("title", ColumnType::String),
        ],
    ))
    .unwrap();

    let alice_id = db.insert_with("users", |b| b
        .set_string_by_name("name", "Alice")
        .build()).unwrap();
    let bob_id = db.insert_with("users", |b| b
        .set_string_by_name("name", "Bob")
        .build()).unwrap();

    let post_id = db.insert_with("posts", |b| b
        .set_ref_by_name("author", alice_id)
        .set_string_by_name("title", "A Post")
        .build()).unwrap();

    // Initially Alice has the post
    assert_eq!(
        db.find_referencing("posts", "author", alice_id)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.find_referencing("posts", "author", bob_id).unwrap().len(),
        0
    );

    // Reassign post to Bob
    db.update_with("posts", post_id, |b| b
        .set_ref_by_name("author", bob_id)
        .build()).unwrap();

    // Now Bob has the post, Alice doesn't
    assert_eq!(
        db.find_referencing("posts", "author", alice_id)
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        db.find_referencing("posts", "author", bob_id).unwrap().len(),
        1
    );
}

#[test]
fn delete_maintains_index() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    ))
    .unwrap();

    db.create_table(TableSchema::new(
        "posts",
        vec![
            ColumnDef::required("author", ColumnType::Ref("users".into())),
            ColumnDef::required("title", ColumnType::String),
        ],
    ))
    .unwrap();

    let alice_id = db.insert_with("users", |b| b
        .set_string_by_name("name", "Alice")
        .build()).unwrap();
    let post_id = db.insert_with("posts", |b| b
        .set_ref_by_name("author", alice_id)
        .set_string_by_name("title", "A Post")
        .build()).unwrap();

    assert_eq!(
        db.find_referencing("posts", "author", alice_id)
            .unwrap()
            .len(),
        1
    );

    // Delete the post
    db.delete("posts", post_id).unwrap();

    // Index should be updated
    assert_eq!(
        db.find_referencing("posts", "author", alice_id)
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn find_referencing_on_non_ref_column_fails() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    ))
    .unwrap();

    let result = db.find_referencing("users", "name", ObjectId::new(123));
    assert!(matches!(result, Err(DatabaseError::NotAReference(_))));
}

#[test]
fn nullable_ref_column() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    ))
    .unwrap();

    db.create_table(TableSchema::new(
        "posts",
        vec![
            ColumnDef::optional("author", ColumnType::Ref("users".into())),
            ColumnDef::required("title", ColumnType::String),
        ],
    ))
    .unwrap();

    // Insert post with no author
    let post_id = db.insert_with("posts", |b| b
        .set_string_by_name("title", "Anonymous")
        .build()).unwrap();
    let post = db.get("posts", post_id).unwrap().unwrap();
    assert_eq!(post.1.get_by_name("author"), Some(RowValue::Null));

    // Insert post with author
    let user_id = db.insert_with("users", |b| b
        .set_string_by_name("name", "Alice")
        .build()).unwrap();
    let post2_id = db.insert_with("posts", |b| b
        .set_ref_by_name("author", user_id)
        .set_string_by_name("title", "By Alice")
        .build()).unwrap();

    // Only the authored post shows in index
    let posts = db.find_referencing("posts", "author", user_id).unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].0, post2_id);
}

// ========== JOIN Tests ==========

#[test]
fn join_basic() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    // Insert a user
    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Insert posts by Alice
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'First Post')",
        alice_id
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Second Post')",
        alice_id
    ))
    .unwrap();

    // JOIN posts with users
    let result = db
        .execute("SELECT * FROM posts JOIN users ON posts.author = users.id")
        .unwrap();

    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 2, "Should return 2 joined rows");
            // Each row should have values from both tables (author, title, name)
            for row in &rows {
                assert_eq!(row.1.descriptor.columns.len(), 3, "Should have 3 columns (2 from posts + 1 from users)");
                // All rows should have Alice as the author (via join)
                assert_eq!(row.1.get_by_name("name"), Some(RowValue::String("Alice")));
            }
            // Verify both post titles are present
            let titles: Vec<_> = rows.iter().map(|r| r.1.get_by_name("title")).collect();
            assert!(titles.contains(&Some(RowValue::String("First Post"))));
            assert!(titles.contains(&Some(RowValue::String("Second Post"))));
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn join_with_where_on_primary_table() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'First Post')",
        alice_id
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Second Post')",
        alice_id
    ))
    .unwrap();

    // JOIN with WHERE filtering on primary table
    let result = db
        .execute("SELECT * FROM posts JOIN users ON posts.author = users.id WHERE posts.title = 'First Post'")
        .unwrap();

    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 1, "Should return 1 row matching WHERE clause");
            assert_eq!(rows[0].1.get_by_name("title"), Some(RowValue::String("First Post")));
            assert_eq!(rows[0].1.get_by_name("name"), Some(RowValue::String("Alice")));
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn join_with_where_on_joined_table() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Alice's posts
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Alice Post 1')",
        alice_id
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Alice Post 2')",
        alice_id
    ))
    .unwrap();

    // Bob's post
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Bob Post')",
        bob_id
    ))
    .unwrap();

    // JOIN with WHERE filtering on joined table
    let result = db
        .execute("SELECT * FROM posts JOIN users ON posts.author = users.id WHERE users.name = 'Alice'")
        .unwrap();

    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 2, "Should return 2 posts by Alice");
            // All rows should have Alice as the author
            for row in &rows {
                assert_eq!(row.1.get_by_name("name"), Some(RowValue::String("Alice")));
            }
            // Verify both Alice's posts are present
            let titles: Vec<_> = rows.iter().map(|r| r.1.get_by_name("title")).collect();
            assert!(titles.contains(&Some(RowValue::String("Alice Post 1"))));
            assert!(titles.contains(&Some(RowValue::String("Alice Post 2"))));
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn join_no_matches() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    // Insert a user but no posts
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap();

    // JOIN should return empty since no posts exist
    let result = db
        .execute("SELECT * FROM posts JOIN users ON posts.author = users.id")
        .unwrap();

    match result {
        ExecuteResult::Selected(rows) => {
            assert!(rows.is_empty(), "Should return no rows when no matches");
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn join_table_star_projection() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Test Post')",
        alice_id
    ))
    .unwrap();

    // SELECT only users.* columns from join
    let result = db
        .execute("SELECT users.* FROM posts JOIN users ON posts.author = users.id")
        .unwrap();

    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 1);
            // Should only have 1 column (name) from users table
            assert_eq!(rows[0].1.descriptor.columns.len(), 1, "Should only have users columns");
            assert_eq!(rows[0].1.get_by_name("name"), Some(RowValue::String("Alice")));
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn join_multiple_conditions_where() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    // Alice is active, Bob is inactive, Charlie is active
    let alice_id = match db
        .execute("INSERT INTO users (name, active) VALUES ('Alice', true)")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name, active) VALUES ('Bob', false)")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };
    let charlie_id = match db
        .execute("INSERT INTO users (name, active) VALUES ('Charlie', true)")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Create posts:
    // - Alice has "Hello" (active=true, title matches)     -> SHOULD MATCH
    // - Alice has "Goodbye" (active=true, title no match)  -> should NOT match
    // - Bob has "Hello" (active=false, title matches)      -> should NOT match
    // - Charlie has "World" (active=true, title no match)  -> should NOT match
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Hello')",
        alice_id
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Goodbye')",
        alice_id
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Hello')",
        bob_id
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'World')",
        charlie_id
    ))
    .unwrap();

    // WHERE with multiple conditions across tables
    // Only Alice's "Hello" post should match (active=true AND title='Hello')
    let result = db
        .execute("SELECT * FROM posts JOIN users ON posts.author = users.id WHERE users.active = true AND posts.title = 'Hello'")
        .unwrap();

    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 1, "Should return only 1 row matching both conditions");
            // Verify it's Alice's post (the name column should be 'Alice')
            // Row has: author (ref), title, name, active (simple column names in execute)
            assert_eq!(rows[0].1.get_by_name("name"), Some(RowValue::String("Alice")));
            assert_eq!(rows[0].1.get_by_name("title"), Some(RowValue::String("Hello")));
        }
        _ => panic!("Expected Selected"),
    }

    // Verify that without the title condition, we'd get 3 rows (Alice's posts + Charlie's)
    let result_active_only = db
        .execute("SELECT * FROM posts JOIN users ON posts.author = users.id WHERE users.active = true")
        .unwrap();
    match result_active_only {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 3, "Should return 3 rows for active users (2 Alice + 1 Charlie)");
            // All rows should have active=true
            for row in &rows {
                assert_eq!(row.1.get_by_name("active"), Some(RowValue::Bool(true)));
            }
            // Verify authors are Alice and Charlie
            let names: Vec<_> = rows.iter().map(|r| r.1.get_by_name("name")).collect();
            assert!(names.contains(&Some(RowValue::String("Alice"))));
            assert!(names.contains(&Some(RowValue::String("Charlie"))));
        }
        _ => panic!("Expected Selected"),
    }

    // Verify that without the active condition, we'd get 2 rows with title='Hello'
    let result_title_only = db
        .execute("SELECT * FROM posts JOIN users ON posts.author = users.id WHERE posts.title = 'Hello'")
        .unwrap();
    match result_title_only {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 2, "Should return 2 rows with title='Hello' (Alice + Bob)");
            // All rows should have title='Hello'
            for row in &rows {
                assert_eq!(row.1.get_by_name("title"), Some(RowValue::String("Hello")));
            }
            // Verify authors are Alice and Bob
            let names: Vec<_> = rows.iter().map(|r| r.1.get_by_name("name")).collect();
            assert!(names.contains(&Some(RowValue::String("Alice"))));
            assert!(names.contains(&Some(RowValue::String("Bob"))));
        }
        _ => panic!("Expected Selected"),
    }
}

// ========== Incremental Query Integration Tests ==========

#[test]
fn incremental_query_returns_current_rows() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)")
        .unwrap();

    let query = db.incremental_query("SELECT * FROM users").unwrap();

    // Should have the current rows
    let rows = query.rows();
    assert_eq!(rows.len(), 2);
    let names: Vec<_> = rows.iter().map(|r| r.1.get_by_name("name")).collect();
    assert!(names.contains(&Some(RowValue::String("Alice"))));
    assert!(names.contains(&Some(RowValue::String("Bob"))));
}

#[test]
fn incremental_query_with_where_clause() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Carol', true)")
        .unwrap();

    let query = db
        .incremental_query("SELECT * FROM users WHERE active = true")
        .unwrap();

    let rows = query.rows();
    assert_eq!(rows.len(), 2);
    // Verify only active users (Alice and Carol) are returned
    let names: Vec<_> = rows.iter().map(|r| r.1.get_by_name("name")).collect();
    assert!(names.contains(&Some(RowValue::String("Alice"))));
    assert!(names.contains(&Some(RowValue::String("Carol"))));
    assert!(!names.contains(&Some(RowValue::String("Bob"))));
}

#[test]
fn incremental_query_auto_updates_on_insert() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap();

    let query = db.incremental_query("SELECT * FROM users").unwrap();

    // Initially has 1 row
    let initial_rows = query.rows();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(initial_rows[0].1.get_by_name("name"), Some(RowValue::String("Alice")));

    // Insert another row - query auto-updates incrementally
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();

    // Query immediately has 2 rows
    let updated_rows = query.rows();
    assert_eq!(updated_rows.len(), 2);
    let names: Vec<_> = updated_rows.iter().map(|r| r.1.get_by_name("name")).collect();
    assert!(names.contains(&Some(RowValue::String("Alice"))));
    assert!(names.contains(&Some(RowValue::String("Bob"))));
}

#[test]
fn incremental_query_auto_updates_on_update() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    let id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    let query = db
        .incremental_query("SELECT * FROM users WHERE name = 'Alice'")
        .unwrap();
    let initial_rows = query.rows();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(initial_rows[0].1.get_by_name("name"), Some(RowValue::String("Alice")));

    // Update the row to have a different name
    db.update_with("users", id, |b| b
        .set_string_by_name("name", "Alicia")
        .build()).unwrap();

    // Query auto-updates - should now return 0 rows (name no longer matches)
    assert_eq!(query.rows().len(), 0);
}

#[test]
fn incremental_query_auto_updates_on_delete() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    let id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    let query = db.incremental_query("SELECT * FROM users").unwrap();
    let initial_rows = query.rows();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(initial_rows[0].1.get_by_name("name"), Some(RowValue::String("Alice")));

    // Delete the row
    db.delete("users", id).unwrap();

    // Query auto-updates - should now return 0 rows
    assert_eq!(query.rows().len(), 0);
}

#[test]
fn incremental_query_callback_on_insert() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap();

    let query = db.incremental_query("SELECT * FROM users").unwrap();

    let call_count = Arc::new(AtomicUsize::new(0));
    let delta_counts = Arc::new(RwLock::new(Vec::<usize>::new()));
    let call_count_clone = call_count.clone();
    let delta_counts_clone = delta_counts.clone();

    // Subscribe with delta callback (first call is initial state as "Added" deltas)
    let _id = query.subscribe(Box::new(move |deltas| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
        delta_counts_clone.write().unwrap().push(deltas.len());
    }));

    // Initial state callback should have been called with 1 row (Alice)
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(*delta_counts.read().unwrap(), vec![1]);

    // Insert a new row - callback should be called
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();

    // Callback should have been called again with 1 delta (Added Bob)
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
    assert_eq!(*delta_counts.read().unwrap(), vec![1, 1]);

    // Insert another row
    db.execute("INSERT INTO users (name) VALUES ('Charlie')")
        .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 3);
    assert_eq!(*delta_counts.read().unwrap(), vec![1, 1, 1]);
}

#[test]
fn incremental_query_callback_on_delete() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();

    // Create query first so it tracks the rows
    let query = db.incremental_query("SELECT * FROM users").unwrap();

    let id1 = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();

    // Verify we have 2 rows with correct names
    let initial_rows = query.rows();
    assert_eq!(initial_rows.len(), 2);
    let names: Vec<_> = initial_rows.iter().map(|r| r.1.get_by_name("name")).collect();
    assert!(names.contains(&Some(RowValue::String("Alice"))));
    assert!(names.contains(&Some(RowValue::String("Bob"))));

    let call_count = Arc::new(AtomicUsize::new(0));
    let delta_counts = Arc::new(RwLock::new(Vec::<usize>::new()));
    let call_count_clone = call_count.clone();
    let delta_counts_clone = delta_counts.clone();

    let _sub_id = query.subscribe(Box::new(move |deltas| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
        delta_counts_clone.write().unwrap().push(deltas.len());
    }));

    // Initial state callback should have 2 rows (Added deltas)
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(*delta_counts.read().unwrap(), vec![2]);

    // Delete a row - callback should be triggered
    db.delete("users", id1).unwrap();

    // Callback called with 1 delta (Removed)
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
    assert_eq!(*delta_counts.read().unwrap(), vec![2, 1]);
}

// ========== Incremental Query JOIN Integration Tests ==========

#[test]
fn incremental_join_basic() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'First Post')",
        alice_id
    ))
    .unwrap();

    // Create incremental query with JOIN
    let query = db
        .incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id")
        .unwrap();

    let rows = query.rows();
    assert_eq!(rows.len(), 1, "Should return 1 joined row");
    // Verify joined row has expected values: author ref, title, name
    assert_eq!(rows[0].1.get_by_name("posts.title"), Some(RowValue::String("First Post")));
    assert_eq!(rows[0].1.get_by_name("users.name"), Some(RowValue::String("Alice")));
}

#[test]
fn incremental_join_updates_on_post_insert() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'First Post')",
        alice_id
    ))
    .unwrap();

    let query = db
        .incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id")
        .unwrap();

    let call_count = Arc::new(AtomicUsize::new(0));
    let delta_counts = Arc::new(RwLock::new(Vec::<usize>::new()));
    let call_count_clone = call_count.clone();
    let delta_counts_clone = delta_counts.clone();

    let _sub_id = query.subscribe(Box::new(move |deltas| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
        delta_counts_clone.write().unwrap().push(deltas.len());
    }));

    // Initial state callback should have 1 row (First Post)
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(*delta_counts.read().unwrap(), vec![1]);

    // Insert another post - should trigger callback
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Second Post')",
        alice_id
    ))
    .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 2);
    assert_eq!(*delta_counts.read().unwrap(), vec![1, 1]);
}

#[test]
fn incremental_join_updates_on_user_change() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Test Post')",
        alice_id
    ))
    .unwrap();

    let query = db
        .incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id")
        .unwrap();

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let _sub_id = query.subscribe(Box::new(move |_delta| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
    }));

    // Update user - should trigger callback since the joined row includes user data
    db.update_with("users", alice_id, |b| b
        .set_string_by_name("name", "Alicia")
        .build()).unwrap();

    // The join should have been notified
    assert!(call_count.load(Ordering::SeqCst) > 0);
}

#[test]
fn incremental_join_delete_post() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let post_id = match db
        .execute(&format!(
            "INSERT INTO posts (author, title) VALUES ('{}', 'Test Post')",
            alice_id
        ))
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    let query = db
        .incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id")
        .unwrap();

    let initial_rows = query.rows();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(initial_rows[0].1.get_by_name("posts.title"), Some(RowValue::String("Test Post")));
    assert_eq!(initial_rows[0].1.get_by_name("users.name"), Some(RowValue::String("Alice")));

    // Delete post
    db.delete("posts", post_id).unwrap();

    // Should now be empty
    assert_eq!(query.rows().len(), 0);
}

#[test]
fn incremental_join_delete_user() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Test Post')",
        alice_id
    ))
    .unwrap();

    let query = db
        .incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id")
        .unwrap();

    let call_count = Arc::new(AtomicUsize::new(0));
    let delta_counts = Arc::new(RwLock::new(Vec::<usize>::new()));
    let call_count_clone = call_count.clone();
    let delta_counts_clone = delta_counts.clone();

    let _sub_id = query.subscribe(Box::new(move |deltas| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
        delta_counts_clone.write().unwrap().push(deltas.len());
    }));

    // Initial state callback should have 1 row
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(*delta_counts.read().unwrap(), vec![1]);

    // Initial: 1 joined row
    let initial_rows = query.rows();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(initial_rows[0].1.get_by_name("posts.title"), Some(RowValue::String("Test Post")));
    assert_eq!(initial_rows[0].1.get_by_name("users.name"), Some(RowValue::String("Alice")));

    // Delete the user - join should now return 0 rows (the post still exists but can't join)
    db.delete("users", alice_id).unwrap();

    // Should get a notification with 1 delta (Removed)
    assert!(call_count.load(Ordering::SeqCst) > 1);
    let counts = delta_counts.read().unwrap();
    assert_eq!(*counts.last().unwrap(), 1);
}

#[test]
fn incremental_join_multiple_users() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE posts (author REFERENCES users NOT NULL, title STRING NOT NULL)")
        .unwrap();

    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Alice has 2 posts, Bob has 1
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Alice Post 1')",
        alice_id
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Alice Post 2')",
        alice_id
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Bob Post')",
        bob_id
    ))
    .unwrap();

    let query = db
        .incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id")
        .unwrap();

    // Should have 3 joined rows
    let rows = query.rows();
    assert_eq!(rows.len(), 3);

    // Verify we have posts from both Alice and Bob
    let author_names: Vec<_> = rows.iter().map(|r| r.1.get_by_name("users.name")).collect();
    assert_eq!(author_names.iter().filter(|n| **n == Some(RowValue::String("Alice"))).count(), 2);
    assert_eq!(author_names.iter().filter(|n| **n == Some(RowValue::String("Bob"))).count(), 1);

    // Verify all post titles are present
    let titles: Vec<_> = rows.iter().map(|r| r.1.get_by_name("posts.title")).collect();
    assert!(titles.contains(&Some(RowValue::String("Alice Post 1"))));
    assert!(titles.contains(&Some(RowValue::String("Alice Post 2"))));
    assert!(titles.contains(&Some(RowValue::String("Bob Post"))));
}

// ========== ARRAY Subquery Execution Tests ==========

#[test]
fn array_subquery_correlated() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE folders (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE notes (folder REFERENCES folders NOT NULL, title STRING NOT NULL)")
        .unwrap();

    // Create two folders
    let folder1_id = match db
        .execute("INSERT INTO folders (name) VALUES ('Work')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };
    let folder2_id = match db
        .execute("INSERT INTO folders (name) VALUES ('Personal')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Create notes in each folder
    db.execute(&format!(
        "INSERT INTO notes (folder, title) VALUES ('{}', 'Meeting Notes')",
        folder1_id
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO notes (folder, title) VALUES ('{}', 'Project Plan')",
        folder1_id
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO notes (folder, title) VALUES ('{}', 'Shopping List')",
        folder2_id
    ))
    .unwrap();

    // Query with ARRAY subquery for correlated notes
    let result = db
        .execute("SELECT f.name, ARRAY(SELECT n.title FROM notes n WHERE n.folder = f.id) AS notes FROM folders f")
        .unwrap();

    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 2, "Should return 2 folders");

            // Find the Work folder row
            let work_row = rows.iter().find(|r| r.1.get_by_name("name") == Some(RowValue::String("Work")));
            assert!(work_row.is_some(), "Should have Work folder");
            if let Some(RowValue::Array(arr)) = work_row.unwrap().1.get_by_name("notes") {
                assert_eq!(arr.len(), 2, "Work folder should have 2 notes");
            } else {
                panic!("Expected Array for notes");
            }

            // Find the Personal folder row
            let personal_row = rows.iter().find(|r| r.1.get_by_name("name") == Some(RowValue::String("Personal")));
            assert!(personal_row.is_some(), "Should have Personal folder");
            if let Some(RowValue::Array(arr)) = personal_row.unwrap().1.get_by_name("notes") {
                assert_eq!(arr.len(), 1, "Personal folder should have 1 note");
            } else {
                panic!("Expected Array for notes");
            }
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn array_subquery_returns_whole_rows() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE folders (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE notes (folder REFERENCES folders NOT NULL, title STRING NOT NULL)")
        .unwrap();

    let folder_id = match db
        .execute("INSERT INTO folders (name) VALUES ('Work')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    db.execute(&format!(
        "INSERT INTO notes (folder, title) VALUES ('{}', 'Meeting Notes')",
        folder_id
    ))
    .unwrap();

    // Query with ARRAY subquery returning whole rows via table alias
    let result = db
        .execute("SELECT f.name, ARRAY(SELECT n FROM notes n WHERE n.folder = f.id) AS notes FROM folders f")
        .unwrap();

    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 1);
            // get_column returns an owned Value, so we need to keep it alive
            let notes_col = rows[0].1.get_by_name("notes");
            assert!(notes_col.is_some(), "Should have notes column");
            if let Some(RowValue::Array(arr)) = notes_col {
                assert_eq!(arr.len(), 1);

                // Each item is an OwnedRow - use iterator to get first item
                let note_row = arr.iter().next().unwrap();
                // Note row should have 2 values: folder (ref), title
                assert_eq!(note_row.descriptor.columns.len(), 2);
                assert_eq!(note_row.get_by_name("title"), Some(RowValue::String("Meeting Notes")));
            } else {
                panic!("Expected Array");
            }
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn array_subquery_empty_result() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE folders (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE notes (folder REFERENCES folders NOT NULL, title STRING NOT NULL)")
        .unwrap();

    // Create a folder with no notes
    db.execute("INSERT INTO folders (name) VALUES ('Empty Folder')")
        .unwrap();

    let result = db
        .execute("SELECT f.name, ARRAY(SELECT n.title FROM notes n WHERE n.folder = f.id) AS notes FROM folders f")
        .unwrap();

    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 1);
            let notes_col = rows[0].1.get_by_name("notes");
            assert!(notes_col.is_some(), "Should have notes column");
            if let Some(RowValue::Array(arr)) = notes_col {
                assert_eq!(arr.len(), 0, "Should return empty array for folder with no notes");
            } else {
                panic!("Expected Array");
            }
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn array_subquery_non_correlated() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE folders (name STRING NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE notes (title STRING NOT NULL)")
        .unwrap();

    db.execute("INSERT INTO folders (name) VALUES ('Folder1')")
        .unwrap();
    db.execute("INSERT INTO notes (title) VALUES ('Note A')")
        .unwrap();
    db.execute("INSERT INTO notes (title) VALUES ('Note B')")
        .unwrap();

    // Non-correlated subquery - returns all notes for each folder
    let result = db
        .execute("SELECT f.name, ARRAY(SELECT title FROM notes) AS all_notes FROM folders f")
        .unwrap();

    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 1);
            let notes_col = rows[0].1.get_by_name("all_notes");
            assert!(notes_col.is_some(), "Should have all_notes column");
            if let Some(RowValue::Array(arr)) = notes_col {
                assert_eq!(arr.len(), 2, "Should return all 2 notes");
            } else {
                panic!("Expected Array");
            }
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn test_note_with_i64_timestamps() {
    let db = Database::in_memory();
    
    db.execute("CREATE TABLE User (name STRING NOT NULL, email STRING NOT NULL)").unwrap();
    db.execute("CREATE TABLE Folder (name STRING NOT NULL, owner REFERENCES User NOT NULL)").unwrap();
    db.execute("CREATE TABLE Note (title STRING NOT NULL, content STRING NOT NULL, author REFERENCES User NOT NULL, folder REFERENCES Folder, createdAt I64 NOT NULL, updatedAt I64 NOT NULL)").unwrap();
    
    let user_result = db.execute("INSERT INTO User (name, email) VALUES ('Alice', 'alice@example.com')").unwrap();
    let user_id = match user_result {
        ExecuteResult::Inserted(id) => id.to_string(),
        _ => panic!("Expected Inserted"),
    };
    
    // Insert note with I64 timestamps (like Date.now() in JS)
    let timestamp = 1704384000000i64;
    let sql = format!(
        "INSERT INTO Note (title, content, author, folder, createdAt, updatedAt) VALUES ('Test Note', 'Content', '{}', NULL, {}, {})",
        user_id, timestamp, timestamp
    );
    println!("SQL: {}", sql);
    
    let result = db.execute(&sql);
    println!("Result: {:?}", result);
    
    assert!(result.is_ok(), "Insert should succeed: {:?}", result);
    
    // Verify the note was inserted
    let rows = db.select_all("Note").unwrap();
    assert_eq!(rows.len(), 1);
    println!("Note: {:?}", rows[0]);
}

// ========== Soft Delete and Hard Delete Tests ==========

#[test]
fn soft_delete_removes_row_from_queries() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    ))
    .unwrap();

    let id = db.insert_with("users", |b| b
        .set_string_by_name("name", "Alice")
        .build()).unwrap();

    // Row exists
    assert!(db.get("users", id).unwrap().is_some());
    assert_eq!(db.select_all("users").unwrap().len(), 1);

    // Soft delete
    let deleted = db.delete("users", id).unwrap();
    assert!(deleted);

    // Row no longer visible in queries
    assert!(db.get("users", id).unwrap().is_none());
    assert_eq!(db.select_all("users").unwrap().len(), 0);
}

#[test]
fn hard_delete_via_sql() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Insert a user
    let result = db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();
    let id = match result {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Verify user exists
    assert!(db.get("users", id).unwrap().is_some());

    // Hard delete via SQL
    let sql = format!("DELETE FROM users WHERE id = '{}' HARD", id);
    let result = db.execute(&sql).unwrap();
    match result {
        ExecuteResult::Deleted(count) => assert_eq!(count, 1),
        _ => panic!("Expected Deleted"),
    }

    // Row no longer visible
    assert!(db.get("users", id).unwrap().is_none());
}

#[test]
fn soft_delete_via_sql() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Insert a user
    let result = db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap();
    let id = match result {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };

    // Soft delete via SQL (no HARD keyword)
    let sql = format!("DELETE FROM users WHERE id = '{}'", id);
    let result = db.execute(&sql).unwrap();
    match result {
        ExecuteResult::Deleted(count) => assert_eq!(count, 1),
        _ => panic!("Expected Deleted"),
    }

    // Row no longer visible
    assert!(db.get("users", id).unwrap().is_none());
}

#[test]
fn delete_multiple_rows_hard() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)").unwrap();

    // Insert multiple users
    db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)").unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)").unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Charlie', false)").unwrap();

    assert_eq!(db.select_all("users").unwrap().len(), 3);

    // Hard delete all inactive users
    let result = db.execute("DELETE FROM users WHERE active = false HARD").unwrap();
    match result {
        ExecuteResult::Deleted(count) => assert_eq!(count, 2),
        _ => panic!("Expected Deleted"),
    }

    // Only Alice remains
    let rows = db.select_all("users").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get_by_name("name"), Some(RowValue::String("Alice")));
}

#[test]
fn hard_delete_all_rows() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Charlie')").unwrap();

    assert_eq!(db.select_all("users").unwrap().len(), 3);

    // Hard delete all
    let result = db.execute("DELETE FROM users HARD").unwrap();
    match result {
        ExecuteResult::Deleted(count) => assert_eq!(count, 3),
        _ => panic!("Expected Deleted"),
    }

    assert_eq!(db.select_all("users").unwrap().len(), 0);
}

// ========== LIMIT and OFFSET Tests ==========

#[test]
fn select_limit() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Insert 5 users
    for name in &["Alice", "Bob", "Charlie", "David", "Eve"] {
        db.execute(&format!("INSERT INTO users (name) VALUES ('{}')", name)).unwrap();
    }

    // Select with limit
    let result = db.execute("SELECT * FROM users LIMIT 3").unwrap();
    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 3);
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn select_offset() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Insert 5 users
    for name in &["Alice", "Bob", "Charlie", "David", "Eve"] {
        db.execute(&format!("INSERT INTO users (name) VALUES ('{}')", name)).unwrap();
    }

    // Select with offset (skip first 2)
    let result = db.execute("SELECT * FROM users OFFSET 2").unwrap();
    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 3); // 5 - 2 = 3
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn select_limit_offset() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Insert 5 users
    for name in &["Alice", "Bob", "Charlie", "David", "Eve"] {
        db.execute(&format!("INSERT INTO users (name) VALUES ('{}')", name)).unwrap();
    }

    // Select with limit and offset
    let result = db.execute("SELECT * FROM users LIMIT 2 OFFSET 1").unwrap();
    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 2);
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn select_limit_exceeds_rows() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Insert 3 users
    for name in &["Alice", "Bob", "Charlie"] {
        db.execute(&format!("INSERT INTO users (name) VALUES ('{}')", name)).unwrap();
    }

    // Limit larger than available rows
    let result = db.execute("SELECT * FROM users LIMIT 100").unwrap();
    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 3); // Should return all available
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn select_offset_exceeds_rows() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Insert 3 users
    for name in &["Alice", "Bob", "Charlie"] {
        db.execute(&format!("INSERT INTO users (name) VALUES ('{}')", name)).unwrap();
    }

    // Offset larger than available rows
    let result = db.execute("SELECT * FROM users OFFSET 100").unwrap();
    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 0); // Should return empty
        }
        _ => panic!("Expected Selected"),
    }
}

#[test]
fn incremental_query_limit() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Create incremental query with limit
    let query = db.incremental_query("SELECT * FROM users LIMIT 2").unwrap();

    // Initially empty
    assert_eq!(query.rows().len(), 0);

    // Insert first row - should appear
    db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();
    assert_eq!(query.rows().len(), 1);

    // Insert second row - should appear
    db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap();
    assert_eq!(query.rows().len(), 2);

    // Insert third row - should NOT appear (limit is 2)
    db.execute("INSERT INTO users (name) VALUES ('Charlie')").unwrap();
    assert_eq!(query.rows().len(), 2);
}

#[test]
fn incremental_query_offset() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Create incremental query with offset
    let query = db.incremental_query("SELECT * FROM users OFFSET 1").unwrap();

    // Initially empty
    assert_eq!(query.rows().len(), 0);

    // Insert first row - should NOT appear (it's in the offset region)
    db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap();
    assert_eq!(query.rows().len(), 0);

    // Insert second row - should appear
    db.execute("INSERT INTO users (name) VALUES ('Bob')").unwrap();
    assert_eq!(query.rows().len(), 1);

    // Insert third row - should appear
    db.execute("INSERT INTO users (name) VALUES ('Charlie')").unwrap();
    assert_eq!(query.rows().len(), 2);
}

#[test]
fn incremental_query_limit_offset() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)").unwrap();

    // Create incremental query: skip 1, take 2
    let query = db.incremental_query("SELECT * FROM users LIMIT 2 OFFSET 1").unwrap();

    // Insert 4 rows
    for name in &["Alice", "Bob", "Charlie", "David"] {
        db.execute(&format!("INSERT INTO users (name) VALUES ('{}')", name)).unwrap();
    }

    // Should have exactly 2 rows (skipped Alice, took Bob and Charlie, skipped David)
    let rows = query.rows();
    assert_eq!(rows.len(), 2);
}

#[test]
fn incremental_query_with_array_subquery() {
    let db = Database::in_memory();

    // Create parent table (Issues)
    db.execute("CREATE TABLE Issues (title STRING NOT NULL)")
        .unwrap();

    // Create child table (IssueLabels) with ref to Issues
    db.execute("CREATE TABLE IssueLabels (issue REFERENCES Issues NOT NULL, name STRING NOT NULL)")
        .unwrap();

    // Insert a parent row
    let issue_id = match db
        .execute("INSERT INTO Issues (title) VALUES ('Test Issue')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    // Insert child rows
    db.execute(&format!(
        "INSERT INTO IssueLabels (issue, name) VALUES ('{}', 'Bug')",
        issue_id
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO IssueLabels (issue, name) VALUES ('{}', 'Priority')",
        issue_id
    ))
    .unwrap();

    // Create incremental query with ARRAY subquery
    let sql = format!(
        "SELECT i.id, i.title, ARRAY(SELECT il.id, il.issue, il.name FROM IssueLabels il WHERE il.issue = i.id) AS labels FROM Issues i"
    );
    eprintln!("SQL: {}", sql);

    let query = db.incremental_query(&sql).unwrap();

    // Get the diagram to verify ArrayAggregate node exists
    let diagram = query.diagram();
    eprintln!("Query Graph:\n{}", diagram);

    // Should contain ArrayAggregate node
    assert!(diagram.contains("ArrayAggregate"), "Query graph should have ArrayAggregate node");

    let rows = query.rows();
    eprintln!("Rows: {:?}", rows);

    assert_eq!(rows.len(), 1, "Should return 1 issue");

    // Verify the row has an array with 2 labels
    // The row has: values[0]=title, values[1]=Array (id is stored separately in Row)
    assert_eq!(rows[0].1.descriptor.columns.len(), 2, "Row should have 2 values (title, array)");
    assert_eq!(rows[0].1.get_by_name("title"), Some(RowValue::String("Test Issue")));

    // NOTE: SQL alias "labels" is not used - array columns are named after the inner table
    // TODO: Pass SQL alias through to array_aggregate for proper naming
    match rows[0].1.get_by_name("IssueLabels") {
        Some(RowValue::Array(arr)) => {
            assert_eq!(arr.len(), 2, "Should have 2 labels");
        }
        other => panic!("Expected Array, got {:?}", other),
    }
}

/// Test that JOIN + ArrayAggregate preserves nullable columns from joined tables.
/// This directly tests the query graph without SQL parsing to isolate the issue.
#[test]
fn incremental_query_join_plus_array_aggregate_preserves_nullable_columns() {
    use groove::sql::query_graph::{JoinGraphBuilder, GraphId};

    let db = Database::in_memory();

    // Use FULL schema matching demo-app to reproduce the issue exactly

    // Projects: name, color, description (nullable)
    let projects_schema = TableSchema::new(
        "Projects",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("color", ColumnType::String),
            ColumnDef::optional("description", ColumnType::String),
        ],
    );
    db.create_table(projects_schema.clone()).unwrap();

    // Issues: title, description (nullable), status, priority, project, createdAt, updatedAt
    let issues_schema = TableSchema::new(
        "Issues",
        vec![
            ColumnDef::required("title", ColumnType::String),
            ColumnDef::optional("description", ColumnType::String),
            ColumnDef::required("status", ColumnType::String),
            ColumnDef::required("priority", ColumnType::String),
            ColumnDef::required("project", ColumnType::Ref("Projects".into())),
            ColumnDef::required("createdAt", ColumnType::I64),
            ColumnDef::required("updatedAt", ColumnType::I64),
        ],
    );
    db.create_table(issues_schema.clone()).unwrap();

    // Labels (needed for foreign key)
    let labels_table_schema = TableSchema::new(
        "Labels",
        vec![
            ColumnDef::required("name", ColumnType::String),
        ],
    );
    db.create_table(labels_table_schema).unwrap();

    // Users (needed for foreign key)
    let users_table_schema = TableSchema::new(
        "Users",
        vec![
            ColumnDef::required("name", ColumnType::String),
        ],
    );
    db.create_table(users_table_schema).unwrap();

    // IssueLabels: issue, label
    let labels_schema = TableSchema::new(
        "IssueLabels",
        vec![
            ColumnDef::required("issue", ColumnType::Ref("Issues".into())),
            ColumnDef::required("label", ColumnType::Ref("Labels".into())),
        ],
    );
    db.create_table(labels_schema.clone()).unwrap();

    // IssueAssignees: issue, user
    let assignees_schema = TableSchema::new(
        "IssueAssignees",
        vec![
            ColumnDef::required("issue", ColumnType::Ref("Issues".into())),
            ColumnDef::required("user", ColumnType::Ref("Users".into())),
        ],
    );
    db.create_table(assignees_schema.clone()).unwrap();

    // Insert project with description (nullable column with value)
    let project_id = db.insert_with("Projects", |b| b
        .set_string_by_name("name", "Test Project")
        .set_string_by_name("color", "#00ff00")
        .set_string_by_name("description", "A test project")
        .build()).unwrap();

    // Insert issue referencing the project
    let issue_id = db.insert_with("Issues", |b| b
        .set_string_by_name("title", "Test Issue")
        .set_string_by_name("description", "Test description")
        .set_string_by_name("status", "open")
        .set_string_by_name("priority", "high")
        .set_ref_by_name("project", project_id)
        .set_i64_by_name("createdAt", 1234567890)
        .set_i64_by_name("updatedAt", 1234567890)
        .build()).unwrap();

    // Insert actual Label row
    let label_id = db.insert_with("Labels", |b| b
        .set_string_by_name("name", "Bug")
        .build()).unwrap();

    // Insert actual User row
    let user_id = db.insert_with("Users", |b| b
        .set_string_by_name("name", "Alice")
        .build()).unwrap();

    // Insert IssueLabel row
    db.insert_with("IssueLabels", |b| b
        .set_ref_by_name("issue", issue_id)
        .set_ref_by_name("label", label_id)
        .build()).unwrap();

    // Insert IssueAssignee row
    db.insert_with("IssueAssignees", |b| b
        .set_ref_by_name("issue", issue_id)
        .set_ref_by_name("user", user_id)
        .build()).unwrap();

    // Build query graph manually:
    // Issues JOIN Projects + ArrayAggregate(IssueLabels) + ArrayAggregate(IssueAssignees)
    let mut builder = JoinGraphBuilder::new(
        "Issues",
        issues_schema.clone(),
        "Projects",
        projects_schema.clone(),
        "project",
    );
    builder.add_schema("IssueLabels", labels_schema.clone());
    builder.add_schema("IssueAssignees", assignees_schema.clone());

    // Add Join node for Issues -> Projects
    let join = builder.join();

    // Add ArrayAggregate for IssueLabels -> Issues
    let agg1 = builder.array_aggregate(
        join,
        "IssueLabels".to_string(),
        "issue",
        labels_schema.clone(),
        vec![], // No inner joins
        -1, // Append at end
    );

    // Add ArrayAggregate for IssueAssignees -> Issues
    let agg2 = builder.array_aggregate(
        agg1,
        "IssueAssignees".to_string(),
        "issue",
        assignees_schema.clone(),
        vec![], // No inner joins
        -1, // Append at end
    );

    let mut graph = builder.output(agg2, GraphId(1));

    // Get the diagram
    let diagram = graph.to_diagram();
    eprintln!("Query Graph:\n{}", diagram);

    // Initialize and get output
    let mut cache = groove::sql::query_graph::RowCache::new();
    let rows = graph.get_output(&mut cache, &db.state());

    eprintln!("Rows count: {}", rows.len());
    assert_eq!(rows.len(), 1, "Should return 1 issue");

    // Output is now (ObjectId, OwnedRow) format
    // ArrayAggregate uses buffer format - values can be verified using RowValue accessors
    let (id, _owned_row) = &rows[0];
    eprintln!("Row id: {:?}", id);
}

/// Test that the SQL path also preserves nullable columns from joined tables.
/// This tests the `build_join_graph` SQL parsing path rather than the direct API.
#[test]
fn incremental_query_sql_join_preserves_nullable_columns() {
    let db = Database::in_memory();

    // Projects: name, color, description (nullable)
    let projects_schema = TableSchema::new(
        "Projects",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("color", ColumnType::String),
            ColumnDef::optional("description", ColumnType::String),
        ],
    );
    db.create_table(projects_schema.clone()).unwrap();

    // Issues: title, description (nullable), status, priority, project, createdAt, updatedAt
    let issues_schema = TableSchema::new(
        "Issues",
        vec![
            ColumnDef::required("title", ColumnType::String),
            ColumnDef::optional("description", ColumnType::String),
            ColumnDef::required("status", ColumnType::String),
            ColumnDef::required("priority", ColumnType::String),
            ColumnDef::required("project", ColumnType::Ref("Projects".into())),
            ColumnDef::required("createdAt", ColumnType::I64),
            ColumnDef::required("updatedAt", ColumnType::I64),
        ],
    );
    db.create_table(issues_schema.clone()).unwrap();

    // Insert project with description
    let project_id = db.insert_with("Projects", |b| b
        .set_string_by_name("name", "Test Project")
        .set_string_by_name("color", "#00ff00")
        .set_string_by_name("description", "A test project")
        .build()).unwrap();

    // Insert issue referencing the project
    db.insert_with("Issues", |b| b
        .set_string_by_name("title", "Test Issue")
        .set_string_by_name("description", "Test description")
        .set_string_by_name("status", "open")
        .set_string_by_name("priority", "high")
        .set_ref_by_name("project", project_id)
        .set_i64_by_name("createdAt", 1234567890)
        .set_i64_by_name("updatedAt", 1234567890)
        .build()).unwrap();

    // Use incremental_query via SQL - this is the path TypeScript uses
    let sql = "SELECT i.* FROM Issues i JOIN Projects ON i.project = Projects.id";
    let query = db.incremental_query(sql).expect("should create incremental query");
    let rows = query.rows();

    eprintln!("SQL query rows: {:?}", rows.len());
    assert_eq!(rows.len(), 1, "Should return 1 issue");

    eprintln!("Row values (len={}):", rows[0].1.descriptor.columns.len());
    for col in &rows[0].1.descriptor.columns {
        eprintln!("  {}: {:?}", col.name, rows[0].1.get_by_name(&col.name));
    }

    // In a JOIN query, the row should contain:
    // Issues: title, description, status, priority, project, createdAt, updatedAt = 7 columns
    // Projects: name, color, description = 3 columns
    // Total: 10 columns
    assert_eq!(rows[0].1.descriptor.columns.len(), 10, "Should have 10 values (7 Issues + 3 Projects)");

    // Projects.description should be accessible by name
    let proj_desc = rows[0].1.get_by_name("Projects.description");
    eprintln!("\nProjects.description: {:?}", proj_desc);

    // get_by_name returns RowValue which handles nullable transparently
    assert_eq!(proj_desc, Some(RowValue::String("A test project")));
}

/// Test that SQL JOIN + ARRAY subquery preserves nullable columns from joined tables.
/// This tests the exact flow used by TypeScript: build_join_graph + add_join_array_aggregates.
#[test]
fn incremental_query_sql_join_with_array_preserves_nullable_columns() {
    let db = Database::in_memory();

    // Projects: name, color, description (nullable)
    let projects_schema = TableSchema::new(
        "Projects",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("color", ColumnType::String),
            ColumnDef::optional("description", ColumnType::String),
        ],
    );
    db.create_table(projects_schema.clone()).unwrap();

    // Issues: title, description (nullable), status, priority, project, createdAt, updatedAt
    let issues_schema = TableSchema::new(
        "Issues",
        vec![
            ColumnDef::required("title", ColumnType::String),
            ColumnDef::optional("description", ColumnType::String),
            ColumnDef::required("status", ColumnType::String),
            ColumnDef::required("priority", ColumnType::String),
            ColumnDef::required("project", ColumnType::Ref("Projects".into())),
            ColumnDef::required("createdAt", ColumnType::I64),
            ColumnDef::required("updatedAt", ColumnType::I64),
        ],
    );
    db.create_table(issues_schema.clone()).unwrap();

    // Labels (needed for IssueLabels foreign key)
    let labels_table_schema = TableSchema::new(
        "Labels",
        vec![ColumnDef::required("name", ColumnType::String)],
    );
    db.create_table(labels_table_schema).unwrap();

    // IssueLabels: issue, label
    let issue_labels_schema = TableSchema::new(
        "IssueLabels",
        vec![
            ColumnDef::required("issue", ColumnType::Ref("Issues".into())),
            ColumnDef::required("label", ColumnType::Ref("Labels".into())),
        ],
    );
    db.create_table(issue_labels_schema.clone()).unwrap();

    // Insert project with description
    let project_id = db.insert_with("Projects", |b| b
        .set_string_by_name("name", "Test Project")
        .set_string_by_name("color", "#00ff00")
        .set_string_by_name("description", "A test project")
        .build()).unwrap();

    // Insert issue referencing the project
    let issue_id = db.insert_with("Issues", |b| b
        .set_string_by_name("title", "Test Issue")
        .set_string_by_name("description", "Test description")
        .set_string_by_name("status", "open")
        .set_string_by_name("priority", "high")
        .set_ref_by_name("project", project_id)
        .set_i64_by_name("createdAt", 1234567890)
        .set_i64_by_name("updatedAt", 1234567890)
        .build()).unwrap();

    // Insert label
    let label_id = db.insert_with("Labels", |b| b
        .set_string_by_name("name", "Bug")
        .build()).unwrap();

    // Insert IssueLabel linking issue to label
    db.insert_with("IssueLabels", |b| b
        .set_ref_by_name("issue", issue_id)
        .set_ref_by_name("label", label_id)
        .build()).unwrap();

    // Use incremental_query with JOIN + ARRAY subquery - this is what TypeScript generates
    let sql = "SELECT i.*, ARRAY(SELECT il.* FROM IssueLabels il WHERE il.issue = i.id) as labels FROM Issues i JOIN Projects ON i.project = Projects.id";
    let query = db.incremental_query(sql).expect("should create incremental query");
    let rows = query.rows();

    eprintln!("SQL JOIN+ARRAY query rows: {:?}", rows.len());
    assert_eq!(rows.len(), 1, "Should return 1 issue");

    eprintln!("Row values (len={}):", rows[0].1.descriptor.columns.len());
    for col in &rows[0].1.descriptor.columns {
        eprintln!("  {}: {:?}", col.name, rows[0].1.get_by_name(&col.name));
    }

    // Expected columns:
    // Issues: title, description, status, priority, project, createdAt, updatedAt = 7
    // Projects: name, color, description = 3
    // IssueLabels array = 1
    // Total: 11
    assert_eq!(rows[0].1.descriptor.columns.len(), 11, "Should have 11 values (7 Issues + 3 Projects + 1 array)");

    // Projects.description should be accessible by name
    let proj_desc = rows[0].1.get_by_name("Projects.description");
    eprintln!("\nProjects.description: {:?}", proj_desc);

    // get_by_name returns RowValue which handles nullable transparently
    assert_eq!(proj_desc, Some(RowValue::String("A test project")));

    // Test binary encoding includes the description
    use groove::sql::encode_single_row;
    let binary = encode_single_row(rows[0].0, &rows[0].1);
    eprintln!("\nBinary output ({} bytes):", binary.len());

    let description_bytes = b"A test project";
    let found = binary.windows(description_bytes.len()).any(|w| w == description_bytes);
    assert!(found, "Binary should contain 'A test project' for Projects.description");
}

/// Test that SQL ARRAY subqueries with nested JOINs work correctly.
/// This tests the case where:
///   ARRAY(SELECT il.*, Labels as label FROM IssueLabels il JOIN Labels ON il.label = Labels.id WHERE il.issue = i.id)
/// The ARRAY elements should include the resolved Labels row, not just the FK.
///
/// In buffer format, nested rows are stored as single-item arrays (ColType::Array)
/// rather than a separate Value::Row type.
#[test]
fn incremental_query_array_with_nested_join() {
    let db = Database::in_memory();

    // Labels: name, color
    let labels_schema = TableSchema::new(
        "Labels",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("color", ColumnType::String),
        ],
    );
    db.create_table(labels_schema.clone()).unwrap();

    // Issues: title
    let issues_schema = TableSchema::new(
        "Issues",
        vec![
            ColumnDef::required("title", ColumnType::String),
        ],
    );
    db.create_table(issues_schema.clone()).unwrap();

    // IssueLabels: issue, label
    let issue_labels_schema = TableSchema::new(
        "IssueLabels",
        vec![
            ColumnDef::required("issue", ColumnType::Ref("Issues".into())),
            ColumnDef::required("label", ColumnType::Ref("Labels".into())),
        ],
    );
    db.create_table(issue_labels_schema.clone()).unwrap();

    // Insert label
    let label_id = db.insert_with("Labels", |b| b
        .set_string_by_name("name", "Bug")
        .set_string_by_name("color", "#ff0000")
        .build()).unwrap();

    // Insert issue
    let issue_id = db.insert_with("Issues", |b| b
        .set_string_by_name("title", "Test Issue")
        .build()).unwrap();

    // Insert IssueLabel linking issue to label
    db.insert_with("IssueLabels", |b| b
        .set_ref_by_name("issue", issue_id)
        .set_ref_by_name("label", label_id)
        .build()).unwrap();

    // Query with ARRAY subquery that has a nested JOIN
    // This is what TypeScript generates for: { IssueLabels: { label: true } }
    let sql = "SELECT i.id, i.title, ARRAY(SELECT il.id, il.issue, Labels as label FROM IssueLabels il JOIN Labels ON il.label = Labels.id WHERE il.issue = i.id) as IssueLabels FROM Issues i";
    let query = db.incremental_query(sql).expect("should create incremental query");
    let rows = query.rows();

    eprintln!("ARRAY with nested JOIN query rows: {:?}", rows.len());
    assert_eq!(rows.len(), 1, "Should return 1 issue");

    eprintln!("Row values (len={}):", rows[0].1.descriptor.columns.len());
    eprintln!("Column names: {:?}", rows[0].1.descriptor.columns.iter().map(|c| &c.name).collect::<Vec<_>>());
    for i in 0..rows[0].1.descriptor.columns.len() {
        eprintln!("  [{}]: {:?}", i, rows[0].1.get(i));
    }

    // Expected columns:
    // Issues: title = 1
    // IssueLabels array = 1
    // Total: 2
    assert_eq!(rows[0].1.descriptor.columns.len(), 2, "Should have 2 values (1 Issue column + 1 array)");

    // The array should contain IssueLabel rows with resolved label (use alias from SQL)
    let array = rows[0].1.get(1);  // Use index since we know it's second column
    eprintln!("\nIssueLabels array: {:?}", array);

    if let Some(RowValue::Array(arr)) = array {
        assert_eq!(arr.len(), 1, "Should have 1 IssueLabel");
        // Use iterator to get first item
        let issue_label_row = arr.iter().next().unwrap();
        // IssueLabel row should have: id (implicit), issue (Ref), label (resolved Row)
        // With nested JOIN, the values should be:
        // [0] = issue (Ref to Issues)
        // [1] = label (resolved Labels Row with id, name, color)
        eprintln!("IssueLabel row values (len={}):", issue_label_row.descriptor.columns.len());
        for i in 0..issue_label_row.descriptor.columns.len() {
            eprintln!("  [{}]: {:?}", i, issue_label_row.get(i));
        }

        // The label should be a nested Row, not a Ref
        // Expected: [Ref(issue_id), Row(Labels row)]
        assert_eq!(
            issue_label_row.descriptor.columns.len(),
            2,
            "IssueLabel row should have 2 values (issue + label), got {}",
            issue_label_row.descriptor.columns.len()
        );

        // Check that issue column is a Ref to the Issue
        assert!(
            matches!(issue_label_row.get_by_name("issue"), Some(RowValue::Ref(_))),
            "issue should be a Ref to Issue, got: {:?}",
            issue_label_row.get_by_name("issue")
        );

        // Check that label is a nested Row (resolved Labels)
        // NOTE: In buffer format, nested rows are stored as single-item Arrays
        if let Some(RowValue::Array(label_arr)) = issue_label_row.get_by_name("label") {
            assert_eq!(label_arr.len(), 1, "Nested row should be a single-item array");
            let label_row = label_arr.iter().next().unwrap();

            eprintln!("Label row values (len={}):", label_row.descriptor.columns.len());
            eprintln!("Label column names: {:?}", label_row.descriptor.columns.iter().map(|c| &c.name).collect::<Vec<_>>());
            for i in 0..label_row.descriptor.columns.len() {
                eprintln!("  [{}]: {:?}", i, label_row.get(i));
            }

            // Labels row should have 2 values: name, color
            assert_eq!(
                label_row.descriptor.columns.len(),
                2,
                "Label row should have 2 values (name + color), got {}",
                label_row.descriptor.columns.len()
            );

            // Check label name
            assert_eq!(
                label_row.get_by_name("name"),
                Some(RowValue::String("Bug")),
                "Labels.name should be 'Bug', got: {:?}",
                label_row.get_by_name("name")
            );

            // Check label color
            assert_eq!(
                label_row.get_by_name("color"),
                Some(RowValue::String("#ff0000")),
                "Labels.color should be '#ff0000', got: {:?}",
                label_row.get_by_name("color")
            );
        } else {
            panic!("label should be an Array (nested row as single-item array), got: {:?}", issue_label_row.get_by_name("label"));
        }
    } else {
        panic!("IssueLabels should be an Array, got: {:?}", array);
    }
}

/// Test that TableRow projection + ARRAY subqueries work together.
/// This tests the exact SQL pattern TypeScript generates:
///   SELECT i.id, i.title, ..., Projects as project, ARRAY(...) as IssueLabels FROM Issues i JOIN Projects
#[test]
fn incremental_query_table_row_plus_array_subquery() {
    let db = Database::in_memory();

    // Projects: name, color, description (nullable)
    let projects_schema = TableSchema::new(
        "Projects",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("color", ColumnType::String),
            ColumnDef::optional("description", ColumnType::String),
        ],
    );
    db.create_table(projects_schema.clone()).unwrap();

    // Issues: title, description, status, priority, project, createdAt, updatedAt
    let issues_schema = TableSchema::new(
        "Issues",
        vec![
            ColumnDef::required("title", ColumnType::String),
            ColumnDef::optional("description", ColumnType::String),
            ColumnDef::required("status", ColumnType::String),
            ColumnDef::required("priority", ColumnType::String),
            ColumnDef::required("project", ColumnType::Ref("Projects".into())),
            ColumnDef::required("createdAt", ColumnType::I64),
            ColumnDef::required("updatedAt", ColumnType::I64),
        ],
    );
    db.create_table(issues_schema.clone()).unwrap();

    // IssueLabels: issue, label
    let issue_labels_schema = TableSchema::new(
        "IssueLabels",
        vec![
            ColumnDef::required("issue", ColumnType::Ref("Issues".into())),
            ColumnDef::required("name", ColumnType::String),
        ],
    );
    db.create_table(issue_labels_schema.clone()).unwrap();

    // Insert project
    let project_id = db.insert_with("Projects", |b| b
        .set_string_by_name("name", "Test Project")
        .set_string_by_name("color", "#00ff00")
        .set_string_by_name("description", "A test project")
        .build()).unwrap();

    // Insert issue
    let issue_id = db.insert_with("Issues", |b| b
        .set_string_by_name("title", "Test Issue")
        .set_string_by_name("description", "Test description")
        .set_string_by_name("status", "open")
        .set_string_by_name("priority", "high")
        .set_ref_by_name("project", project_id)
        .set_i64_by_name("createdAt", 1234567890)
        .set_i64_by_name("updatedAt", 1234567890)
        .build()).unwrap();

    // Insert IssueLabel
    db.insert_with("IssueLabels", |b| b
        .set_ref_by_name("issue", issue_id)
        .set_string_by_name("name", "Bug")
        .build()).unwrap();

    // Query with TableRow projection (Projects as project) + ARRAY subquery
    // This is what TypeScript generates for: .with({ project: true, IssueLabels: true })
    let sql = "SELECT i.id, i.title, i.description, i.status, i.priority, i.project, i.createdAt, i.updatedAt, Projects as project, ARRAY(SELECT il.id, il.issue, il.name FROM IssueLabels il WHERE il.issue = i.id) as IssueLabels FROM Issues i JOIN Projects ON i.project = Projects.id";

    eprintln!("SQL: {}", sql);
    let query = db.incremental_query(sql).expect("should create incremental query");
    let rows = query.rows();

    eprintln!("Query returned {} rows", rows.len());
    assert_eq!(rows.len(), 1, "Should return 1 issue");

    eprintln!("Row columns (len={}):", rows[0].1.descriptor.columns.len());
    for col in &rows[0].1.descriptor.columns {
        eprintln!("  {}: {:?} = {:?}", col.name, col.ty, rows[0].1.get_by_name(&col.name));
    }

    // Expected columns:
    // Issues: project, createdAt, updatedAt = 3 fixed
    // Issues: title, description, status, priority = 4 variable
    // Projects (expanded): name, color, description = 3 variable
    // IssueLabels array = 1 variable
    // Total: 11 (7 from Issues + 3 from Projects + 1 array)
    // Note: Groove expands JOIN columns instead of bundling as TableRow
    assert_eq!(
        rows[0].1.descriptor.columns.len(), 11,
        "Should have 11 columns (7 Issues + 3 Projects expanded + 1 IssueLabels array)"
    );

    // Check that IssueLabels array has 1 item
    let labels = rows[0].1.get_by_name("IssueLabels");
    eprintln!("\nIssueLabels: {:?}", labels);
    assert!(
        matches!(labels, Some(RowValue::Array(arr)) if arr.len() == 1),
        "IssueLabels should be an array with 1 item, got: {:?}",
        labels
    );
}

/// Test multiple ARRAY subqueries in a single query.
/// This verifies that multiple array columns are properly accumulated.
#[test]
fn incremental_query_multiple_array_subqueries() {
    let db = Database::in_memory();

    // Create Projects table
    let projects_schema = TableSchema::new(
        "Projects",
        vec![
            ColumnDef::required("name", ColumnType::String),
        ],
    );
    db.create_table(projects_schema.clone()).unwrap();

    // Create Issues table
    let issues_schema = TableSchema::new(
        "Issues",
        vec![
            ColumnDef::required("title", ColumnType::String),
            ColumnDef::required("project", ColumnType::Ref("Projects".into())),
        ],
    );
    db.create_table(issues_schema.clone()).unwrap();

    // IssueLabels: issue, name
    let issue_labels_schema = TableSchema::new(
        "IssueLabels",
        vec![
            ColumnDef::required("issue", ColumnType::Ref("Issues".into())),
            ColumnDef::required("name", ColumnType::String),
        ],
    );
    db.create_table(issue_labels_schema.clone()).unwrap();

    // IssueAssignees: issue, name
    let issue_assignees_schema = TableSchema::new(
        "IssueAssignees",
        vec![
            ColumnDef::required("issue", ColumnType::Ref("Issues".into())),
            ColumnDef::required("name", ColumnType::String),
        ],
    );
    db.create_table(issue_assignees_schema.clone()).unwrap();

    // Insert data
    let project_id = db.insert_with("Projects", |b| b
        .set_string_by_name("name", "Test Project")
        .build()).unwrap();

    let issue_id = db.insert_with("Issues", |b| b
        .set_string_by_name("title", "Test Issue")
        .set_ref_by_name("project", project_id)
        .build()).unwrap();

    db.insert_with("IssueLabels", |b| b
        .set_ref_by_name("issue", issue_id)
        .set_string_by_name("name", "Bug")
        .build()).unwrap();

    db.insert_with("IssueAssignees", |b| b
        .set_ref_by_name("issue", issue_id)
        .set_string_by_name("name", "Alice")
        .build()).unwrap();

    // Query with TWO ARRAY subqueries
    let sql = "SELECT i.id, i.title, i.project, \
               ARRAY(SELECT il.id, il.issue, il.name FROM IssueLabels il WHERE il.issue = i.id) as IssueLabels, \
               ARRAY(SELECT ia.id, ia.issue, ia.name FROM IssueAssignees ia WHERE ia.issue = i.id) as IssueAssignees \
               FROM Issues i JOIN Projects ON i.project = Projects.id";

    eprintln!("SQL: {}", sql);
    let query = db.incremental_query(sql).expect("should create incremental query");
    let rows = query.rows();

    eprintln!("Query returned {} rows", rows.len());
    assert_eq!(rows.len(), 1, "Should return 1 issue");

    eprintln!("Row columns (len={}):", rows[0].1.descriptor.columns.len());
    for col in &rows[0].1.descriptor.columns {
        eprintln!("  {}: {:?} = {:?}", col.name, col.ty, rows[0].1.get_by_name(&col.name));
    }

    // Should have both IssueLabels and IssueAssignees arrays
    let labels = rows[0].1.get_by_name("IssueLabels");
    let assignees = rows[0].1.get_by_name("IssueAssignees");

    eprintln!("\nIssueLabels: {:?}", labels);
    eprintln!("IssueAssignees: {:?}", assignees);

    assert!(
        matches!(labels, Some(RowValue::Array(arr)) if arr.len() == 1),
        "IssueLabels should be an array with 1 item, got: {:?}",
        labels
    );

    assert!(
        matches!(assignees, Some(RowValue::Array(arr)) if arr.len() == 1),
        "IssueAssignees should be an array with 1 item, got: {:?}",
        assignees
    );
}

/// Test ARRAY subqueries with inner JOINs - matches demo app pattern.
/// SQL: ARRAY(SELECT ... FROM IssueLabels il JOIN Labels ON il.label = Labels.id ...)
#[test]
fn incremental_query_array_with_inner_join() {
    let db = Database::in_memory();

    // Create Labels table (the joined table inside ARRAY)
    let labels_schema = TableSchema::new(
        "Labels",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("color", ColumnType::String),
        ],
    );
    db.create_table(labels_schema.clone()).unwrap();

    // Create Issues table
    let issues_schema = TableSchema::new(
        "Issues",
        vec![
            ColumnDef::required("title", ColumnType::String),
        ],
    );
    db.create_table(issues_schema.clone()).unwrap();

    // IssueLabels: issue (Ref to Issues), label (Ref to Labels)
    let issue_labels_schema = TableSchema::new(
        "IssueLabels",
        vec![
            ColumnDef::required("issue", ColumnType::Ref("Issues".into())),
            ColumnDef::required("label", ColumnType::Ref("Labels".into())),
        ],
    );
    db.create_table(issue_labels_schema.clone()).unwrap();

    // Insert data
    let label_bug = db.insert_with("Labels", |b| b
        .set_string_by_name("name", "Bug")
        .set_string_by_name("color", "#ff0000")
        .build()).unwrap();

    let issue_id = db.insert_with("Issues", |b| b
        .set_string_by_name("title", "Test Issue")
        .build()).unwrap();

    db.insert_with("IssueLabels", |b| b
        .set_ref_by_name("issue", issue_id)
        .set_ref_by_name("label", label_bug)
        .build()).unwrap();

    // Query mimicking TypeScript pattern: ARRAY with inner JOIN
    // SELECT i.id, i.title, ARRAY(SELECT il.id, il.issue, Labels as label FROM IssueLabels il JOIN Labels ON il.label = Labels.id WHERE il.issue = i.id) as IssueLabels FROM Issues i
    let sql = "SELECT i.id, i.title, \
               ARRAY(SELECT il.id, il.issue, Labels as label FROM IssueLabels il JOIN Labels ON il.label = Labels.id WHERE il.issue = i.id) as IssueLabels \
               FROM Issues i";

    eprintln!("SQL: {}", sql);
    let query = db.incremental_query(sql).expect("should create incremental query");
    let rows = query.rows();

    eprintln!("Query returned {} rows", rows.len());
    assert_eq!(rows.len(), 1, "Should return 1 issue");

    eprintln!("Row columns (len={}):", rows[0].1.descriptor.columns.len());
    for col in &rows[0].1.descriptor.columns {
        eprintln!("  {}: {:?} = {:?}", col.name, col.ty, rows[0].1.get_by_name(&col.name));
    }

    // Check array has items
    let labels = rows[0].1.get_by_name("IssueLabels");
    eprintln!("\nIssueLabels: {:?}", labels);

    assert!(
        matches!(labels, Some(RowValue::Array(arr)) if arr.len() == 1),
        "IssueLabels should be an array with 1 item (the Bug label), got: {:?}",
        labels
    );
}

/// Test that matches the demo app SQL exactly:
/// Outer JOIN + multiple ARRAY subqueries with inner JOINs
#[test]
fn incremental_query_outer_join_with_inner_array_joins() {
    let db = Database::in_memory();

    // Create all tables matching demo app schema
    let projects_schema = TableSchema::new(
        "Projects",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("color", ColumnType::String),
        ],
    );
    db.create_table(projects_schema.clone()).unwrap();

    let labels_schema = TableSchema::new(
        "Labels",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("color", ColumnType::String),
        ],
    );
    db.create_table(labels_schema.clone()).unwrap();

    let users_schema = TableSchema::new(
        "Users",
        vec![
            ColumnDef::required("name", ColumnType::String),
        ],
    );
    db.create_table(users_schema.clone()).unwrap();

    let issues_schema = TableSchema::new(
        "Issues",
        vec![
            ColumnDef::required("title", ColumnType::String),
            ColumnDef::required("project", ColumnType::Ref("Projects".into())),
        ],
    );
    db.create_table(issues_schema.clone()).unwrap();

    let issue_labels_schema = TableSchema::new(
        "IssueLabels",
        vec![
            ColumnDef::required("issue", ColumnType::Ref("Issues".into())),
            ColumnDef::required("label", ColumnType::Ref("Labels".into())),
        ],
    );
    db.create_table(issue_labels_schema.clone()).unwrap();

    let issue_assignees_schema = TableSchema::new(
        "IssueAssignees",
        vec![
            ColumnDef::required("issue", ColumnType::Ref("Issues".into())),
            ColumnDef::required("user", ColumnType::Ref("Users".into())),
        ],
    );
    db.create_table(issue_assignees_schema.clone()).unwrap();

    // Insert data
    let project_id = db.insert_with("Projects", |b| b
        .set_string_by_name("name", "Test Project")
        .set_string_by_name("color", "#00ff00")
        .build()).unwrap();

    let label_bug = db.insert_with("Labels", |b| b
        .set_string_by_name("name", "Bug")
        .set_string_by_name("color", "#ff0000")
        .build()).unwrap();

    let user_alice = db.insert_with("Users", |b| b
        .set_string_by_name("name", "Alice")
        .build()).unwrap();

    let issue_id = db.insert_with("Issues", |b| b
        .set_string_by_name("title", "Test Issue")
        .set_ref_by_name("project", project_id)
        .build()).unwrap();

    db.insert_with("IssueLabels", |b| b
        .set_ref_by_name("issue", issue_id)
        .set_ref_by_name("label", label_bug)
        .build()).unwrap();

    db.insert_with("IssueAssignees", |b| b
        .set_ref_by_name("issue", issue_id)
        .set_ref_by_name("user", user_alice)
        .build()).unwrap();

    // Query matching demo app: Outer JOIN + ARRAY subqueries with inner JOINs
    let sql = "SELECT i.id, i.title, i.project, Projects as project, \
               ARRAY(SELECT il.id, il.issue, Labels as label FROM IssueLabels il JOIN Labels ON il.label = Labels.id WHERE il.issue = i.id) as IssueLabels, \
               ARRAY(SELECT ia.id, ia.issue, Users as user FROM IssueAssignees ia JOIN Users ON ia.user = Users.id WHERE ia.issue = i.id) as IssueAssignees \
               FROM Issues i JOIN Projects ON i.project = Projects.id";

    eprintln!("SQL: {}", sql);
    let query = db.incremental_query(sql).expect("should create incremental query");
    let rows = query.rows();

    eprintln!("Query returned {} rows", rows.len());
    assert_eq!(rows.len(), 1, "Should return 1 issue");

    eprintln!("Row columns (len={}):", rows[0].1.descriptor.columns.len());
    for col in &rows[0].1.descriptor.columns {
        eprintln!("  {}: {:?} = {:?}", col.name, col.ty, rows[0].1.get_by_name(&col.name));
    }

    // Check arrays have items
    let labels = rows[0].1.get_by_name("IssueLabels");
    let assignees = rows[0].1.get_by_name("IssueAssignees");

    eprintln!("\nIssueLabels: {:?}", labels);
    eprintln!("IssueAssignees: {:?}", assignees);

    assert!(
        matches!(labels, Some(RowValue::Array(arr)) if arr.len() == 1),
        "IssueLabels should be an array with 1 item, got: {:?}",
        labels
    );

    assert!(
        matches!(assignees, Some(RowValue::Array(arr)) if arr.len() == 1),
        "IssueAssignees should be an array with 1 item, got: {:?}",
        assignees
    );
}

/// Test that filter JOIN + ARRAY subqueries work together.
/// This is the exact failing scenario from TypeScript tests:
///   SELECT ... FROM Issues i
///   JOIN Projects ON i.project = Projects.id
///   JOIN IssueLabels ON IssueLabels.issue = i.id
///   WHERE IssueLabels.label = 'xxx'
/// Combined with ARRAY subqueries for includes.
#[test]
fn filter_join_plus_array_subqueries() {
    let db = Database::in_memory();

    // Create all tables matching demo app schema
    let projects_schema = TableSchema::new(
        "Projects",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("color", ColumnType::String),
        ],
    );
    db.create_table(projects_schema.clone()).unwrap();

    let labels_schema = TableSchema::new(
        "Labels",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("color", ColumnType::String),
        ],
    );
    db.create_table(labels_schema.clone()).unwrap();

    let users_schema = TableSchema::new(
        "Users",
        vec![ColumnDef::required("name", ColumnType::String)],
    );
    db.create_table(users_schema.clone()).unwrap();

    let issues_schema = TableSchema::new(
        "Issues",
        vec![
            ColumnDef::required("title", ColumnType::String),
            ColumnDef::required("project", ColumnType::Ref("Projects".into())),
        ],
    );
    db.create_table(issues_schema.clone()).unwrap();

    // Junction table: IssueLabels
    let issue_labels_schema = TableSchema::new(
        "IssueLabels",
        vec![
            ColumnDef::required("issue", ColumnType::Ref("Issues".into())),
            ColumnDef::required("label", ColumnType::Ref("Labels".into())),
        ],
    );
    db.create_table(issue_labels_schema.clone()).unwrap();

    // Junction table: IssueAssignees
    let issue_assignees_schema = TableSchema::new(
        "IssueAssignees",
        vec![
            ColumnDef::required("issue", ColumnType::Ref("Issues".into())),
            ColumnDef::required("user", ColumnType::Ref("Users".into())),
        ],
    );
    db.create_table(issue_assignees_schema.clone()).unwrap();

    // Insert test data
    let project_id = db.insert_with("Projects", |b| {
        b.set_string_by_name("name", "Test Project")
            .set_string_by_name("color", "#00ff00")
            .build()
    }).unwrap();

    let label_bug = db.insert_with("Labels", |b| {
        b.set_string_by_name("name", "Bug")
            .set_string_by_name("color", "#ff0000")
            .build()
    }).unwrap();

    let user_alice = db.insert_with("Users", |b| {
        b.set_string_by_name("name", "Alice")
            .build()
    }).unwrap();

    let issue_id = db.insert_with("Issues", |b| {
        b.set_string_by_name("title", "Test Issue")
            .set_ref_by_name("project", project_id)
            .build()
    }).unwrap();

    // Link issue to label and user
    db.insert_with("IssueLabels", |b| {
        b.set_ref_by_name("issue", issue_id)
            .set_ref_by_name("label", label_bug)
            .build()
    }).unwrap();

    db.insert_with("IssueAssignees", |b| {
        b.set_ref_by_name("issue", issue_id)
            .set_ref_by_name("user", user_alice)
            .build()
    }).unwrap();

    // This is the exact SQL pattern that fails in TypeScript:
    // - JOIN Projects (forward ref include)
    // - JOIN IssueLabels (for filter)
    // - WHERE IssueLabels.label = '...' (reverse join filter)
    // - ARRAY subqueries for includes
    let sql = format!(
        "SELECT i.id, i.title, i.project, Projects as project, \
         ARRAY(SELECT il.id, il.issue, Labels as label FROM IssueLabels il JOIN Labels ON il.label = Labels.id WHERE il.issue = i.id) as IssueLabels, \
         ARRAY(SELECT ia.id, ia.issue, Users as user FROM IssueAssignees ia JOIN Users ON ia.user = Users.id WHERE ia.issue = i.id) as IssueAssignees \
         FROM Issues i \
         JOIN Projects ON i.project = Projects.id \
         JOIN IssueLabels ON IssueLabels.issue = i.id \
         WHERE IssueLabels.label = '{}'",
        label_bug
    );

    eprintln!("SQL: {}", sql);
    let query = db.incremental_query(&sql).expect("should create incremental query");

    // Print the query graph for debugging
    let diagram = query.diagram();
    eprintln!("Query Graph:\n{}", diagram);

    let rows = query.rows();
    eprintln!("Query returned {} rows", rows.len());

    // This is the failing assertion - currently returns 0 rows
    assert_eq!(rows.len(), 1, "Should return 1 issue matching the filter");

    if !rows.is_empty() {
        eprintln!("Row columns (len={}):", rows[0].1.descriptor.columns.len());
        for col in &rows[0].1.descriptor.columns {
            eprintln!("  {}: {:?} = {:?}", col.name, col.ty, rows[0].1.get_by_name(&col.name));
        }

        // Verify includes work
        let labels_arr = rows[0].1.get_by_name("IssueLabels");
        let assignees_arr = rows[0].1.get_by_name("IssueAssignees");

        eprintln!("\nIssueLabels: {:?}", labels_arr);
        eprintln!("IssueAssignees: {:?}", assignees_arr);

        assert!(
            matches!(labels_arr, Some(RowValue::Array(arr)) if arr.len() == 1),
            "IssueLabels should be an array with 1 item, got: {:?}",
            labels_arr
        );

        assert!(
            matches!(assignees_arr, Some(RowValue::Array(arr)) if arr.len() == 1),
            "IssueAssignees should be an array with 1 item, got: {:?}",
            assignees_arr
        );
    }
}
