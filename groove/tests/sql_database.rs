//! Integration tests for sql::Database

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use groove::sql::{
    ColumnDef, ColumnType, Database, DatabaseError, ExecuteResult, TableSchema, Value,
};
use groove::ObjectId;

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

    db.create_table(TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::optional("age", ColumnType::I64),
        ],
    ))
    .unwrap();

    let id = db
        .insert(
            "users",
            &["name", "age"],
            vec![Value::String("Alice".into()), Value::I64(30)],
        )
        .unwrap();

    let row = db.get("users", id).unwrap().unwrap();
    assert_eq!(row.id, id);
    assert_eq!(row.values[0], Value::String("Alice".into()));
    // age is optional, so it's wrapped in NullableSome
    assert_eq!(row.values[1], Value::NullableSome(Box::new(Value::I64(30))));
}

#[test]
fn insert_with_null() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::optional("email", ColumnType::String),
        ],
    ))
    .unwrap();

    let id = db
        .insert("users", &["name"], vec![Value::String("Bob".into())])
        .unwrap();

    let row = db.get("users", id).unwrap().unwrap();
    assert_eq!(row.values[1], Value::NullableNone);
}

#[test]
fn insert_missing_required_column() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    ))
    .unwrap();

    let result = db.insert("users", &[], vec![]);
    assert!(matches!(result, Err(DatabaseError::MissingColumn(_))));
}

// ========== Update Tests ==========

#[test]
fn update_row() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::optional("age", ColumnType::I64),
        ],
    ))
    .unwrap();

    let id = db
        .insert(
            "users",
            &["name", "age"],
            vec![Value::String("Alice".into()), Value::I64(30)],
        )
        .unwrap();

    let updated = db.update("users", id, &[("age", Value::I64(31))]).unwrap();
    assert!(updated);

    let row = db.get("users", id).unwrap().unwrap();
    // age is optional, so it's wrapped in NullableSome
    assert_eq!(row.values[1], Value::NullableSome(Box::new(Value::I64(31))));
}

// ========== Delete Tests ==========

#[test]
fn delete_row() {
    let db = Database::in_memory();

    db.create_table(TableSchema::new(
        "users",
        vec![ColumnDef::required("name", ColumnType::String)],
    ))
    .unwrap();

    let id = db
        .insert("users", &["name"], vec![Value::String("Alice".into())])
        .unwrap();

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

    db.insert(
        "users",
        &["name", "active"],
        vec![Value::String("Alice".into()), Value::Bool(true)],
    )
    .unwrap();
    db.insert(
        "users",
        &["name", "active"],
        vec![Value::String("Bob".into()), Value::Bool(false)],
    )
    .unwrap();
    db.insert(
        "users",
        &["name", "active"],
        vec![Value::String("Carol".into()), Value::Bool(true)],
    )
    .unwrap();

    let rows = db.select_all("users").unwrap();
    assert_eq!(rows.len(), 3);

    // Verify specific row properties
    let names: Vec<_> = rows.iter().map(|r| &r.values[0]).collect();
    assert!(names.contains(&&Value::String("Alice".into())));
    assert!(names.contains(&&Value::String("Bob".into())));
    assert!(names.contains(&&Value::String("Carol".into())));
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

    db.insert(
        "users",
        &["name", "active"],
        vec![Value::String("Alice".into()), Value::Bool(true)],
    )
    .unwrap();
    db.insert(
        "users",
        &["name", "active"],
        vec![Value::String("Bob".into()), Value::Bool(false)],
    )
    .unwrap();
    db.insert(
        "users",
        &["name", "active"],
        vec![Value::String("Carol".into()), Value::Bool(true)],
    )
    .unwrap();

    let active = db.select_where("users", "active", &Value::Bool(true)).unwrap();
    assert_eq!(active.len(), 2);
    // Verify active users are Alice and Carol
    let active_names: Vec<_> = active.iter().map(|r| &r.values[0]).collect();
    assert!(active_names.contains(&&Value::String("Alice".into())));
    assert!(active_names.contains(&&Value::String("Carol".into())));

    let inactive = db
        .select_where("users", "active", &Value::Bool(false))
        .unwrap();
    assert_eq!(inactive.len(), 1);
    assert_eq!(inactive[0].values[0], Value::String("Bob".into()));
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
        ExecuteResult::Inserted { row_id: id, .. } => {
            let row = db.get("users", id).unwrap().unwrap();
            assert_eq!(row.values[0], Value::String("Alice".into()));
            // age is optional (no NOT NULL), so it's wrapped in NullableSome
            assert_eq!(row.values[1], Value::NullableSome(Box::new(Value::I64(30))));
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
            let names: Vec<_> = rows.iter().map(|r| &r.values[0]).collect();
            assert!(names.contains(&&Value::String("Alice".into())));
            assert!(names.contains(&&Value::String("Bob".into())));
        }
        _ => panic!("expected Selected"),
    }

    let result = db
        .execute("SELECT * FROM users WHERE active = true")
        .unwrap();
    match result {
        ExecuteResult::Selected(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], Value::String("Alice".into()));
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
    // age is optional (no NOT NULL), so it's wrapped in NullableSome
    assert_eq!(row.values[1], Value::NullableSome(Box::new(Value::I64(31))));
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
    let names: Vec<_> = remaining.iter().map(|r| &r.values[0]).collect();
    assert!(names.contains(&&Value::String("Bob".into())));
    assert!(names.contains(&&Value::String("Carol".into())));
    assert!(!names.contains(&&Value::String("Alice".into())));
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
    let names: Vec<_> = remaining.iter().map(|r| &r.values[0]).collect();
    assert!(names.contains(&&Value::String("Alice".into())));
    assert!(names.contains(&&Value::String("Carol".into())));
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
    let result = db.insert(
        "posts",
        &["author", "title"],
        vec![
            Value::Ref(ObjectId::new(0x12345)), // fake user ID
            Value::String("Hello".into()),
        ],
    );
    assert!(matches!(result, Err(DatabaseError::InvalidReference { .. })));

    // Create a user
    let user_id = db
        .insert("users", &["name"], vec![Value::String("Alice".into())])
        .unwrap();

    // Now insert post with valid ref works
    let post_id = db
        .insert(
            "posts",
            &["author", "title"],
            vec![Value::Ref(user_id), Value::String("Hello".into())],
        )
        .unwrap();

    let post = db.get("posts", post_id).unwrap().unwrap();
    assert_eq!(post.values[0], Value::Ref(user_id));
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

    let alice_id = db
        .insert("users", &["name"], vec![Value::String("Alice".into())])
        .unwrap();
    let bob_id = db
        .insert("users", &["name"], vec![Value::String("Bob".into())])
        .unwrap();

    // Create posts by Alice
    db.insert(
        "posts",
        &["author", "title"],
        vec![Value::Ref(alice_id), Value::String("Post 1".into())],
    )
    .unwrap();
    db.insert(
        "posts",
        &["author", "title"],
        vec![Value::Ref(alice_id), Value::String("Post 2".into())],
    )
    .unwrap();

    // Create post by Bob
    db.insert(
        "posts",
        &["author", "title"],
        vec![Value::Ref(bob_id), Value::String("Bob's Post".into())],
    )
    .unwrap();

    // Find all posts by Alice
    let alice_posts = db.find_referencing("posts", "author", alice_id).unwrap();
    assert_eq!(alice_posts.len(), 2);
    let alice_titles: Vec<_> = alice_posts.iter().map(|r| &r.values[1]).collect();
    assert!(alice_titles.contains(&&Value::String("Post 1".into())));
    assert!(alice_titles.contains(&&Value::String("Post 2".into())));

    // Find all posts by Bob
    let bob_posts = db.find_referencing("posts", "author", bob_id).unwrap();
    assert_eq!(bob_posts.len(), 1);
    assert_eq!(bob_posts[0].values[1], Value::String("Bob's Post".into()));
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

    let alice_id = db
        .insert("users", &["name"], vec![Value::String("Alice".into())])
        .unwrap();
    let bob_id = db
        .insert("users", &["name"], vec![Value::String("Bob".into())])
        .unwrap();

    let post_id = db
        .insert(
            "posts",
            &["author", "title"],
            vec![Value::Ref(alice_id), Value::String("A Post".into())],
        )
        .unwrap();

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
    db.update("posts", post_id, &[("author", Value::Ref(bob_id))])
        .unwrap();

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

    let alice_id = db
        .insert("users", &["name"], vec![Value::String("Alice".into())])
        .unwrap();
    let post_id = db
        .insert(
            "posts",
            &["author", "title"],
            vec![Value::Ref(alice_id), Value::String("A Post".into())],
        )
        .unwrap();

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
    let post_id = db
        .insert("posts", &["title"], vec![Value::String("Anonymous".into())])
        .unwrap();
    let post = db.get("posts", post_id).unwrap().unwrap();
    assert_eq!(post.values[0], Value::NullableNone);

    // Insert post with author
    let user_id = db
        .insert("users", &["name"], vec![Value::String("Alice".into())])
        .unwrap();
    let post2_id = db
        .insert(
            "posts",
            &["author", "title"],
            vec![Value::Ref(user_id), Value::String("By Alice".into())],
        )
        .unwrap();

    // Only the authored post shows in index
    let posts = db.find_referencing("posts", "author", user_id).unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].id, post2_id);
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
                assert_eq!(row.values.len(), 3, "Should have 3 columns (2 from posts + 1 from users)");
                // All rows should have Alice as the author (via join)
                assert_eq!(row.values[2], Value::String("Alice".to_string()));
            }
            // Verify both post titles are present
            let titles: Vec<_> = rows.iter().map(|r| &r.values[1]).collect();
            assert!(titles.contains(&&Value::String("First Post".into())));
            assert!(titles.contains(&&Value::String("Second Post".into())));
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
            assert_eq!(rows[0].values[1], Value::String("First Post".into()));
            assert_eq!(rows[0].values[2], Value::String("Alice".into()));
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("Expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
                assert_eq!(row.values[2], Value::String("Alice".into()));
            }
            // Verify both Alice's posts are present
            let titles: Vec<_> = rows.iter().map(|r| &r.values[1]).collect();
            assert!(titles.contains(&&Value::String("Alice Post 1".into())));
            assert!(titles.contains(&&Value::String("Alice Post 2".into())));
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
            assert_eq!(rows[0].values.len(), 1, "Should only have users columns");
            assert_eq!(rows[0].values[0], Value::String("Alice".to_string()));
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("Expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name, active) VALUES ('Bob', false)")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("Expected Inserted"),
    };
    let charlie_id = match db
        .execute("INSERT INTO users (name, active) VALUES ('Charlie', true)")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
            // Row has: author (ref), title, name, active -> name is index 2
            assert_eq!(rows[0].values[2], Value::String("Alice".to_string()));
            assert_eq!(rows[0].values[1], Value::String("Hello".to_string()));
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
                assert_eq!(row.values[3], Value::Bool(true));
            }
            // Verify authors are Alice and Charlie
            let names: Vec<_> = rows.iter().map(|r| &r.values[2]).collect();
            assert!(names.contains(&&Value::String("Alice".into())));
            assert!(names.contains(&&Value::String("Charlie".into())));
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
                assert_eq!(row.values[1], Value::String("Hello".into()));
            }
            // Verify authors are Alice and Bob
            let names: Vec<_> = rows.iter().map(|r| &r.values[2]).collect();
            assert!(names.contains(&&Value::String("Alice".into())));
            assert!(names.contains(&&Value::String("Bob".into())));
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
    let names: Vec<_> = rows.iter().map(|r| &r.values[0]).collect();
    assert!(names.contains(&&Value::String("Alice".into())));
    assert!(names.contains(&&Value::String("Bob".into())));
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
    let names: Vec<_> = rows.iter().map(|r| &r.values[0]).collect();
    assert!(names.contains(&&Value::String("Alice".into())));
    assert!(names.contains(&&Value::String("Carol".into())));
    assert!(!names.contains(&&Value::String("Bob".into())));
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
    assert_eq!(initial_rows[0].values[0], Value::String("Alice".into()));

    // Insert another row - query auto-updates incrementally
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();

    // Query immediately has 2 rows
    let updated_rows = query.rows();
    assert_eq!(updated_rows.len(), 2);
    let names: Vec<_> = updated_rows.iter().map(|r| &r.values[0]).collect();
    assert!(names.contains(&&Value::String("Alice".into())));
    assert!(names.contains(&&Value::String("Bob".into())));
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let query = db
        .incremental_query("SELECT * FROM users WHERE name = 'Alice'")
        .unwrap();
    let initial_rows = query.rows();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(initial_rows[0].values[0], Value::String("Alice".into()));

    // Update the row to have a different name
    db.update("users", id, &[("name", Value::String("Alicia".into()))])
        .unwrap();

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
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("expected Inserted"),
    };

    let query = db.incremental_query("SELECT * FROM users").unwrap();
    let initial_rows = query.rows();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(initial_rows[0].values[0], Value::String("Alice".into()));

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
    let row_counts = Arc::new(RwLock::new(Vec::<usize>::new()));
    let call_count_clone = call_count.clone();
    let row_counts_clone = row_counts.clone();

    // Subscribe with rows callback
    let _id = query.subscribe(move |rows| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
        row_counts_clone.write().unwrap().push(rows.len());
    });

    // Insert a new row - callback should be called
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();

    // Callback should have been called with 2 rows
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(*row_counts.read().unwrap(), vec![2]);

    // Insert another row
    db.execute("INSERT INTO users (name) VALUES ('Charlie')")
        .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 2);
    assert_eq!(*row_counts.read().unwrap(), vec![2, 3]);
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("Expected Inserted"),
    };
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();

    // Verify we have 2 rows with correct names
    let initial_rows = query.rows();
    assert_eq!(initial_rows.len(), 2);
    let names: Vec<_> = initial_rows.iter().map(|r| &r.values[0]).collect();
    assert!(names.contains(&&Value::String("Alice".into())));
    assert!(names.contains(&&Value::String("Bob".into())));

    let call_count = Arc::new(AtomicUsize::new(0));
    let row_counts = Arc::new(RwLock::new(Vec::<usize>::new()));
    let call_count_clone = call_count.clone();
    let row_counts_clone = row_counts.clone();

    let _sub_id = query.subscribe(move |rows| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
        row_counts_clone.write().unwrap().push(rows.len());
    });

    // Delete a row - callback should be triggered
    db.delete("users", id1).unwrap();

    // Callback called with 1 row remaining
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(*row_counts.read().unwrap(), vec![1]);
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
    assert_eq!(rows[0].values[1], Value::String("First Post".into()));
    assert_eq!(rows[0].values[2], Value::String("Alice".into()));
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
    let row_counts = Arc::new(RwLock::new(Vec::<usize>::new()));
    let call_count_clone = call_count.clone();
    let row_counts_clone = row_counts.clone();

    let _sub_id = query.subscribe(move |rows| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
        row_counts_clone.write().unwrap().push(rows.len());
    });

    // Insert another post - should trigger callback
    db.execute(&format!(
        "INSERT INTO posts (author, title) VALUES ('{}', 'Second Post')",
        alice_id
    ))
    .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(*row_counts.read().unwrap(), vec![2]);
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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

    let _sub_id = query.subscribe_delta(Box::new(move |_delta| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
    }));

    // Update user - should trigger callback since the joined row includes user data
    db.update("users", alice_id, &[("name", Value::String("Alicia".into()))])
        .unwrap();

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
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("Expected Inserted"),
    };

    let post_id = match db
        .execute(&format!(
            "INSERT INTO posts (author, title) VALUES ('{}', 'Test Post')",
            alice_id
        ))
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("Expected Inserted"),
    };

    let query = db
        .incremental_query("SELECT * FROM posts JOIN users ON posts.author = users.id")
        .unwrap();

    let initial_rows = query.rows();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(initial_rows[0].values[1], Value::String("Test Post".into()));
    assert_eq!(initial_rows[0].values[2], Value::String("Alice".into()));

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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
    let row_counts = Arc::new(RwLock::new(Vec::<usize>::new()));
    let call_count_clone = call_count.clone();
    let row_counts_clone = row_counts.clone();

    let _sub_id = query.subscribe(move |rows| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
        row_counts_clone.write().unwrap().push(rows.len());
    });

    // Initial: 1 joined row
    let initial_rows = query.rows();
    assert_eq!(initial_rows.len(), 1);
    assert_eq!(initial_rows[0].values[1], Value::String("Test Post".into()));
    assert_eq!(initial_rows[0].values[2], Value::String("Alice".into()));

    // Delete the user - join should now return 0 rows (the post still exists but can't join)
    db.delete("users", alice_id).unwrap();

    // Should get a notification with 0 rows
    assert!(call_count.load(Ordering::SeqCst) > 0);
    let counts = row_counts.read().unwrap();
    assert_eq!(*counts.last().unwrap(), 0);
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("Expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
    let author_names: Vec<_> = rows.iter().map(|r| &r.values[2]).collect();
    assert_eq!(author_names.iter().filter(|n| **n == &Value::String("Alice".into())).count(), 2);
    assert_eq!(author_names.iter().filter(|n| **n == &Value::String("Bob".into())).count(), 1);

    // Verify all post titles are present
    let titles: Vec<_> = rows.iter().map(|r| &r.values[1]).collect();
    assert!(titles.contains(&&Value::String("Alice Post 1".into())));
    assert!(titles.contains(&&Value::String("Alice Post 2".into())));
    assert!(titles.contains(&&Value::String("Bob Post".into())));
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
        _ => panic!("Expected Inserted"),
    };
    let folder2_id = match db
        .execute("INSERT INTO folders (name) VALUES ('Personal')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
            let work_row = rows.iter().find(|r| r.values[0] == Value::String("Work".into()));
            assert!(work_row.is_some(), "Should have Work folder");
            let work_notes = work_row.unwrap().values[1].as_array().unwrap();
            assert_eq!(work_notes.len(), 2, "Work folder should have 2 notes");

            // Find the Personal folder row
            let personal_row = rows.iter().find(|r| r.values[0] == Value::String("Personal".into()));
            assert!(personal_row.is_some(), "Should have Personal folder");
            let personal_notes = personal_row.unwrap().values[1].as_array().unwrap();
            assert_eq!(personal_notes.len(), 1, "Personal folder should have 1 note");
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
            let notes_array = rows[0].values[1].as_array().unwrap();
            assert_eq!(notes_array.len(), 1);

            // Each item should be a Row value
            let note_row = notes_array[0].as_row().unwrap();
            // Note row should have 2 values: folder (ref), title
            assert_eq!(note_row.values.len(), 2);
            assert_eq!(note_row.values[1], Value::String("Meeting Notes".into()));
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
            let notes_array = rows[0].values[1].as_array().unwrap();
            assert!(notes_array.is_empty(), "Should return empty array for folder with no notes");
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
            let all_notes = rows[0].values[1].as_array().unwrap();
            assert_eq!(all_notes.len(), 2, "Should return all 2 notes");
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
        ExecuteResult::Inserted { row_id: id, .. } => id.to_string(),
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

    let id = db
        .insert("users", &["name"], vec![Value::String("Alice".into())])
        .unwrap();

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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
    assert_eq!(rows[0].values[0], Value::String("Alice".into()));
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
        ExecuteResult::Inserted { row_id: id, .. } => id,
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
    assert_eq!(rows[0].values.len(), 2, "Row should have 2 values (title, array)");
    assert_eq!(rows[0].values[0], Value::String("Test Issue".into()));

    match &rows[0].values[1] {
        Value::Array(arr) => {
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
    let project_id = db
        .insert(
            "Projects",
            &["name", "color", "description"],
            vec![
                Value::String("Test Project".into()),
                Value::String("#00ff00".into()),
                Value::String("A test project".into()),
            ],
        )
        .unwrap();

    // Insert issue referencing the project
    let issue_id = db
        .insert(
            "Issues",
            &["title", "description", "status", "priority", "project", "createdAt", "updatedAt"],
            vec![
                Value::String("Test Issue".into()),
                Value::String("Test description".into()),
                Value::String("open".into()),
                Value::String("high".into()),
                Value::Ref(project_id),
                Value::I64(1234567890),
                Value::I64(1234567890),
            ],
        )
        .unwrap();

    // Insert actual Label row
    let label_id = db.insert("Labels", &["name"], vec![Value::String("Bug".into())]).unwrap();

    // Insert actual User row
    let user_id = db.insert("Users", &["name"], vec![Value::String("Alice".into())]).unwrap();

    // Insert IssueLabel row
    db.insert(
        "IssueLabels",
        &["issue", "label"],
        vec![Value::Ref(issue_id), Value::Ref(label_id)],
    )
    .unwrap();

    // Insert IssueAssignee row
    db.insert(
        "IssueAssignees",
        &["issue", "user"],
        vec![Value::Ref(issue_id), Value::Ref(user_id)],
    )
    .unwrap();

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

    // Expected columns:
    // Issues: title, description (nullable), status, priority, project, createdAt, updatedAt = 7
    // Projects: name, color, description (nullable) = 3
    // IssueLabels array = 1
    // IssueAssignees array = 1
    // Total = 12

    eprintln!("Row values (len={}, expected 12):", rows[0].values.len());
    for (i, v) in rows[0].values.iter().enumerate() {
        eprintln!("  [{}]: {:?}", i, v);
    }

    assert_eq!(rows[0].values.len(), 12, "Should have 12 values (7 Issues + 3 Projects + 2 arrays)");

    // Find Projects.description - it should be at index 9 (after 7 Issues + 2 Projects columns)
    // Index: 0=title, 1=description*, 2=status, 3=priority, 4=project, 5=createdAt, 6=updatedAt
    //        7=name, 8=color, 9=description*
    //        10=IssueLabels[], 11=IssueAssignees[]

    let proj_desc = &rows[0].values[9];
    eprintln!("\nProjects.description (index 9): {:?}", proj_desc);

    assert!(
        matches!(proj_desc, Value::NullableSome(_)),
        "Projects.description should be NullableSome, got: {:?}",
        proj_desc
    );

    if let Value::NullableSome(inner) = proj_desc {
        assert_eq!(**inner, Value::String("A test project".into()));
    }

    // Now test the binary encoding
    use groove::sql::encode_single_row;
    let binary = encode_single_row(&rows[0]);
    eprintln!("\nBinary output ({} bytes):", binary.len());
    eprintln!("{:02x?}", &binary);

    // The binary should contain "A test project" for Projects.description
    let description_bytes = b"A test project";
    let found = binary.windows(description_bytes.len()).any(|w| w == description_bytes);
    assert!(found, "Binary should contain 'A test project' for Projects.description");
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
    let project_id = db
        .insert(
            "Projects",
            &["name", "color", "description"],
            vec![
                Value::String("Test Project".into()),
                Value::String("#00ff00".into()),
                Value::String("A test project".into()),
            ],
        )
        .unwrap();

    // Insert issue referencing the project
    db.insert(
        "Issues",
        &["title", "description", "status", "priority", "project", "createdAt", "updatedAt"],
        vec![
            Value::String("Test Issue".into()),
            Value::String("Test description".into()),
            Value::String("open".into()),
            Value::String("high".into()),
            Value::Ref(project_id),
            Value::I64(1234567890),
            Value::I64(1234567890),
        ],
    )
    .unwrap();

    // Use incremental_query via SQL - this is the path TypeScript uses
    let sql = "SELECT i.* FROM Issues i JOIN Projects ON i.project = Projects.id";
    let query = db.incremental_query(sql).expect("should create incremental query");
    let rows = query.rows();

    eprintln!("SQL query rows: {:?}", rows.len());
    assert_eq!(rows.len(), 1, "Should return 1 issue");

    eprintln!("Row values (len={}):", rows[0].values.len());
    for (i, v) in rows[0].values.iter().enumerate() {
        eprintln!("  [{}]: {:?}", i, v);
    }

    // In a JOIN query, the row should contain:
    // Issues: title, description, status, priority, project, createdAt, updatedAt = 7 columns
    // Projects: name, color, description = 3 columns
    // Total: 10 columns
    assert_eq!(rows[0].values.len(), 10, "Should have 10 values (7 Issues + 3 Projects)");

    // Projects.description should be at index 9
    let proj_desc = &rows[0].values[9];
    eprintln!("\nProjects.description (index 9): {:?}", proj_desc);

    assert!(
        matches!(proj_desc, Value::NullableSome(_)),
        "Projects.description should be NullableSome, got: {:?}",
        proj_desc
    );

    if let Value::NullableSome(inner) = proj_desc {
        assert_eq!(**inner, Value::String("A test project".into()));
    }
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
    let project_id = db
        .insert(
            "Projects",
            &["name", "color", "description"],
            vec![
                Value::String("Test Project".into()),
                Value::String("#00ff00".into()),
                Value::String("A test project".into()),
            ],
        )
        .unwrap();

    // Insert issue referencing the project
    let issue_id = db
        .insert(
            "Issues",
            &["title", "description", "status", "priority", "project", "createdAt", "updatedAt"],
            vec![
                Value::String("Test Issue".into()),
                Value::String("Test description".into()),
                Value::String("open".into()),
                Value::String("high".into()),
                Value::Ref(project_id),
                Value::I64(1234567890),
                Value::I64(1234567890),
            ],
        )
        .unwrap();

    // Insert label
    let label_id = db.insert("Labels", &["name"], vec![Value::String("Bug".into())]).unwrap();

    // Insert IssueLabel linking issue to label
    db.insert(
        "IssueLabels",
        &["issue", "label"],
        vec![Value::Ref(issue_id), Value::Ref(label_id)],
    )
    .unwrap();

    // Use incremental_query with JOIN + ARRAY subquery - this is what TypeScript generates
    let sql = "SELECT i.*, ARRAY(SELECT il.* FROM IssueLabels il WHERE il.issue = i.id) as labels FROM Issues i JOIN Projects ON i.project = Projects.id";
    let query = db.incremental_query(sql).expect("should create incremental query");
    let rows = query.rows();

    eprintln!("SQL JOIN+ARRAY query rows: {:?}", rows.len());
    assert_eq!(rows.len(), 1, "Should return 1 issue");

    eprintln!("Row values (len={}):", rows[0].values.len());
    for (i, v) in rows[0].values.iter().enumerate() {
        eprintln!("  [{}]: {:?}", i, v);
    }

    // Expected columns:
    // Issues: title, description, status, priority, project, createdAt, updatedAt = 7
    // Projects: name, color, description = 3
    // IssueLabels array = 1
    // Total: 11
    assert_eq!(rows[0].values.len(), 11, "Should have 11 values (7 Issues + 3 Projects + 1 array)");

    // Projects.description should be at index 9 (after 7 Issues + 2 Projects columns)
    let proj_desc = &rows[0].values[9];
    eprintln!("\nProjects.description (index 9): {:?}", proj_desc);

    assert!(
        matches!(proj_desc, Value::NullableSome(_)),
        "Projects.description should be NullableSome, got: {:?}",
        proj_desc
    );

    if let Value::NullableSome(inner) = proj_desc {
        assert_eq!(**inner, Value::String("A test project".into()));
    }

    // Test binary encoding includes the description
    use groove::sql::encode_single_row;
    let binary = encode_single_row(&rows[0]);
    eprintln!("\nBinary output ({} bytes):", binary.len());

    let description_bytes = b"A test project";
    let found = binary.windows(description_bytes.len()).any(|w| w == description_bytes);
    assert!(found, "Binary should contain 'A test project' for Projects.description");
}

/// Test that SQL ARRAY subqueries with nested JOINs work correctly.
/// This tests the case where:
///   ARRAY(SELECT il.*, Labels as label FROM IssueLabels il JOIN Labels ON il.label = Labels.id WHERE il.issue = i.id)
/// The ARRAY elements should include the resolved Labels row, not just the FK.
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
    let label_id = db
        .insert("Labels", &["name", "color"], vec![
            Value::String("Bug".into()),
            Value::String("#ff0000".into()),
        ])
        .unwrap();

    // Insert issue
    let issue_id = db
        .insert("Issues", &["title"], vec![Value::String("Test Issue".into())])
        .unwrap();

    // Insert IssueLabel linking issue to label
    db.insert(
        "IssueLabels",
        &["issue", "label"],
        vec![Value::Ref(issue_id), Value::Ref(label_id)],
    )
    .unwrap();

    // Query with ARRAY subquery that has a nested JOIN
    // This is what TypeScript generates for: { IssueLabels: { label: true } }
    let sql = "SELECT i.id, i.title, ARRAY(SELECT il.id, il.issue, Labels as label FROM IssueLabels il JOIN Labels ON il.label = Labels.id WHERE il.issue = i.id) as IssueLabels FROM Issues i";
    let query = db.incremental_query(sql).expect("should create incremental query");
    let rows = query.rows();

    eprintln!("ARRAY with nested JOIN query rows: {:?}", rows.len());
    assert_eq!(rows.len(), 1, "Should return 1 issue");

    eprintln!("Row values (len={}):", rows[0].values.len());
    for (i, v) in rows[0].values.iter().enumerate() {
        eprintln!("  [{}]: {:?}", i, v);
    }

    // Expected columns:
    // Issues: title = 1
    // IssueLabels array = 1
    // Total: 2
    assert_eq!(rows[0].values.len(), 2, "Should have 2 values (1 Issue column + 1 array)");

    // The array should contain IssueLabel rows with resolved label
    let array = &rows[0].values[1];
    eprintln!("\nIssueLabels array: {:?}", array);

    if let Value::Array(arr) = array {
        assert_eq!(arr.len(), 1, "Should have 1 IssueLabel");
        if let Value::Row(issue_label_row) = &arr[0] {
            // IssueLabel row should have: id (implicit), issue (Ref), label (resolved Row)
            // With nested JOIN, the values should be:
            // [0] = issue (Ref to Issues)
            // [1] = label (resolved Labels Row with id, name, color)
            eprintln!("IssueLabel row values (len={}):", issue_label_row.values.len());
            for (i, v) in issue_label_row.values.iter().enumerate() {
                eprintln!("  [{}]: {:?}", i, v);
            }

            // The label should be a nested Row, not a Ref
            // Expected: [Ref(issue_id), Row(Labels row)]
            assert_eq!(
                issue_label_row.values.len(),
                2,
                "IssueLabel row should have 2 values (issue + label), got {}",
                issue_label_row.values.len()
            );

            // Check that values[0] is a Ref to the Issue
            assert!(
                matches!(&issue_label_row.values[0], Value::Ref(_)),
                "values[0] should be a Ref to Issue, got: {:?}",
                issue_label_row.values[0]
            );

            // Check that values[1] is a nested Row (resolved Labels)
            if let Value::Row(label_row) = &issue_label_row.values[1] {
                eprintln!("Label row values (len={}):", label_row.values.len());
                for (i, v) in label_row.values.iter().enumerate() {
                    eprintln!("  [{}]: {:?}", i, v);
                }

                // Labels row should have 2 values: name, color
                assert_eq!(
                    label_row.values.len(),
                    2,
                    "Label row should have 2 values (name + color), got {}",
                    label_row.values.len()
                );

                // Check label name
                assert!(
                    matches!(&label_row.values[0], Value::String(s) if s == "Bug"),
                    "Labels.name should be 'Bug', got: {:?}",
                    label_row.values[0]
                );

                // Check label color
                assert!(
                    matches!(&label_row.values[1], Value::String(s) if s == "#ff0000"),
                    "Labels.color should be '#ff0000', got: {:?}",
                    label_row.values[1]
                );
            } else {
                panic!("values[1] should be a Row (resolved Labels), got: {:?}", issue_label_row.values[1]);
            }
        } else {
            panic!("Array element should be a Row, got: {:?}", arr[0]);
        }
    } else {
        panic!("values[1] should be an Array, got: {:?}", array);
    }
}
