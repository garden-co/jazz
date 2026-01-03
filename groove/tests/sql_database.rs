//! Integration tests for sql::Database

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use groove::sql::{
    ColumnDef, ColumnType, Database, DatabaseError, ExecuteResult, TableSchema, Value,
};

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
    assert!(id > 0);

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
    assert_eq!(row.values[1], Value::I64(30));
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
    assert_eq!(row.values[1], Value::Null);
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
    assert_eq!(row.values[1], Value::I64(31));
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
        ExecuteResult::Inserted(id) => {
            let row = db.get("users", id).unwrap().unwrap();
            assert_eq!(row.values[0], Value::String("Alice".into()));
            assert_eq!(row.values[1], Value::I64(30));
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
        ExecuteResult::Inserted(id) => id,
        _ => panic!("expected Inserted"),
    };

    let result = db
        .execute(&format!(
            "UPDATE users SET age = 31 WHERE id = x'{:032x}'",
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
    assert_eq!(row.values[1], Value::I64(31));
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
            Value::Ref(0x12345), // fake user ID
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

    // Find all posts by Bob
    let bob_posts = db.find_referencing("posts", "author", bob_id).unwrap();
    assert_eq!(bob_posts.len(), 1);
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

    let result = db.find_referencing("users", "name", 123);
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
    assert_eq!(post.values[0], Value::Null);

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

// ========== Reactive Query Tests ==========

#[test]
fn reactive_query_returns_current_rows() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL, active BOOL NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Alice', true)")
        .unwrap();
    db.execute("INSERT INTO users (name, active) VALUES ('Bob', false)")
        .unwrap();

    let query = db.reactive_query("SELECT * FROM users").unwrap();

    // Should immediately have the current rows
    let state = query.get();
    assert!(state.is_loaded());
    let rows = state.rows().unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn reactive_query_with_where_clause() {
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
        .reactive_query("SELECT * FROM users WHERE active = true")
        .unwrap();

    let rows = query.rows().unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn reactive_query_once_helper() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();

    // Use once() for a one-shot query
    let rows = db.reactive_query("SELECT * FROM users").unwrap().once();
    assert_eq!(rows.len(), 2);
}

#[test]
fn reactive_query_nonexistent_table_fails() {
    let db = Database::in_memory();

    // Query for non-existent table should fail
    let result = db.reactive_query("SELECT * FROM users");
    assert!(result.is_err());
}

#[test]
fn reactive_query_only_accepts_select() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();

    // SELECT should work
    let result = db.reactive_query("SELECT * FROM users");
    assert!(result.is_ok());

    // INSERT should fail
    let result = db.reactive_query("INSERT INTO users (name) VALUES ('Alice')");
    assert!(result.is_err());
}

#[test]
fn reactive_query_auto_updates_on_insert() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap();

    let query = db.reactive_query("SELECT * FROM users").unwrap();

    // Initially has 1 row
    assert_eq!(query.rows().unwrap().len(), 1);

    // Insert another row - query auto-updates synchronously
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();

    // Query immediately has 2 rows (auto-updated on insert)
    assert_eq!(query.rows().unwrap().len(), 2);
}

#[test]
fn reactive_query_auto_updates_on_update() {
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
        .reactive_query("SELECT * FROM users WHERE name = 'Alice'")
        .unwrap();
    assert_eq!(query.rows().unwrap().len(), 1);

    // Update the row to have a different name
    db.update("users", id, &[("name", Value::String("Alicia".into()))])
        .unwrap();

    // Query auto-updates - should now return 0 rows (name no longer matches)
    assert_eq!(query.rows().unwrap().len(), 0);
}

#[test]
fn reactive_query_auto_updates_on_delete() {
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

    let query = db.reactive_query("SELECT * FROM users").unwrap();
    assert_eq!(query.rows().unwrap().len(), 1);

    // Delete the row
    db.delete("users", id).unwrap();

    // Query auto-updates - should now return 0 rows
    assert_eq!(query.rows().unwrap().len(), 0);
}

// ========== Callback-based Reactive Query Tests ==========

#[test]
fn reactive_query_initial_evaluation() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();

    let query = db.reactive_query("SELECT * FROM users").unwrap();

    // Should have initial rows
    let rows = query.rows().unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn reactive_query_subscribe_callback() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap();

    let query = db.reactive_query("SELECT * FROM users").unwrap();

    let call_count = Arc::new(AtomicUsize::new(0));
    let row_counts = Arc::new(RwLock::new(Vec::<usize>::new()));
    let call_count_clone = call_count.clone();
    let row_counts_clone = row_counts.clone();

    // Subscribe - callback should be called immediately with current state
    let _id = query.subscribe(Box::new(move |rows| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
        row_counts_clone.write().unwrap().push(rows.len());
    }));

    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(*row_counts.read().unwrap(), vec![1]);
}

#[test]
fn reactive_query_callback_on_insert() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap();

    let query = db.reactive_query("SELECT * FROM users").unwrap();

    let call_count = Arc::new(AtomicUsize::new(0));
    let row_counts = Arc::new(RwLock::new(Vec::<usize>::new()));
    let call_count_clone = call_count.clone();
    let row_counts_clone = row_counts.clone();

    // Subscribe - callback called with initial state (1 row)
    let _id = query.subscribe(Box::new(move |rows| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
        row_counts_clone.write().unwrap().push(rows.len());
    }));

    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(*row_counts.read().unwrap(), vec![1]);

    // Insert a new row - callback should be called synchronously
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();

    // Callback should have been called again with 2 rows
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
    assert_eq!(*row_counts.read().unwrap(), vec![1, 2]);

    // Insert another row
    db.execute("INSERT INTO users (name) VALUES ('Charlie')")
        .unwrap();

    assert_eq!(call_count.load(Ordering::SeqCst), 3);
    assert_eq!(*row_counts.read().unwrap(), vec![1, 2, 3]);
}

#[test]
fn reactive_query_callback_on_delete() {
    let db = Database::in_memory();
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    let id1 = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted(id) => id,
        _ => panic!("Expected Inserted"),
    };
    db.execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap();

    let query = db.reactive_query("SELECT * FROM users").unwrap();

    let call_count = Arc::new(AtomicUsize::new(0));
    let row_counts = Arc::new(RwLock::new(Vec::<usize>::new()));
    let call_count_clone = call_count.clone();
    let row_counts_clone = row_counts.clone();

    let _sub_id = query.subscribe(Box::new(move |rows| {
        call_count_clone.fetch_add(1, Ordering::SeqCst);
        row_counts_clone.write().unwrap().push(rows.len());
    }));

    // Initial callback with 2 rows
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    assert_eq!(*row_counts.read().unwrap(), vec![2]);

    // Delete a row
    db.delete("users", id1).unwrap();

    // Callback called with 1 row
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
    assert_eq!(*row_counts.read().unwrap(), vec![2, 1]);
}
