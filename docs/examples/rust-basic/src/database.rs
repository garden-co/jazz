//! Database creation and configuration

use groove::sql::Database;

/// Create a new database with the tasks schema
pub fn create_tasks_database() -> Result<Database, groove::sql::Error> {
    let mut db = Database::new();

    // Define the schema
    db.execute(
        "CREATE TABLE Tasks (
            title STRING NOT NULL,
            description STRING,
            completed BOOLEAN NOT NULL,
            priority STRING NOT NULL,
            createdAt I64 NOT NULL
        )",
        vec![],
    )?;

    // Create indexes for common queries
    db.execute(
        "CREATE INDEX idx_tasks_completed ON Tasks(completed)",
        vec![],
    )?;

    db.execute(
        "CREATE INDEX idx_tasks_priority ON Tasks(priority)",
        vec![],
    )?;

    Ok(db)
}

/// Initialize database with sample data
pub fn seed_database(db: &mut Database) -> Result<(), groove::sql::Error> {
    let tasks = vec![
        ("Learn Groove", "Read the documentation", "high"),
        ("Build a demo", "Create a sample application", "medium"),
        ("Write tests", "Add comprehensive tests", "medium"),
        ("Deploy", "Set up production deployment", "low"),
    ];

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    for (i, (title, desc, priority)) in tasks.into_iter().enumerate() {
        db.execute(
            "INSERT INTO Tasks (id, title, description, completed, priority, createdAt)
             VALUES (?, ?, ?, ?, ?, ?)",
            vec![
                format!("task{}", i + 1).into(),
                title.into(),
                desc.into(),
                false.into(),
                priority.into(),
                now.into(),
            ],
        )?;
    }

    Ok(())
}
