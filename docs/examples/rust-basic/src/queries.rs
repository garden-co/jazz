//! Query examples

use groove::sql::{Database, Value};

/// Basic query operations
pub fn query_examples(db: &Database) -> Result<(), groove::sql::Error> {
    // Select all tasks
    let all_tasks = db.query("SELECT * FROM Tasks", vec![])?;
    println!("All tasks: {} rows", all_tasks.len());

    // Select with WHERE clause
    let active = db.query(
        "SELECT * FROM Tasks WHERE completed = ?",
        vec![false.into()],
    )?;
    println!("Active tasks: {} rows", active.len());

    // Select with ORDER BY
    let sorted = db.query(
        "SELECT * FROM Tasks ORDER BY createdAt DESC",
        vec![],
    )?;
    println!("Sorted by date: {} rows", sorted.len());

    // Aggregate query
    let counts = db.query(
        "SELECT priority, COUNT(*) as count FROM Tasks GROUP BY priority",
        vec![],
    )?;
    println!("Tasks by priority: {:?}", counts);

    // JOIN query (assuming Projects table exists)
    // let with_project = db.query(
    //     "SELECT t.title, p.name as project_name
    //      FROM Tasks t
    //      JOIN Projects p ON t.project = p.id",
    //     vec![],
    // )?;

    Ok(())
}

/// Parameterized query for safety
pub fn find_tasks_by_priority(
    db: &Database,
    priority: &str,
) -> Result<Vec<Vec<Value>>, groove::sql::Error> {
    db.query(
        "SELECT * FROM Tasks WHERE priority = ? AND completed = ?",
        vec![priority.into(), false.into()],
    )
}
