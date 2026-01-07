//! Basic Rust example using the Groove core library

use groove::sql::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new in-memory database
    let mut db = Database::new();

    // Define the schema
    db.execute(
        "CREATE TABLE Tasks (
            title STRING NOT NULL,
            completed BOOLEAN NOT NULL,
            priority STRING NOT NULL
        )",
        vec![],
    )?;

    // Insert some tasks
    db.execute(
        "INSERT INTO Tasks (id, title, completed, priority) VALUES (?, ?, ?, ?)",
        vec!["task1".into(), "Learn Groove".into(), false.into(), "high".into()],
    )?;

    db.execute(
        "INSERT INTO Tasks (id, title, completed, priority) VALUES (?, ?, ?, ?)",
        vec!["task2".into(), "Build an app".into(), false.into(), "medium".into()],
    )?;

    // Query all tasks
    let tasks = db.query("SELECT * FROM Tasks", vec![])?;
    println!("All tasks: {:?}", tasks);

    // Query with filter
    let high_priority = db.query(
        "SELECT * FROM Tasks WHERE priority = ?",
        vec!["high".into()],
    )?;
    println!("High priority tasks: {:?}", high_priority);

    // Update a task
    db.execute(
        "UPDATE Tasks SET completed = ? WHERE id = ?",
        vec![true.into(), "task1".into()],
    )?;

    Ok(())
}
