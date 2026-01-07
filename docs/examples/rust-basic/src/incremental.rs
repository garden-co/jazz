//! Incremental query examples

use groove::sql::Database;
use groove::listener::Listener;

/// Set up an incremental query subscription
pub fn subscribe_to_tasks(db: &mut Database) {
    // Create a listener for change notifications
    let listener = Listener::new();

    // Register a callback for query results
    listener.subscribe("active_tasks", |rows| {
        println!("Active tasks updated: {} rows", rows.len());
        for row in rows {
            println!("  - {:?}", row);
        }
    });

    // Register the query with the database
    // When underlying data changes, the callback fires automatically
    db.register_query(
        "active_tasks",
        "SELECT * FROM Tasks WHERE completed = false ORDER BY priority",
        vec![],
        listener,
    );
}

/// Example: Dashboard with multiple live queries
pub fn setup_dashboard_queries(db: &mut Database) {
    let listener = Listener::new();

    // Task count by status
    listener.subscribe("task_counts", |rows| {
        println!("Task counts: {:?}", rows);
    });

    db.register_query(
        "task_counts",
        "SELECT
            SUM(CASE WHEN completed THEN 1 ELSE 0 END) as done,
            SUM(CASE WHEN NOT completed THEN 1 ELSE 0 END) as pending
         FROM Tasks",
        vec![],
        listener.clone(),
    );

    // High priority tasks
    listener.subscribe("urgent", |rows| {
        if !rows.is_empty() {
            println!("Urgent tasks: {}", rows.len());
        }
    });

    db.register_query(
        "urgent",
        "SELECT * FROM Tasks
         WHERE priority = 'high' AND completed = false",
        vec![],
        listener,
    );
}
