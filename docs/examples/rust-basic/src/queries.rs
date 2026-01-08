//! Query examples including incremental queries

use groove::sql::Database;

//#region basic-query
/// Execute a basic query
pub fn basic_query(db: &Database) {
    // Simple SELECT all rows from a table
    let rows = db.select_all("tasks").unwrap();
    println!("Found {} tasks", rows.len());

    // For filtered queries, use incremental_query
    let query = db
        .incremental_query("SELECT * FROM tasks WHERE completed = false")
        .unwrap();
    let active_tasks = query.rows();
    println!("Found {} active tasks", active_tasks.len());
}
//#endregion

//#region incremental-query
/// Create an incremental query that updates automatically
pub fn incremental_query(db: &Database) {
    // Create an incremental query
    let query = db
        .incremental_query("SELECT * FROM tasks WHERE completed = false")
        .unwrap();

    // Get current results
    let rows = query.rows();
    println!("Active tasks: {}", rows.len());

    // When data changes, query.rows() returns updated results automatically
    // The query graph propagates deltas efficiently
}
//#endregion

//#region query-with-join
/// Query with JOIN
pub fn query_with_join(db: &Database) {
    let query = db
        .incremental_query(
            "SELECT tasks.title, users.name
             FROM tasks
             JOIN users ON tasks.assignee = users.id
             WHERE tasks.completed = false",
        )
        .unwrap();

    for row in query.rows() {
        println!("Task: {:?}", row);
    }
}
//#endregion

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> Database {
        let db = Database::in_memory();
        db.execute("CREATE TABLE tasks (title STRING NOT NULL, completed BOOL NOT NULL)")
            .unwrap();
        db.execute("INSERT INTO tasks (title, completed) VALUES ('Task 1', false)")
            .unwrap();
        db.execute("INSERT INTO tasks (title, completed) VALUES ('Task 2', true)")
            .unwrap();
        db
    }

    #[test]
    fn test_basic_query() {
        let db = setup_db();
        basic_query(&db);
    }

    #[test]
    fn test_incremental_query() {
        let db = setup_db();
        incremental_query(&db);
    }
}
