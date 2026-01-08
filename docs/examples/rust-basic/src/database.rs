//! Database setup and schema examples

use groove::sql::Database;

//#region create-database
/// Create an in-memory database and initialize schema
pub fn create_database() -> Database {
    // Create an in-memory database
    let db = Database::in_memory();

    // Initialize schema with SQL DDL
    db.execute(
        "CREATE TABLE tasks (
            title STRING NOT NULL,
            description STRING,
            completed BOOL NOT NULL
        )",
    )
    .unwrap();

    db
}
//#endregion

//#region execute-statements
/// Execute SQL statements
pub fn execute_statements(db: &Database) {
    // Insert a row
    db.execute("INSERT INTO tasks (title, completed) VALUES ('Learn Groove', false)")
        .unwrap();

    // Update a row (requires knowing the ID)
    // db.update("tasks", row_id, &[("completed", Value::Bool(true))]).unwrap();

    // Delete a row
    // db.delete("tasks", row_id).unwrap();
}
//#endregion

//#region schema-definition
/// Define a complete schema
pub fn define_schema(db: &Database) {
    db.execute(
        "
        CREATE TABLE users (
            name STRING NOT NULL,
            email STRING NOT NULL
        )
    ",
    )
    .unwrap();

    db.execute(
        "
        CREATE TABLE projects (
            name STRING NOT NULL,
            owner REFERENCES users NOT NULL
        )
    ",
    )
    .unwrap();

    db.execute(
        "
        CREATE TABLE tasks (
            title STRING NOT NULL,
            description STRING,
            completed BOOL NOT NULL,
            project REFERENCES projects NOT NULL
        )
    ",
    )
    .unwrap();
}
//#endregion

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_database() {
        let db = create_database();
        execute_statements(&db);
    }

    #[test]
    fn test_define_schema() {
        let db = Database::in_memory();
        define_schema(&db);
    }
}
