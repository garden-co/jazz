use groove::db::{Database, GraphBuilder, PredicateExpr};
use groove::records::Value;
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("year", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("albums_by_year", ["year"]))]);

    let storage = MemoryStorage::new(&["albums", "indices"]);
    let mut database = Database::new(schema, storage)?;

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::String("Blue Train".to_owned()),
            Value::U64(1957),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(2),
            Value::String("Kind of Blue".to_owned()),
            Value::U64(1959),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(3),
            Value::String("A Love Supreme".to_owned()),
            Value::U64(1965),
        ],
    );
    database.commit_batch(batch)?;

    let rows = database.query_graph(
        GraphBuilder::table("albums")
            .filter(PredicateExpr::eq("year", Value::U64(1959)))
            .project(["id", "title", "year"]),
    )?;
    println!("query rows:");
    for (values, weight) in rows.to_values()? {
        println!("  {values:?} weight={weight}");
    }

    let index_rows = database.index_scan_range(
        "albums",
        "albums_by_year",
        &[Value::U64(1957)],
        &[Value::U64(1960)],
    )?;
    println!("index scan rows:");
    for row in index_rows {
        println!("  {:?}", row.to_values()?);
    }

    Ok(())
}
