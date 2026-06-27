use std::collections::BTreeMap;

use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, RowCells, SeededRowIdSource};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::node::CurrentRow;
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

fn todo_table() -> TableSchema {
    TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())
}

fn todo_cells(title: &str, done: bool) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
    ])
}

fn print_todos(label: &str, table: &TableSchema, rows: &[CurrentRow]) {
    println!("{label}");
    for row in rows {
        let title = match row.cell(table, "title") {
            Some(Value::String(title)) => title,
            _ => "<missing title>".to_owned(),
        };
        let done = matches!(row.cell(table, "done"), Some(Value::Bool(true)));
        println!("- {:?}: title={title:?}, done={done}", row.row_uuid());
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let table = todo_table();
    let schema = JazzSchema::new([table.clone()]);
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = MemoryStorage::new(&column_family_refs);

    let db = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorId::from_bytes([0xa1; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x1111))),
        large_value_checkpoint_op_interval: 1024,
    }))?;

    let insert_milk = db.insert("todos", todo_cells("buy milk", false))?;
    let buy_milk = insert_milk.row_uuid();
    block_on(insert_milk.wait(DurabilityTier::Local))?;

    let insert_docs = db.insert("todos", todo_cells("write docs", false))?;
    let write_docs = insert_docs.row_uuid();
    block_on(insert_docs.wait(DurabilityTier::Local))?;

    let query = db.prepare_query(&db.table("todos").select(["title", "done"]))?;
    let rows = block_on(db.all(&query, ReadOpts::default()))?;
    print_todos("After insert:", &table, &rows);

    let update_milk = db.update(
        "todos",
        buy_milk,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    )?;
    block_on(update_milk.wait(DurabilityTier::Local))?;

    let delete_docs = db.delete("todos", write_docs)?;
    block_on(delete_docs.wait(DurabilityTier::Local))?;

    let rows = block_on(db.all(&query, ReadOpts::default()))?;
    print_todos("After update/delete:", &table, &rows);

    Ok(())
}
