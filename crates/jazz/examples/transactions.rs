use std::cell::Cell;
use std::collections::BTreeMap;

use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, Error, Node, ReadOpts, RowCells, SeededRowIdSource};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, OpenTxId};
use jazz::protocol::SyncMessage;
use jazz::query::Query;
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::{DurabilityTier, Fate, RejectionReason, TxId};

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

fn title_patch(title: &str) -> RowCells {
    BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))])
}

fn open_db() -> Result<Db<MemoryStorage>, Box<dyn std::error::Error>> {
    let schema = JazzSchema::new([todo_table()]);
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = MemoryStorage::new(&column_family_refs);

    Ok(block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorId::from_bytes([0xa1; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x1111))),
        large_value_checkpoint_op_interval: 1024,
    }))?)
}

struct CoreDb {
    server: Node<MemoryStorage>,
    schema: JazzSchema,
    author: AuthorId,
    next_now_ms: Cell<u64>,
}

fn open_core() -> Result<CoreDb, Box<dyn std::error::Error>> {
    let schema = JazzSchema::new([todo_table()]);
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = MemoryStorage::new(&column_family_refs);
    let node =
        NodeState::new_history_complete(NodeUuid::from_bytes([0x22; 16]), schema.clone(), storage)?;

    Ok(CoreDb {
        server: Node::new(node),
        schema,
        author: AuthorId::from_bytes([0xa1; 16]),
        next_now_ms: Cell::new(1),
    })
}

impl CoreDb {
    fn next_now_ms(&self) -> u64 {
        let next = self.next_now_ms.get();
        self.next_now_ms.set(next + 1);
        next
    }

    fn table(&self, table: impl Into<String>) -> Query {
        Query::from(table)
    }

    fn one(&self, query: &Query) -> Result<Option<jazz::node::CurrentRow>, Error> {
        let shape = query.validate(&self.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.server
            .node()
            .borrow_mut()
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .map(|rows| rows.into_iter().next())
            .map_err(Into::into)
    }

    fn insert_with_id(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<TxId, Error> {
        let node = self.server.node();
        let tx_id = node.borrow_mut().commit_mergeable(
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(self.author)
                .cells(cells),
        )?;
        node.borrow_mut().finalize_local_mergeable_commit(tx_id)?;
        Ok(tx_id)
    }

    fn exclusive_tx(&self) -> Result<CoreExclusiveTx<'_>, Error> {
        let tx_id = self.server.node().borrow_mut().open_exclusive()?;
        Ok(CoreExclusiveTx {
            core: self,
            tx_id,
            has_reads: Cell::new(false),
        })
    }
}

struct CoreExclusiveTx<'a> {
    core: &'a CoreDb,
    tx_id: OpenTxId,
    has_reads: Cell<bool>,
}

impl CoreExclusiveTx<'_> {
    fn read(&self, table: &str, row: RowUuid) -> Result<Option<RowCells>, Error> {
        self.has_reads.set(true);
        self.core
            .server
            .node()
            .borrow_mut()
            .tx_read(self.tx_id, table, row)
            .map_err(Into::into)
    }

    fn insert_with_id(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        self.core
            .server
            .node()
            .borrow_mut()
            .tx_write(self.tx_id, table, row, cells, None)
            .map_err(Into::into)
    }

    fn update(&self, table: &str, row: RowUuid, patch: RowCells) -> Result<(), Error> {
        let mut cells = self.read(table, row)?.unwrap_or_default();
        cells.extend(patch);
        self.insert_with_id(table, row, cells)
    }

    fn commit(self) -> Result<TxId, RejectionReason> {
        let node = self.core.server.node();
        if self.has_reads.get()
            && node
                .borrow()
                .open_exclusive_snapshot_moved(self.tx_id)
                .expect("exclusive snapshot check succeeds")
        {
            node.borrow_mut()
                .abandon_tx(self.tx_id)
                .expect("abandoning conflicting transaction succeeds");
            return Err(write_rejected(RejectionReason::ExclusiveConflict));
        }

        let (tx_id, unit) = node
            .borrow_mut()
            .commit_exclusive(self.tx_id, self.core.author, self.core.next_now_ms())
            .expect("core exclusive commit succeeds");
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            panic!("commit_exclusive must yield a CommitUnit");
        };
        let fate = node
            .borrow_mut()
            .finalize_local_exclusive_commit(tx, versions)
            .expect("core exclusive finalize succeeds");
        if let Fate::Rejected(reason) = fate {
            return Err(write_rejected(reason));
        }
        Ok(tx_id)
    }
}

fn write_rejected(reason: RejectionReason) -> RejectionReason {
    reason
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_db()?;
    let mut mergeable = db.mergeable_tx();
    let first = mergeable.insert("todos", todo_cells("write examples", false))?;
    let second = mergeable.insert("todos", todo_cells("run examples", true))?;
    let mergeable_tx = mergeable.commit()?;

    let todos = db.prepare_query(&db.table("todos"))?;
    let rows = block_on(db.all(&todos, ReadOpts::default()))?;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| row.row_uuid() == first));
    assert!(rows.iter().any(|row| row.row_uuid() == second));
    println!("mergeable_tx committed 2 writes as {mergeable_tx:?}");

    let core = open_core()?;
    let row = RowUuid::from_bytes([0x22; 16]);
    core.insert_with_id("todos", row, todo_cells("base title", false))?;

    let first_tx = core.exclusive_tx()?;
    let second_tx = core.exclusive_tx()?;
    let read = second_tx.read("todos", row)?.expect("row is visible");
    assert_eq!(
        read.get("title"),
        Some(&Value::String("base title".to_owned()))
    );

    first_tx.update("todos", row, title_patch("first writer"))?;
    let accepted = first_tx.commit().expect("first writer is accepted");

    second_tx.update("todos", row, title_patch("stale writer"))?;
    let rejected = second_tx.commit().expect_err("stale read must conflict");
    assert_eq!(rejected, RejectionReason::ExclusiveConflict);

    let current = core
        .one(&core.table("todos"))?
        .expect("row remains visible");
    assert_eq!(
        current.cell(&todo_table(), "title"),
        Some(Value::String("first writer".to_owned()))
    );
    println!("exclusive_tx committed {accepted:?} and rejected a stale writer");

    Ok(())
}
