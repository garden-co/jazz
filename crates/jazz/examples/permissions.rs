use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ErrorCode, Node, RowCells, RowIdSource, SeededRowIdSource,
    Transport, WriteState,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::protocol::SyncMessage;
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::{DurabilityTier, Fate, RejectionReason};
use jazz::wire::TransportError;

fn author(byte: u8) -> AuthorId {
    AuthorId::from_bytes([byte; 16])
}

fn todo_table() -> TableSchema {
    TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))
    .with_write_policy(Policy::owner_only("todos", "owner"))
}

fn todo_cells(title: &str, done: bool, owner: AuthorId) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
        ("owner".to_owned(), Value::Uuid(owner.0)),
    ])
}

fn open_db(
    node_byte: u8,
    author: AuthorId,
    schema: JazzSchema,
    storage: MemoryStorage,
) -> Result<Db<MemoryStorage>, Box<dyn std::error::Error>> {
    Ok(block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([node_byte; 16]),
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(u64::from(node_byte)))),
        large_value_checkpoint_op_interval: 1024,
    }))?)
}

struct CoreDb {
    server: Node<MemoryStorage>,
    author: AuthorId,
    next_now_ms: u64,
    id_source: SeededRowIdSource,
}

fn open_core(
    node_byte: u8,
    author: AuthorId,
    schema: JazzSchema,
    storage: MemoryStorage,
) -> Result<CoreDb, Box<dyn std::error::Error>> {
    let node =
        NodeState::new_history_complete(NodeUuid::from_bytes([node_byte; 16]), schema, storage)?;
    Ok(CoreDb {
        server: Node::new(node),
        author,
        next_now_ms: 1,
        id_source: SeededRowIdSource::new(u64::from(node_byte)),
    })
}

impl CoreDb {
    fn next_now_ms(&mut self) -> u64 {
        let next = self.next_now_ms;
        self.next_now_ms += 1;
        next
    }

    fn insert_attributed(
        &mut self,
        made_by: AuthorId,
        table: &str,
        cells: RowCells,
    ) -> Result<RowUuid, Error> {
        let row = self.id_source.next_row_id();
        let node = self.server.node();
        let tx_id = node.borrow_mut().commit_mergeable(
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(made_by)
                .permission_subject(self.author)
                .cells(cells),
        )?;
        node.borrow_mut().finalize_local_mergeable_commit(tx_id)?;
        Ok(row)
    }

    fn read(&self, table: &str) -> Result<Vec<jazz::node::CurrentRow>, Error> {
        self.server
            .node()
            .borrow_mut()
            .current_rows(table, DurabilityTier::Local)
            .map_err(Into::into)
    }

    fn accept_subscriber(&self, transport: Box<dyn Transport>, identity: AuthorId) {
        let _subscriber = self.server.accept_subscriber(transport, identity);
    }

    fn tick(&self) -> Result<(), Error> {
        self.server.tick().map(|_| ())
    }
}

struct DuplexTransport {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for DuplexTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
        }),
        Box::new(DuplexTransport {
            outbound: right,
            inbound: left,
        }),
    )
}

fn sync_client_to_core(
    client: &Db<MemoryStorage>,
    core: &CoreDb,
    identity: AuthorId,
) -> Result<(), Error> {
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    core.accept_subscriber(server_transport, identity);
    client.tick()?;
    core.tick()?;
    client.tick()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = JazzSchema::new([todo_table()]);
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = MemoryStorage::new(&column_family_refs);
    let owner = author(0xa1);
    let other = author(0xb2);

    let owner_db = open_db(0xa1, owner, schema.clone(), storage.clone())?;
    let other_db = open_db(0xb2, other, schema.clone(), storage.clone())?;

    assert!(owner_db.can_insert("todos", todo_cells("owned", false, owner))?);
    assert!(!other_db.can_insert("todos", todo_cells("owned", false, owner))?);
    let todos = owner_db.prepare_query(&owner_db.table("todos"))?;
    assert_eq!(owner_db.read(&todos)?.len(), 0);

    let row = RowUuid::from_bytes([0x33; 16]);
    owner_db.insert_with_id("todos", row, todo_cells("private", false, owner))?;

    assert!(owner_db.can_read("todos", row)?);
    assert!(owner_db.can_update("todos", row)?);
    assert!(owner_db.can_delete("todos", row)?);
    assert!(!other_db.can_read("todos", row)?);
    assert!(!other_db.can_update("todos", row)?);
    assert!(!other_db.can_delete("todos", row)?);
    assert_eq!(owner_db.read(&todos)?.len(), 1);
    println!("permission previews allow the owner and reject another user");

    let backend = author(0xbe);
    let attributed_user = author(0xc3);
    let mut core = open_core(0x5e, backend, schema.clone(), storage.clone())?;
    let attributed = core.insert_attributed(
        attributed_user,
        "todos",
        todo_cells("written by core for user", false, attributed_user),
    )?;

    let client_err =
        match owner_db.insert_attributed(other, "todos", todo_cells("forged", false, other)) {
            Ok(_) => panic!("clients cannot attribute writes to another user"),
            Err(err) => err,
        };
    assert_eq!(client_err.code, ErrorCode::WriteRejected);
    assert!(client_err.message.contains("attribution"));
    println!(
        "insert_attributed wrote {:?} from Core and rejected client forgery",
        attributed
    );

    let forbidden_row = RowUuid::from_bytes([0x44; 16]);
    let forbidden = other_db.insert_with_id(
        "todos",
        forbidden_row,
        todo_cells("forbidden at authority", false, owner),
    )?;
    assert_eq!(
        forbidden.write_state()?,
        WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
        }
    );

    sync_client_to_core(&other_db, &core, other)?;
    assert_eq!(
        forbidden.write_state()?,
        WriteState {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            durability: DurabilityTier::Local,
        }
    );
    assert!(
        !core
            .read("todos")?
            .iter()
            .any(|candidate| candidate.row_uuid() == forbidden_row)
    );
    println!("client write uploaded to Core and was rejected by write policy");

    Ok(())
}
