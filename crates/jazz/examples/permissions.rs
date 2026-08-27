use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ErrorCode, Node, PermissionAdvice, RowCells, RowIdSource,
    SeededRowIdSource, Transport, WriteState,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::protocol::SyncMessage;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{CmpOp, PolicyValue};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate, RejectionReason};
use jazz::wire::TransportError;

fn author(byte: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([byte; 16])
}

fn todo_schema() -> JazzSchema {
    let owner = PolicyExpr::Cmp {
        column: "owner".to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec!["user".to_owned()]),
    };
    let policies = TablePolicies::new()
        .with_select(owner.clone())
        .with_insert(owner.clone())
        .with_update(Some(owner.clone()), owner.clone())
        .with_delete(owner);
    let source = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .column("owner", ColumnType::Text)
                .policies(policies),
        )
        .build();
    JazzSchema::new(&source).expect("permissions public schema compiles")
}

fn todo_cells(title: &str, done: bool, owner: AuthorSubject) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
        (
            "owner".to_owned(),
            Value::String(owner.canonical().to_owned()),
        ),
    ])
}

fn open_db(
    node_byte: u8,
    author: AuthorSubject,
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
    }))?)
}

struct CoreDb {
    server: Node<MemoryStorage>,
    author: AuthorSubject,
    next_now_ms: u64,
    id_source: SeededRowIdSource,
}

fn open_core(
    node_byte: u8,
    author: AuthorSubject,
    schema: JazzSchema,
    storage: MemoryStorage,
) -> Result<CoreDb, Box<dyn std::error::Error>> {
    let node = block_on(NodeState::new_history_complete(
        NodeUuid::from_bytes([node_byte; 16]),
        schema,
        storage,
    ))?;
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
        made_by: AuthorSubject,
        table: &str,
        cells: RowCells,
    ) -> Result<RowUuid, Error> {
        let row = self.id_source.next_row_id();
        let node = self.server.node();
        let tx_id = block_on(async {
            let mut node = node.lock().await;
            let published = node
                .commit_mergeable(
                    MergeableCommit::new(table, row, self.next_now_ms())
                        .made_by(made_by)
                        .permission_subject(self.author)
                        .cells(cells),
                )
                .await?;
            node.persist_and_settle_transaction(published).await
        })?;
        block_on(async {
            let mut node = node.lock().await;
            let outcome = node.finalize_local_mergeable_commit(tx_id).await?;
            node.persist_and_settle_outcome(outcome).await
        })?;
        Ok(row)
    }

    fn read(&self, table: &str) -> Result<Vec<jazz::node::CurrentRow>, Error> {
        let node = self.server.node();
        block_on(async {
            node.lock()
                .await
                .current_rows(table, DurabilityTier::Local)
                .await
        })
        .map_err(Into::into)
    }

    fn accept_subscriber(&self, transport: Box<dyn Transport>, identity: AuthorSubject) {
        let _subscriber = self.server.accept_subscriber(transport, identity);
    }

    fn tick(&self) -> Result<(), Error> {
        block_on(self.server.tick()).map(|_| ())
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
    identity: AuthorSubject,
) -> Result<(), Error> {
    let (client_transport, server_transport) = duplex();
    let _upstream = block_on(client.connect_upstream(client_transport));
    core.accept_subscriber(server_transport, identity);
    block_on(client.tick())?;
    core.tick()?;
    block_on(client.tick())?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = todo_schema();
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

    assert_eq!(
        owner_db.can_insert("todos", todo_cells("owned", false, owner))?,
        PermissionAdvice::Unknown,
    );
    assert_eq!(
        other_db.can_insert("todos", todo_cells("owned", false, owner))?,
        PermissionAdvice::Unknown,
    );
    let todos = owner_db.prepare_query(&owner_db.table("todos"))?;
    assert_eq!(owner_db.read(&todos)?.len(), 0);

    let row = RowUuid::from_bytes([0x33; 16]);
    block_on(owner_db.insert(
        "todos",
        todo_cells("private", false, owner),
        jazz::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))?;

    assert_eq!(owner_db.can_read("todos", row)?, PermissionAdvice::Unknown);
    assert_eq!(
        owner_db.can_update("todos", row)?,
        PermissionAdvice::Unknown
    );
    assert_eq!(
        owner_db.can_delete("todos", row)?,
        PermissionAdvice::Unknown
    );
    assert_eq!(other_db.can_read("todos", row)?, PermissionAdvice::Unknown);
    assert_eq!(
        other_db.can_update("todos", row)?,
        PermissionAdvice::Unknown
    );
    assert_eq!(
        other_db.can_delete("todos", row)?,
        PermissionAdvice::Unknown
    );
    assert_eq!(owner_db.read(&todos)?.len(), 1);
    println!("client permission previews remain unknown until a serving authority evaluates them");

    let backend = author(0xbe);
    let attributed_user = author(0xc3);
    let mut core = open_core(0x5e, backend, schema.clone(), storage.clone())?;
    let attributed = core.insert_attributed(
        attributed_user,
        "todos",
        todo_cells("written by core for user", false, attributed_user),
    )?;

    let client_err = match block_on(owner_db.insert(
        "todos",
        todo_cells("forged", false, other),
        jazz::db::InsertOptions {
            identity: jazz::db::WriteIdentity::Attribution(other),
            ..Default::default()
        },
    )) {
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
    let forbidden = block_on(other_db.insert(
        "todos",
        todo_cells("forbidden at authority", false, owner),
        jazz::db::InsertOptions {
            row_id: Some(forbidden_row),
            ..Default::default()
        },
    ))?;
    assert_eq!(
        block_on(forbidden.write_state())?,
        WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
        }
    );

    sync_client_to_core(&other_db, &core, other)?;
    assert_eq!(
        block_on(forbidden.write_state())?,
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
