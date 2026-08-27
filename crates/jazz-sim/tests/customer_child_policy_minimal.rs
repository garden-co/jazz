use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent, Transport,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::SyncMessage;
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    ColumnType as PublicColumnType, SchemaBuilder, TablePolicies, TableSchema as PublicTableSchema,
    Value as PublicValue,
};
use jazz::wire::TransportError;
use jazz_sim::public_schema_fixture::{compile_public_schema, seeded_recursive_access_policy};

const GROUP: &str = "group";
const GROUP_ENTRY: &str = "group_entry";
const PARENT: &str = "parent";
const PARENT_ACCESS: &str = "parent_access_edges";
const CHILD: &str = "child";
const CHILD_ACCESS: &str = "child_access_edges";

#[derive(Clone)]
struct QueueTransport {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    sent: Rc<Cell<usize>>,
}

impl Transport for QueueTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.sent.set(self.sent.get() + 1);
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

struct Duplex {
    left: Box<dyn Transport>,
    right: Box<dyn Transport>,
}

fn duplex() -> Duplex {
    let left_queue = Rc::new(RefCell::new(VecDeque::new()));
    let right_queue = Rc::new(RefCell::new(VecDeque::new()));
    let left_sent = Rc::new(Cell::new(0));
    let right_sent = Rc::new(Cell::new(0));
    Duplex {
        left: Box::new(QueueTransport {
            outbound: Rc::clone(&left_queue),
            inbound: Rc::clone(&right_queue),
            sent: Rc::clone(&left_sent),
        }),
        right: Box::new(QueueTransport {
            outbound: right_queue,
            inbound: left_queue,
            sent: Rc::clone(&right_sent),
        }),
    }
}

fn schema() -> JazzSchema {
    let parent_policy = seeded_recursive_access_policy(
        PARENT_ACCESS,
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        GROUP,
        GROUP_ENTRY,
        "member_id",
        "target_id",
        &[("administrator", PublicValue::Boolean(false))],
        GROUP,
        "identity",
        &["user"],
        "id",
    );
    let child_policy = seeded_recursive_access_policy(
        CHILD_ACCESS,
        "child",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        GROUP,
        GROUP_ENTRY,
        "member_id",
        "target_id",
        &[("administrator", PublicValue::Boolean(false))],
        GROUP,
        "identity",
        &["user"],
        "id",
    );
    compile_public_schema(
        SchemaBuilder::new()
            .table(
                PublicTableSchema::builder(GROUP)
                    .column("label", PublicColumnType::Text)
                    .column("identity", PublicColumnType::Text),
            )
            .table(
                PublicTableSchema::builder(GROUP_ENTRY)
                    .fk_column("member_id", GROUP)
                    .fk_column("target_id", GROUP)
                    .column("administrator", PublicColumnType::Boolean),
            )
            .table(
                PublicTableSchema::builder(PARENT)
                    .column("label", PublicColumnType::Text)
                    .fk_column("team", GROUP)
                    .policies(TablePolicies::new().with_select(parent_policy)),
            )
            .table(
                PublicTableSchema::builder(PARENT_ACCESS)
                    .fk_column("resource", PARENT)
                    .fk_column("team", GROUP)
                    .column("administrator", PublicColumnType::Boolean),
            )
            .table(
                PublicTableSchema::builder(CHILD)
                    .fk_column("parent_id", PARENT)
                    .column("label", PublicColumnType::Text)
                    .policies(TablePolicies::new().with_select(child_policy)),
            )
            .table(
                PublicTableSchema::builder(CHILD_ACCESS)
                    .fk_column("child", CHILD)
                    .fk_column("team", GROUP)
                    .column("administrator", PublicColumnType::Boolean),
            )
            .build(),
    )
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn node_seed(node_uuid: NodeUuid) -> u64 {
    let bytes = node_uuid.to_bytes();
    u64::from_be_bytes(bytes[..8].try_into().unwrap())
}

fn db_config(
    schema: JazzSchema,
    node_uuid: NodeUuid,
    author: AuthorSubject,
) -> DbConfig<MemoryStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    DbConfig {
        schema,
        storage: MemoryStorage::new(&refs).expect("valid memory storage families"),
        identity: DbIdentity {
            node: node_uuid,
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(node_seed(node_uuid)))),
    }
}

fn open_db(schema: JazzSchema, node_uuid: NodeUuid, author: AuthorSubject) -> Db<MemoryStorage> {
    let db = jazz::db::block_on(Db::open(db_config(schema, node_uuid, author))).expect("open db");
    install_db_claims(&db, author);
    db
}

fn open_history_complete_db(
    schema: JazzSchema,
    node_uuid: NodeUuid,
    author: AuthorSubject,
) -> Db<MemoryStorage> {
    jazz::db::block_on(Db::open_history_complete(db_config(
        schema, node_uuid, author,
    )))
    .map(|db| {
        install_db_claims(&db, author);
        db
    })
    .expect("open db")
}

fn session_claims() -> BTreeMap<String, Value> {
    BTreeMap::new()
}

fn install_db_claims(db: &Db<MemoryStorage>, author: AuthorSubject) {
    if author != AuthorSubject::SYSTEM {
        db.set_identity_claims(author, session_claims());
    }
}

fn insert(db: &Db<MemoryStorage>, table: &str, row: RowUuid, cells: BTreeMap<String, Value>) {
    db.seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, cells)
        .expect("seed settled row");
}

fn count(db: &Db<MemoryStorage>, table: &str, author: AuthorSubject) -> usize {
    let prepared = db.prepare_query(&Query::from(table)).expect("prepare");
    let rows = jazz::db::block_on(db.all_for_identity(&prepared, ReadOpts::default(), author))
        .expect("one-shot");
    rows.len()
}

fn apply_event(rows: &mut BTreeSet<RowUuid>, event: SubscriptionEvent) {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } => {
            if reset {
                rows.clear();
            }
            for row in removed {
                rows.remove(&row.row_uuid);
            }
            for row in added.into_iter().chain(updated) {
                rows.insert(row.row.row_uuid());
            }
        }
        SubscriptionEvent::Rejected { reason } => {
            panic!("subscription rejected unexpectedly: {reason:?}");
        }
        SubscriptionEvent::Closed => {}
    }
}

fn tick_all(core: &Db<MemoryStorage>, relay: &Db<MemoryStorage>, client: &Db<MemoryStorage>) {
    jazz::db::block_on(core.tick()).expect("core tick");
    jazz::db::block_on(relay.tick()).expect("relay tick");
    jazz::db::block_on(client.tick()).expect("client tick");
}

#[test]
fn child_policy_reaches_client_through_relay() {
    let schema = schema();
    let member = AuthorSubject::for_test_uuid(row(0x10).0);
    let core = open_history_complete_db(schema.clone(), node(0x01), AuthorSubject::SYSTEM);
    let relay = open_db(schema.clone(), node(0x02), AuthorSubject::SYSTEM);
    let client = open_db(schema.clone(), node(0x03), member);
    install_db_claims(&core, member);
    install_db_claims(&relay, member);

    let member_group = row(0x10);
    let reachable_group = row(0x11);
    let parent = row(0x20);
    let child = row(0x30);

    insert(
        &core,
        GROUP,
        member_group,
        BTreeMap::from([
            ("label".to_owned(), Value::String("member".to_owned())),
            (
                "identity".to_owned(),
                Value::String(member.canonical().to_owned()),
            ),
        ]),
    );
    insert(
        &core,
        GROUP,
        reachable_group,
        BTreeMap::from([
            ("label".to_owned(), Value::String("reachable".to_owned())),
            ("identity".to_owned(), Value::String("unrelated".to_owned())),
        ]),
    );
    insert(
        &core,
        GROUP_ENTRY,
        row(0x40),
        BTreeMap::from([
            ("member_id".to_owned(), Value::Uuid(member_group.0)),
            ("target_id".to_owned(), Value::Uuid(reachable_group.0)),
            ("administrator".to_owned(), Value::Bool(false)),
        ]),
    );
    insert(
        &core,
        PARENT,
        parent,
        BTreeMap::from([
            (
                "label".to_owned(),
                Value::String("visible-parent".to_owned()),
            ),
            ("team".to_owned(), Value::Uuid(reachable_group.0)),
        ]),
    );
    insert(
        &core,
        PARENT_ACCESS,
        row(0x21),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(parent.0)),
            ("team".to_owned(), Value::Uuid(reachable_group.0)),
            ("administrator".to_owned(), Value::Bool(false)),
        ]),
    );
    insert(
        &core,
        CHILD,
        child,
        BTreeMap::from([
            ("parent_id".to_owned(), Value::Uuid(parent.0)),
            ("label".to_owned(), Value::String("child".to_owned())),
        ]),
    );
    insert(
        &core,
        CHILD_ACCESS,
        row(0x50),
        BTreeMap::from([
            ("child".to_owned(), Value::Uuid(child.0)),
            ("team".to_owned(), Value::Uuid(reachable_group.0)),
            ("administrator".to_owned(), Value::Bool(false)),
        ]),
    );

    let core_member_count = count(&core, CHILD, member);
    assert_eq!(core_member_count, 1, "core member one-shot must see child");

    let relay_core = duplex();
    let client_relay = duplex();
    let _relay_upstream = jazz::db::block_on(relay.connect_upstream(relay_core.left));
    let _core_sub = core.accept_subscriber(relay_core.right, AuthorSubject::SYSTEM);
    let _client_upstream = jazz::db::block_on(client.connect_upstream(client_relay.left));
    let _relay_sub = relay.accept_subscriber(client_relay.right, member);

    let mut subscriptions = Vec::new();
    for table in [
        GROUP,
        GROUP_ENTRY,
        PARENT,
        PARENT_ACCESS,
        CHILD_ACCESS,
        CHILD,
    ] {
        let query = client
            .prepare_query(&Query::from(table))
            .unwrap_or_else(|error| panic!("prepare {table}: {error}"));
        let stream = jazz::db::block_on(client.subscribe(&query, ReadOpts::default()))
            .unwrap_or_else(|error| panic!("subscribe {table}: {error}"));
        subscriptions.push((table, stream, BTreeSet::<RowUuid>::new()));
    }

    for _ in 0..200 {
        tick_all(&core, &relay, &client);
        for (_, stream, rows) in &mut subscriptions {
            while let Some(event) = stream.try_next_event() {
                apply_event(rows, event);
            }
        }
        if subscriptions
            .iter()
            .find(|(table, _, _)| *table == CHILD)
            .map(|(_, _, rows)| rows)
            .unwrap()
            .contains(&child)
        {
            return;
        }
    }

    let relay_member_count = count(&relay, CHILD, member);
    let seen = subscriptions
        .iter()
        .find(|(table, _, _)| *table == CHILD)
        .map(|(_, _, rows)| rows)
        .unwrap();
    assert_eq!(
        relay_member_count, 1,
        "relay member one-shot must see child"
    );
    assert_eq!(seen.len(), 1, "client subscription must receive child");
}
