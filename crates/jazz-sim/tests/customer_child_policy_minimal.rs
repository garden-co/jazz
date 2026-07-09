use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent, Transport,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::protocol::SyncMessage;
use jazz::query::{Query, claim, col, eq, lit};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::wire::TransportError;

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
    left_sent: Rc<Cell<usize>>,
    right_sent: Rc<Cell<usize>>,
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
        left_sent,
        right_sent,
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(GROUP, [ColumnSchema::new("label", ColumnType::String)]),
        TableSchema::new(
            GROUP_ENTRY,
            [
                ColumnSchema::new("member_id", ColumnType::Uuid),
                ColumnSchema::new("target_id", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("member_id", GROUP)
        .with_reference("target_id", GROUP),
        TableSchema::new(
            PARENT,
            [
                ColumnSchema::new("label", ColumnType::String),
                ColumnSchema::new("team", ColumnType::Uuid),
            ],
        )
        .with_reference("team", GROUP)
        .with_read_policy(Policy::shape(
            Query::from(PARENT).reachable_via_with_access_filters(
                PARENT_ACCESS,
                "resource",
                "team",
                claim("sub"),
                [eq(col("administrator"), lit(false))],
                GROUP_ENTRY,
                "member_id",
                "target_id",
                [eq(col("administrator"), lit(false))],
            ),
        )),
        TableSchema::new(
            PARENT_ACCESS,
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("resource", PARENT)
        .with_reference("team", GROUP),
        TableSchema::new(
            CHILD,
            [
                ColumnSchema::new("parent_id", ColumnType::Uuid),
                ColumnSchema::new("label", ColumnType::String),
            ],
        )
        .with_reference("parent_id", PARENT)
        .with_read_policy(Policy::shape(
            Query::from(CHILD).reachable_via_with_access_filters(
                CHILD_ACCESS,
                "child",
                "team",
                claim("sub"),
                [eq(col("administrator"), lit(false))],
                GROUP_ENTRY,
                "member_id",
                "target_id",
                [eq(col("administrator"), lit(false))],
            ),
        )),
        TableSchema::new(
            CHILD_ACCESS,
            [
                ColumnSchema::new("child", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
                ColumnSchema::new("administrator", ColumnType::Bool),
            ],
        )
        .with_reference("child", CHILD)
        .with_reference("team", GROUP),
    ])
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

fn db_config(schema: JazzSchema, node_uuid: NodeUuid, author: AuthorId) -> DbConfig<MemoryStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    DbConfig {
        schema,
        storage: MemoryStorage::new(&refs),
        identity: DbIdentity {
            node: node_uuid,
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(node_seed(node_uuid)))),
        large_value_checkpoint_op_interval: 1024,
    }
}

fn open_db(schema: JazzSchema, node_uuid: NodeUuid, author: AuthorId) -> Db<MemoryStorage> {
    jazz::db::block_on(Db::open(db_config(schema, node_uuid, author))).expect("open db")
}

fn open_history_complete_db(
    schema: JazzSchema,
    node_uuid: NodeUuid,
    author: AuthorId,
) -> Db<MemoryStorage> {
    jazz::db::block_on(Db::open_history_complete(db_config(
        schema, node_uuid, author,
    )))
    .expect("open db")
}

fn insert(db: &Db<MemoryStorage>, table: &str, row: RowUuid, cells: BTreeMap<String, Value>) {
    db.seed_settled_mergeable_for_bootstrap(table, row, AuthorId::SYSTEM, cells)
        .expect("seed settled row");
}

fn count(db: &Db<MemoryStorage>, table: &str, author: AuthorId) -> usize {
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
                rows.insert(row.row_uuid());
            }
        }
        SubscriptionEvent::Closed => {}
    }
}

fn tick_all(core: &Db<MemoryStorage>, relay: &Db<MemoryStorage>, client: &Db<MemoryStorage>) {
    core.tick().expect("core tick");
    relay.tick().expect("relay tick");
    client.tick().expect("client tick");
}

#[test]
fn child_policy_reaches_client_through_relay() {
    let schema = schema();
    let member = AuthorId(row(0x10).0);
    let core = open_history_complete_db(schema.clone(), node(0x01), AuthorId::SYSTEM);
    let relay = open_db(schema.clone(), node(0x02), AuthorId::SYSTEM);
    let client = open_db(schema.clone(), node(0x03), member);

    let member_group = row(0x10);
    let reachable_group = row(0x11);
    let parent = row(0x20);
    let child = row(0x30);

    insert(
        &core,
        GROUP,
        member_group,
        BTreeMap::from([("label".to_owned(), Value::String("member".to_owned()))]),
    );
    insert(
        &core,
        GROUP,
        reachable_group,
        BTreeMap::from([("label".to_owned(), Value::String("reachable".to_owned()))]),
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
    eprintln!("MIN_CHILD core member one-shot child rows={core_member_count}");
    assert_eq!(core_member_count, 1, "core member one-shot must see child");

    let relay_core = duplex();
    let client_relay = duplex();
    let relay_core_left_sent = Rc::clone(&relay_core.left_sent);
    let relay_core_right_sent = Rc::clone(&relay_core.right_sent);
    let client_relay_left_sent = Rc::clone(&client_relay.left_sent);
    let client_relay_right_sent = Rc::clone(&client_relay.right_sent);
    let _relay_upstream = relay.connect_upstream(relay_core.left);
    let _core_sub = core.accept_subscriber(relay_core.right, AuthorId::SYSTEM);
    let _client_upstream = client.connect_upstream(client_relay.left);
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

    for tick in 0..200 {
        tick_all(&core, &relay, &client);
        for (_, stream, rows) in &mut subscriptions {
            while let Some(event) = stream.try_next_event() {
                apply_event(rows, event);
            }
        }
        let seen = subscriptions
            .iter()
            .find(|(table, _, _)| *table == CHILD)
            .map(|(_, _, rows)| rows)
            .unwrap();

        if tick == 10 || tick == 50 || tick == 199 {
            let relay_member_count = count(&relay, CHILD, member);
            let relay_system_child = count(&relay, CHILD, AuthorId::SYSTEM);
            let relay_system_child_access = count(&relay, CHILD_ACCESS, AuthorId::SYSTEM);
            let relay_system_group_entry = count(&relay, GROUP_ENTRY, AuthorId::SYSTEM);
            let relay_system_parent_access = count(&relay, PARENT_ACCESS, AuthorId::SYSTEM);
            eprintln!(
                "MIN_CHILD tick={tick} relay->core={} core->relay={} client->relay={} relay->client={} relay member child={relay_member_count} relay system child={relay_system_child} child_access={relay_system_child_access} group_entry={relay_system_group_entry} parent_access={relay_system_parent_access} client_subscription_rows={}",
                relay_core_left_sent.get(),
                relay_core_right_sent.get(),
                client_relay_left_sent.get(),
                client_relay_right_sent.get(),
                seen.len()
            );
        }
        if seen.contains(&child) {
            let relay_member_count = count(&relay, CHILD, member);
            eprintln!(
                "MIN_CHILD reached tick={tick} relay member one-shot child rows={relay_member_count} client_subscription_rows={}",
                seen.len()
            );
            return;
        }
    }

    let relay_member_count = count(&relay, CHILD, member);
    let relay_system_child = count(&relay, CHILD, AuthorId::SYSTEM);
    let relay_system_child_access = count(&relay, CHILD_ACCESS, AuthorId::SYSTEM);
    let relay_system_group_entry = count(&relay, GROUP_ENTRY, AuthorId::SYSTEM);
    let relay_system_parent_access = count(&relay, PARENT_ACCESS, AuthorId::SYSTEM);
    let seen = subscriptions
        .iter()
        .find(|(table, _, _)| *table == CHILD)
        .map(|(_, _, rows)| rows)
        .unwrap();
    eprintln!(
        "MIN_CHILD final relay member child={relay_member_count} relay system child={relay_system_child} child_access={relay_system_child_access} group_entry={relay_system_group_entry} parent_access={relay_system_parent_access} client_subscription_rows={}",
        seen.len()
    );
    assert_eq!(
        relay_member_count, 1,
        "relay member one-shot must see child"
    );
    assert_eq!(seen.len(), 1, "client subscription must receive child");
}
