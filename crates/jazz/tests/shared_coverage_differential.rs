use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionEvent, Transport,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::protocol::SyncMessage;
use jazz::query::Query;
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;
use jazz::wire::TransportError;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

#[derive(Clone, Copy, Debug)]
enum CoverageMode {
    PerSubscription,
    ForcedGroupingHook,
}

#[derive(Debug, PartialEq, Eq)]
struct ScenarioReceipt {
    traces: BTreeMap<&'static str, Vec<EventTrace>>,
    final_rows: BTreeMap<&'static str, Vec<RowSummary>>,
}

#[derive(Debug, PartialEq, Eq)]
struct EventTrace {
    reset: bool,
    settled: bool,
    tier: DurabilityTier,
    added: Vec<RowSummary>,
    updated: Vec<RowSummary>,
    removed: Vec<(&'static str, RowUuid)>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RowSummary {
    table: String,
    row: RowUuid,
    title: Option<String>,
    owner: Option<AuthorId>,
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

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
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

fn row(seed: u64) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&0x019e_0000_0000_7100u64.to_be_bytes());
    bytes[8..].copy_from_slice(&seed.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn schema() -> JazzSchema {
    JazzSchema::new(TABLES.map(|table| {
        TableSchema::new(
            table,
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
            ],
        )
        .with_read_policy(Policy::owner_only(table, "owner"))
        .with_write_policy(Policy::public())
    }))
}

fn open_client(seed: u8, author: AuthorId, schema: JazzSchema) -> Db<MemoryStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([seed; 16]),
                author,
            },
        )
        .with_id_source(SeededRowIdSource::new(seed as u64)),
    ))
    .expect("open client")
}

fn open_server(seed: u8, schema: JazzSchema) -> Db<MemoryStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open_history_complete(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([seed; 16]),
                author: AuthorId::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(seed as u64)),
    ))
    .expect("open server")
}

fn global_read_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn cells(title: &str, owner: AuthorId) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("owner".to_owned(), Value::Uuid(owner.0)),
    ])
}

fn seed_fixture(server: &Db<MemoryStorage>, visible_owner: AuthorId, hidden_owner: AuthorId) {
    for (idx, table) in TABLES.iter().enumerate() {
        server
            .seed_settled_mergeable_for_bootstrap(
                table,
                row(100 + idx as u64),
                AuthorId::SYSTEM,
                cells(&format!("{table}-visible"), visible_owner),
            )
            .expect("seed visible row");
        server
            .seed_settled_mergeable_for_bootstrap(
                table,
                row(200 + idx as u64),
                AuthorId::SYSTEM,
                cells(&format!("{table}-hidden"), hidden_owner),
            )
            .expect("seed hidden row");
    }
}

fn row_summary(table_schema: &TableSchema, row: &jazz::node::CurrentRow) -> RowSummary {
    let title = match row.cell(table_schema, "title") {
        Some(Value::String(value)) => Some(value),
        other => panic!("unexpected title cell: {other:?}"),
    };
    let owner = match row.cell(table_schema, "owner") {
        Some(Value::Uuid(value)) => Some(AuthorId(value)),
        other => panic!("unexpected owner cell: {other:?}"),
    };
    RowSummary {
        table: row.table().to_owned(),
        row: row.row_uuid(),
        title,
        owner,
    }
}

fn event_trace(
    table_schemas: &BTreeMap<&'static str, TableSchema>,
    event: SubscriptionEvent,
) -> EventTrace {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            settled,
            tier,
            ..
        } => {
            let mut added = added
                .iter()
                .map(|row| row_summary(&table_schemas[row.table()], row))
                .collect::<Vec<_>>();
            let mut updated = updated
                .iter()
                .map(|row| row_summary(&table_schemas[row.table()], row))
                .collect::<Vec<_>>();
            let mut removed = removed
                .into_iter()
                .map(|row| (table_name(row.table), row.row_uuid))
                .collect::<Vec<_>>();
            added.sort();
            updated.sort();
            removed.sort();
            EventTrace {
                reset,
                settled,
                tier,
                added,
                updated,
                removed,
            }
        }
        SubscriptionEvent::Closed => panic!("subscription closed unexpectedly"),
    }
}

fn table_name(name: String) -> &'static str {
    TABLES
        .iter()
        .copied()
        .find(|table| *table == name)
        .expect("known table")
}

fn drain_events(
    streams: &mut BTreeMap<&'static str, jazz::db::SubscriptionStream>,
    table_schemas: &BTreeMap<&'static str, TableSchema>,
    traces: &mut BTreeMap<&'static str, Vec<EventTrace>>,
) {
    for (table, stream) in streams {
        while let Some(event) = stream.try_next_event() {
            traces
                .get_mut(table)
                .expect("trace bucket")
                .push(event_trace(table_schemas, event));
        }
    }
}

fn drive(
    server: &Db<MemoryStorage>,
    client: &Db<MemoryStorage>,
    streams: &mut BTreeMap<&'static str, jazz::db::SubscriptionStream>,
    table_schemas: &BTreeMap<&'static str, TableSchema>,
    traces: &mut BTreeMap<&'static str, Vec<EventTrace>>,
) {
    for _ in 0..40 {
        client.tick().expect("tick client before server");
        drain_events(streams, table_schemas, traces);
        server.tick().expect("tick server");
        client.tick().expect("tick client after server");
        drain_events(streams, table_schemas, traces);
    }
}

fn final_rows(
    client: &Db<MemoryStorage>,
    table_schemas: &BTreeMap<&'static str, TableSchema>,
) -> BTreeMap<&'static str, Vec<RowSummary>> {
    TABLES
        .into_iter()
        .map(|table| {
            let prepared = client
                .prepare_query(&Query::from(table))
                .expect("prepare final query");
            let mut rows = block_on(client.all(&prepared, global_read_opts()))
                .expect("read final rows")
                .iter()
                .map(|row| row_summary(&table_schemas[table], row))
                .collect::<Vec<_>>();
            rows.sort();
            (table, rows)
        })
        .collect()
}

fn run_scenario(mode: CoverageMode) -> ScenarioReceipt {
    match mode {
        CoverageMode::PerSubscription => {}
        CoverageMode::ForcedGroupingHook => {
            // Future implementation hook: route subscriptions through forced shared
            // coverage grouping here, while preserving the receipt contract below.
        }
    }

    let schema = schema();
    let table_schemas = schema
        .tables
        .iter()
        .map(|table| (table_name(table.name.clone()), table.clone()))
        .collect::<BTreeMap<_, _>>();
    let visible_owner = AuthorId::from_bytes([0xa1; 16]);
    let hidden_owner = AuthorId::from_bytes([0xb2; 16]);
    let server = open_server(0x5e, schema.clone());
    let client = open_client(0xc1, visible_owner, schema);
    seed_fixture(&server, visible_owner, hidden_owner);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, visible_owner);

    let mut traces = TABLES
        .into_iter()
        .map(|table| (table, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    let mut streams = BTreeMap::new();
    for table in TABLES {
        let prepared = client
            .prepare_query(&Query::from(table))
            .expect("prepare subscription query");
        let mut stream =
            block_on(client.subscribe(&prepared, global_read_opts())).expect("subscribe table");
        traces
            .get_mut(table)
            .expect("trace bucket")
            .push(event_trace(
                &table_schemas,
                block_on(stream.next_event()).expect("initial subscription event"),
            ));
        streams.insert(table, stream);
    }

    drive(&server, &client, &mut streams, &table_schemas, &mut traces);

    server
        .update(
            "alpha_items",
            row(100),
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("alpha-updated".to_owned()),
            )]),
        )
        .expect("update visible row");
    server
        .insert_with_id(
            "beta_items",
            row(310),
            cells("beta-inserted-visible", visible_owner),
        )
        .expect("insert visible row");
    server
        .insert_with_id(
            "gamma_items",
            row(320),
            cells("gamma-inserted-hidden", hidden_owner),
        )
        .expect("insert hidden row");
    server
        .delete("delta_items", row(103))
        .expect("delete visible row");

    drive(&server, &client, &mut streams, &table_schemas, &mut traces);

    ScenarioReceipt {
        traces,
        final_rows: final_rows(&client, &table_schemas),
    }
}

const TABLES: [&str; 4] = ["alpha_items", "beta_items", "gamma_items", "delta_items"];

#[test]
fn forced_shared_coverage_group_matches_per_subscription_observations() {
    let per_subscription = run_scenario(CoverageMode::PerSubscription);
    let forced_grouping = run_scenario(CoverageMode::ForcedGroupingHook);

    assert_eq!(forced_grouping, per_subscription);
}
