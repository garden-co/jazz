//! Native receipt for completed semantic frames held behind a busy owner poll.
//! Public Db/query APIs; memory storage and SYSTEM admission isolate scheduling.
//! The host alternates foreground progress at owner suspension boundaries.
//! It is not a browser/IndexedDB/authorization or network timing claim.

use futures::task::noop_waker;
use jazz::{
    block_on,
    db::{
        ClientRelayScope, Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriptionEvent,
        SubscriptionStream, Transport,
    },
    groove::{records::Value, storage::MemoryStorage},
    ids::{AuthorSubject, NodeUuid, RowUuid},
    protocol::SyncMessage,
    query::{Query, col, eq, lit},
    schema::{JazzSchema, TableSchema},
    tools::{ColumnType, SchemaBuilder, TableSchemaBuilder},
    wire::TransportError,
};
use serde_json::json;
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    future::Future,
    rc::Rc,
    task::{Context, Poll},
    time::Instant,
};

mod support;

struct Envelope {
    message: SyncMessage,
    queued: Instant,
}
#[derive(Default)]
struct Handoff {
    frame_holds_ms: Vec<f64>,
    first_sent_ms: Option<f64>,
    first_received_ms: Option<f64>,
}
struct Carrier {
    incoming: Rc<RefCell<VecDeque<Envelope>>>,
    outgoing: Rc<RefCell<VecDeque<Envelope>>>,
    timing: Rc<RefCell<Handoff>>,
    began: Instant,
    serving: bool,
}
impl Transport for Carrier {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if self.serving && matches!(message, SyncMessage::ViewUpdate(_)) {
            self.timing
                .borrow_mut()
                .first_sent_ms
                .get_or_insert_with(|| self.began.elapsed().as_secs_f64() * 1000.);
        }
        self.outgoing.borrow_mut().push_back(Envelope {
            message,
            queued: Instant::now(),
        });
        Ok(())
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        let frame = self.incoming.borrow_mut().pop_front()?;
        if !self.serving && matches!(frame.message, SyncMessage::ViewUpdate(_)) {
            let mut timing = self.timing.borrow_mut();
            timing
                .first_received_ms
                .get_or_insert_with(|| self.began.elapsed().as_secs_f64() * 1000.);
            timing
                .frame_holds_ms
                .push(frame.queued.elapsed().as_secs_f64() * 1000.);
        }
        Some(frame.message)
    }
}

fn row(index: usize) -> RowUuid {
    RowUuid::from_bytes((index as u128 + 1).to_le_bytes())
}
fn open(schema: &JazzSchema, tag: u8, core: bool) -> Db<MemoryStorage> {
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let config = DbConfig::new(
        schema.clone(),
        MemoryStorage::new(&refs).unwrap(),
        DbIdentity {
            node: NodeUuid::from_bytes([tag; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    if core {
        block_on(Db::open_history_complete(config)).unwrap()
    } else {
        block_on(Db::open(config)).unwrap()
    }
}

type RowCells = BTreeMap<String, Value>;

fn drain(
    streams: &mut [SubscriptionStream],
    results: &mut [BTreeMap<RowUuid, RowCells>],
    table: &TableSchema,
) {
    for (stream, rows) in streams.iter_mut().zip(results) {
        while let Some(event) = stream.try_next_event() {
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
                    for r in removed {
                        rows.remove(&r.row_uuid);
                    }
                    for r in added.into_iter().chain(updated) {
                        let cells = table
                            .columns
                            .iter()
                            .map(|column| {
                                (
                                    column.name().to_owned(),
                                    r.cell(table, column.name()).expect("present cell"),
                                )
                            })
                            .collect();
                        rows.insert(r.row_uuid(), cells);
                    }
                }
                SubscriptionEvent::Rejected { reason } => panic!("query rejected: {reason:?}"),
                SubscriptionEvent::Closed => panic!("query closed"),
            }
        }
    }
}

fn run(rows: usize, queries: usize, local_relay: bool) {
    let setup = Instant::now();
    let mut table = TableSchemaBuilder::new("items").column("title", ColumnType::Text);
    for c in 0..17 {
        table = table.column(&format!("field_{c}"), ColumnType::Text);
    }
    let schema = JazzSchema::new(&SchemaBuilder::new().table(table).build()).unwrap();
    let table = schema
        .tables()
        .iter()
        .find(|table| table.name == "items")
        .unwrap();
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let owner_storage = MemoryStorage::new(&family_refs).unwrap();
    let owner_config = || {
        DbConfig::new(
            schema.clone(),
            owner_storage.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0x71; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
    };
    let owner = block_on(Db::open_history_complete(owner_config())).unwrap();
    for index in 0..rows {
        let mut cells =
            BTreeMap::from([("title".to_owned(), Value::String(format!("item-{index}")))]);
        cells.extend((0..17).map(|c| {
            (
                format!("field_{c}"),
                Value::String(format!("value-{index}-{c}")),
            )
        }));
        let write = block_on(owner.insert(
            "items",
            cells,
            InsertOptions {
                row_id: Some(row(index)),
                ..Default::default()
            },
        ))
        .unwrap();
        owner
            .finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
            .unwrap();
    }
    let reopen = Instant::now();
    let owner = if local_relay {
        block_on(owner.close()).unwrap();
        drop(owner);
        // SAFETY: this fixture owns the fresh synthetic storage and exposes
        // it only to the single admitted SYSTEM foreground. The same host
        // capability is used by the durable browser worker constructor.
        let scope = unsafe {
            ClientRelayScope::from_admitted_storage_owner(
                "publication-fairness-local-relay".to_owned(),
                AuthorSubject::SYSTEM,
            )
        };
        block_on(unsafe { Db::open_scope_isolated_client_relay(owner_config(), scope) }).unwrap()
    } else {
        owner
    };
    let owner_reopen_ms = reopen.elapsed().as_secs_f64() * 1000.;
    let foreground = open(&schema, 0x72, false);
    foreground.set_non_durable_client();
    let setup_ms = setup.elapsed().as_secs_f64() * 1000.;
    let began = Instant::now();
    let a = Rc::new(RefCell::new(VecDeque::new()));
    let b = Rc::new(RefCell::new(VecDeque::new()));
    let timing = Rc::new(RefCell::new(Handoff::default()));
    block_on(foreground.connect_upstream(Box::new(Carrier {
        incoming: a.clone(),
        outgoing: b.clone(),
        timing: timing.clone(),
        began,
        serving: false,
    })));
    owner.accept_subscriber(
        Box::new(Carrier {
            incoming: b,
            outgoing: a,
            timing: timing.clone(),
            began,
            serving: true,
        }),
        AuthorSubject::SYSTEM,
    );
    let mut streams = Vec::new();
    let mut results = Vec::new();
    let mut prepare_ms = 0.;
    let mut subscribe_ms = 0.;
    for index in 0..queries {
        let query = Query::from("items")
            .filter(eq(col("id"), lit(row(index).0)))
            .limit(1);
        let phase = Instant::now();
        let prepared = foreground.prepare_query(&query).unwrap();
        prepare_ms += phase.elapsed().as_secs_f64() * 1000.;
        let phase = Instant::now();
        streams.push(
            block_on(foreground.subscribe(
                &prepared,
                ReadOpts {
                    tier: if local_relay {
                        jazz::tx::DurabilityTier::Local
                    } else {
                        jazz::tx::DurabilityTier::Global
                    },
                    ..ReadOpts::default()
                },
            ))
            .unwrap(),
        );
        subscribe_ms += phase.elapsed().as_secs_f64() * 1000.;
        results.push(BTreeMap::new());
    }
    let mut owner_ms = 0.;
    let mut foreground_ms = 0.;
    let mut materialize_ms = 0.;
    let mut owner_polls_ms = Vec::new();
    let mut first_result_ms = None;
    let mut completed = false;
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..1024 {
        let phase = Instant::now();
        block_on(foreground.tick()).unwrap();
        foreground_ms += phase.elapsed().as_secs_f64() * 1000.;
        let phase = Instant::now();
        drain(&mut streams, &mut results, table);
        materialize_ms += phase.elapsed().as_secs_f64() * 1000.;
        if first_result_ms.is_none() && results.iter().any(|r| !r.is_empty()) {
            first_result_ms = Some(began.elapsed().as_secs_f64() * 1000.);
        }
        if results.iter().all(|r| r.len() == 1) {
            completed = true;
            break;
        }
        let mut tick = Box::pin(owner.tick());
        for poll in 0..10000 {
            let phase = Instant::now();
            let progress = tick.as_mut().poll(&mut cx);
            let elapsed = phase.elapsed().as_secs_f64() * 1000.;
            owner_ms += elapsed;
            owner_polls_ms.push(elapsed);
            if let Poll::Ready(result) = progress {
                result.unwrap();
                break;
            }
            assert!(poll < 9999, "owner progress remains bounded");
            // A suspended owner yields to the transport consumer, just as
            // a JS microtask can drain a completed frame between Rust polls.
            let phase = Instant::now();
            block_on(foreground.tick()).unwrap();
            foreground_ms += phase.elapsed().as_secs_f64() * 1000.;
            let phase = Instant::now();
            drain(&mut streams, &mut results, table);
            materialize_ms += phase.elapsed().as_secs_f64() * 1000.;
            if first_result_ms.is_none() && results.iter().any(|r| !r.is_empty()) {
                first_result_ms = Some(began.elapsed().as_secs_f64() * 1000.);
            }
        }
    }
    let elapsed_ms = began.elapsed().as_secs_f64() * 1000.;
    assert!(completed, "all point subscriptions deliver their row");
    for (index, result) in results.iter().enumerate() {
        let mut expected =
            BTreeMap::from([("title".to_owned(), Value::String(format!("item-{index}")))]);
        expected.extend((0..17).map(|c| {
            (
                format!("field_{c}"),
                Value::String(format!("value-{index}-{c}")),
            )
        }));
        assert_eq!(result, &BTreeMap::from([(row(index), expected)]));
    }
    let timing = timing.borrow();
    println!(
        "{}",
        json!({"benchmark":"publication_fairness","mode":if local_relay { "local-relay" } else { "global-core" },"owner_reopen_ms":owner_reopen_ms,"rows":rows,"queries":queries,"setup_ms":setup_ms,"prepare_ms":prepare_ms,"subscribe_ms":subscribe_ms,"owner_ms":owner_ms,"foreground_ms":foreground_ms,"materialize_ms":materialize_ms,"elapsed_ms":elapsed_ms,"first_result_ms":first_result_ms,"first_frame_sent_ms":timing.first_sent_ms,"first_frame_received_ms":timing.first_received_ms,"owner_poll_ms":owner_polls_ms,"frame_holds_ms":timing.frame_holds_ms,"result_signature":blake3::hash(&postcard::to_allocvec(&results).unwrap()).to_hex().to_string(),"compilations":[owner.query_program_compilations_for_test(),foreground.query_program_compilations_for_test()]})
    );
}
fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let rows = support::env_usize("JAZZ_FAIR_ROWS", 600);
    let mode = std::env::var("JAZZ_FAIR_MODE").unwrap_or_else(|_| "global-core".to_owned());
    assert!(
        matches!(mode.as_str(), "global-core" | "local-relay"),
        "known read topology"
    );
    for queries in support::csv_usizes("JAZZ_FAIR_QUERIES", "1,12,60") {
        assert!(queries > 0 && queries <= rows);
        run(rows, queries, mode == "local-relay");
    }
}
