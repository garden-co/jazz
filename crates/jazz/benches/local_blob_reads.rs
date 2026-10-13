//! Attribute complete local file-row reads separately from byte-range reads.
//! Synthetic resident storage; excludes browser transport and image/PDF decoding.
use jazz::{
    block_on,
    db::{
        ClientRelayScope, Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SerializedReadResult,
        Transport,
    },
    groove::{
        large_values::full_materializations_for_test, records::Value, storage::MemoryStorage,
    },
    ids::{AuthorSubject, NodeUuid, RowUuid},
    protocol::SyncMessage,
    query::{Query, col, eq, lit},
    schema::JazzSchema,
    tools::{ColumnType, SchemaBuilder, TableSchemaBuilder},
    wire::TransportError,
};
use serde_json::json;
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    future::Future,
    rc::Rc,
    task::{Context, Poll, Waker},
    time::Instant,
};

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

fn milliseconds(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1000.
}

fn run(length: usize, repetitions: usize) {
    let setup = Instant::now();
    let with_reference = std::env::var_os("JAZZ_BLOB_REFERENCE").is_some();
    let mut assets = TableSchemaBuilder::new("assets")
        .column("label", ColumnType::Text)
        .column("size", ColumnType::BigInt)
        .column("contents", ColumnType::Bytea);
    if with_reference {
        assets = assets.fk_column("folder", "folders");
    }
    let table_count =
        std::env::var("JAZZ_BLOB_SCHEMA_TABLES").map_or(2, |value| value.parse::<usize>().unwrap());
    assert!(table_count >= 2);
    let mut schema_builder = SchemaBuilder::new()
        .table(assets)
        .table(TableSchemaBuilder::new("folders").column("name", ColumnType::Text));
    for index in 2..table_count {
        schema_builder = schema_builder.table(
            TableSchemaBuilder::new(&format!("metadata_{index}"))
                .column("label", ColumnType::Text)
                .column("rank", ColumnType::BigInt)
                .column("enabled", ColumnType::Boolean),
        );
    }
    let schema = JazzSchema::new(&schema_builder.build()).unwrap();
    let table = schema
        .tables()
        .iter()
        .find(|table| table.name == "assets")
        .unwrap()
        .clone();
    let families = schema.column_families();
    let names = families.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = MemoryStorage::new(&names).unwrap();
    let config = || {
        DbConfig::new(
            schema.clone(),
            storage.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0x64; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
    };
    let seed = block_on(Db::open_history_complete(config())).unwrap();
    let id = RowUuid::from_bytes([0x65; 16]);
    // Different bytes across leaf boundaries, without compression-friendly runs.
    let mut state = 0xdecafbad_u32;
    let expected = (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect::<Vec<_>>();
    let folder = RowUuid::from_bytes([0x68; 16]);
    if with_reference {
        let write = block_on(seed.insert(
            "folders",
            BTreeMap::from([("name".to_owned(), Value::String("folder".to_owned()))]),
            InsertOptions {
                row_id: Some(folder),
                ..Default::default()
            },
        ))
        .unwrap();
        seed.finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
            .unwrap();
    }
    let mut cells = BTreeMap::from([
        ("label".to_owned(), Value::String("sample.bin".to_owned())),
        ("size".to_owned(), Value::I64(length as i64)),
        ("contents".to_owned(), Value::Bytes(expected.clone())),
    ]);
    if with_reference {
        cells.insert("folder".to_owned(), Value::Uuid(folder.0));
    }
    let write = block_on(seed.insert(
        "assets",
        cells,
        InsertOptions {
            row_id: Some(id),
            ..Default::default()
        },
    ))
    .unwrap();
    seed.finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
        .unwrap();
    block_on(seed.close()).unwrap();
    drop(seed);
    if std::env::var_os("JAZZ_BLOB_COVERAGE").is_some() {
        run_coverage(
            &schema,
            &storage.export_snapshot().unwrap(),
            id,
            &expected,
            repetitions,
        );
        return;
    }
    let db = block_on(Db::open(config())).unwrap();
    let prepared_at = Instant::now();
    let query = db
        .prepare_query(
            &Query::from("assets")
                .filter(eq(col("id"), lit(id.0)))
                .limit(1),
        )
        .unwrap();
    let prepare_ms = milliseconds(prepared_at.elapsed());
    println!(
        "{}",
        json!({"benchmark":"local_blob_reads", "phase":"setup", "bytes":length,
        "setup_ms":milliseconds(setup.elapsed()), "prepare_ms":prepare_ms})
    );
    for repetition in 0..repetitions {
        let before = full_materializations_for_test();
        let started = Instant::now();
        let (rows, profile) = db.read_profiled(&query).unwrap();
        let read_ms = milliseconds(started.elapsed());
        let materializations = full_materializations_for_test() - before;
        let extraction = Instant::now();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_uuid(), id);
        let value = rows[0].cell(&table, "contents").unwrap();
        let extraction_ms = milliseconds(extraction.elapsed());
        assert_eq!(value, Value::Bytes(expected.clone()));
        assert_eq!(
            rows[0].cell(&table, "size").unwrap(),
            Value::I64(length as i64)
        );
        println!(
            "{}",
            json!({"benchmark":"local_blob_reads", "phase":"full_row", "bytes":length,
                "repetition":repetition, "read_ms":read_ms, "extraction_ms":extraction_ms,
                "full_materializations":materializations,
                "resolve_view_ms":milliseconds(profile.resolve_view),
                "compile_ms":milliseconds(profile.compile_program),
                "select_plan_ms":milliseconds(profile.select_plan),
                "execute_ms":milliseconds(profile.execute_plan),
                "materialize_ms":milliseconds(profile.decode_materialize),
                "finish_ms":milliseconds(profile.finish_rows),
                "projection_ms":milliseconds(profile.apply_projection),
                "unattributed_ms":milliseconds(std::time::Duration::from_secs_f64(read_ms / 1000.).saturating_sub(profile.total)),
            })
        );
    }
    for repetition in 0..repetitions {
        let before = full_materializations_for_test();
        let started = Instant::now();
        let bytes =
            block_on(db.read_value_range("assets", id, "contents", 0..length as u64)).unwrap();
        let read_ms = milliseconds(started.elapsed());
        let materializations = full_materializations_for_test() - before;
        assert_eq!(bytes, expected);
        println!(
            "{}",
            json!({"benchmark":"local_blob_reads", "phase":"full_range", "bytes":length,
            "repetition":repetition, "read_ms":read_ms, "full_materializations":materializations})
        );
    }
    block_on(db.close()).unwrap();
}

// These independent preloaded stores isolate coverage overhead for bytes already
// present at both ends. This is not a cold-transfer or browser/IDB benchmark.
// Opt-in attribution; byte counting and JSON construction happen during send,
// so records from this mode must never be treated as timing measurements.
#[derive(Default)]
struct ProtocolTrace {
    messages: Vec<serde_json::Value>,
}
impl ProtocolTrace {
    fn record(&mut self, direction: &str, message: &SyncMessage) {
        use jazz::protocol::KnownStateDeclaration;
        let mut detail = json!({});
        let kind = match message {
            SyncMessage::RegisterShape { .. } => "RegisterShape",
            SyncMessage::Subscribe(subscribe) => {
                detail["known_state"] = match &subscribe.known_state {
                    None => json!("none"),
                    Some(KnownStateDeclaration::Fast { .. }) => json!("fast"),
                    Some(KnownStateDeclaration::FastWithAuthorizationProgress { .. }) => {
                        json!("fast_with_authorization")
                    }
                    Some(KnownStateDeclaration::ExactVersionSet { versions }) => {
                        detail["known_versions"] = json!(versions.len());
                        json!("exact_versions")
                    }
                };
                "Subscribe"
            }
            SyncMessage::Unsubscribe { .. } => "Unsubscribe",
            SyncMessage::ViewUpdate(view) => {
                let bundles = view
                    .version_carriers
                    .iter()
                    .flat_map(|carrier| carrier.bundle_refs().expect("valid carrier"))
                    .collect::<Vec<_>>();
                detail["bundles"] = json!(bundles.len());
                detail["versions"] = json!(
                    bundles
                        .iter()
                        .map(|bundle| bundle.versions.len())
                        .sum::<usize>()
                );
                detail["support_adds"] = json!(view.supporting_rows.added_rows().len());
                detail["support_removes"] = json!(view.supporting_rows.removed_rows().len());
                detail["snapshot"] = json!(view.supporting_rows.is_snapshot());
                detail["complete_payload_refs"] =
                    json!(view.peer_payload_inventory.complete_tx_payloads.len());
                "ViewUpdate"
            }
            SyncMessage::CatalogueSnapshot { .. } => "CatalogueSnapshot",
            SyncMessage::CatalogueAck { .. } => "CatalogueAck",
            SyncMessage::SessionClaims { .. } => "SessionClaims",
            SyncMessage::ChunkRequestBatch { .. } => "ChunkRequestBatch",
            SyncMessage::ChunkResponseBatch { .. } => "ChunkResponseBatch",
            SyncMessage::FetchRowVersions { .. } => "FetchRowVersions",
            SyncMessage::RowVersionPayloads { .. } => "RowVersionPayloads",
            SyncMessage::CommitUnit { .. } => "CommitUnit",
            SyncMessage::FateUpdate { .. } => "FateUpdate",
            _ => "other",
        };
        let bytes =
            postcard::serialize_with_flavor(message, postcard::ser_flavors::Size::default())
                .expect("canonical payload size");
        self.messages
            .push(json!({"direction":direction, "kind":kind,
            "canonical_payload_bytes":bytes, "detail":detail}));
    }
    fn flush(&mut self, phase: &str, bytes: usize, repetition: Option<usize>) {
        println!(
            "{}",
            json!({"benchmark":"local_blob_protocol", "measurement_kind":"attribution_only",
            "phase":phase, "bytes":bytes, "repetition":repetition, "messages":self.messages})
        );
        self.messages.clear();
    }
}

struct CachedPeer {
    trace: Option<Rc<RefCell<ProtocolTrace>>>,
    direction: &'static str,
    incoming: Rc<RefCell<VecDeque<SyncMessage>>>,
    outgoing: Rc<RefCell<VecDeque<SyncMessage>>>,
    sent: Rc<RefCell<usize>>,
}
impl Transport for CachedPeer {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        *self.sent.borrow_mut() += 1;
        if let Some(trace) = &self.trace {
            trace.borrow_mut().record(self.direction, &message);
        }
        self.outgoing.borrow_mut().push_back(message);
        Ok(())
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.incoming.borrow_mut().pop_front()
    }
}
fn run_coverage(
    schema: &JazzSchema,
    snapshot: &[u8],
    id: RowUuid,
    expected: &[u8],
    repetitions: usize,
) {
    let account_identity = std::env::var_os("JAZZ_BLOB_ACCOUNT").is_some();
    let author = if account_identity {
        AuthorSubject::for_test_bytes([0x69; 16])
    } else {
        AuthorSubject::SYSTEM
    };
    let config = |tag| {
        let storage = MemoryStorage::default();
        storage.import_snapshot(snapshot).unwrap();
        DbConfig::new(
            schema.clone(),
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([tag; 16]),
                author,
            },
        )
    };
    // SAFETY: only this fixture owns these synthetic stores and the sole
    // foreground has the same admitted author as its local relay.
    let scope = unsafe {
        ClientRelayScope::from_admitted_storage_owner("local-blob-coverage".to_owned(), author)
    };
    let owner =
        block_on(unsafe { Db::open_scope_isolated_client_relay(config(0x66), scope) }).unwrap();
    let foreground = block_on(Db::open(config(0x67))).unwrap();
    foreground.set_non_durable_client();
    let a = Rc::new(RefCell::new(VecDeque::new()));
    let b = Rc::new(RefCell::new(VecDeque::new()));
    let trace = std::env::var_os("JAZZ_BLOB_PROTOCOL_TRACE")
        .map(|_| Rc::new(RefCell::new(ProtocolTrace::default())));
    let owner_sent = Rc::new(RefCell::new(0));
    let foreground_sent = Rc::new(RefCell::new(0));
    let claims = if account_identity || std::env::var_os("JAZZ_BLOB_CLAIMS").is_some() {
        jazz::tools::policy_claims::canonical_policy_binding_claims(&author, BTreeMap::new())
    } else {
        BTreeMap::new()
    };
    let claim_count = claims.len();
    foreground.set_identity_claims(author, claims.clone());
    let _upstream = block_on(foreground.connect_upstream(Box::new(CachedPeer {
        trace: trace.clone(),
        direction: "foreground_to_owner",
        incoming: a.clone(),
        outgoing: b.clone(),
        sent: foreground_sent.clone(),
    })));
    let _subscriber = owner.accept_subscriber_with_claims(
        Box::new(CachedPeer {
            trace: trace.clone(),
            direction: "owner_to_foreground",
            incoming: b,
            outgoing: a,
            sent: owner_sent.clone(),
        }),
        author,
        claims,
    );
    // Hold independent, already-covered metadata queries while opening the file.
    // Their source table is unchanged by the measured reads.
    let background_count =
        std::env::var("JAZZ_BLOB_BACKGROUND").map_or(0, |v| v.parse::<usize>().unwrap());
    let mut background = Vec::new();
    for index in 0..background_count {
        let query = foreground
            .prepare_query(&Query::from("folders").limit(index + 1))
            .unwrap();
        background.push(foreground.attach_query(&query).unwrap());
    }
    for turn in 0..128 {
        block_on(foreground.tick()).unwrap();
        block_on(owner.tick()).unwrap();
        if background
            .iter()
            .all(|handle| foreground.query_attachment_is_covered(handle))
        {
            break;
        }
        assert!(turn < 127, "background coverage completes before timing");
    }
    for _ in 0..4 {
        block_on(foreground.tick()).unwrap();
        block_on(owner.tick()).unwrap();
    }
    if let Some(trace) = &trace {
        trace.borrow_mut().flush("setup", expected.len(), None);
    }
    let query = postcard::to_allocvec(
        &Query::from("assets")
            .filter(eq(col("id"), lit(id.0)))
            .limit(1),
    )
    .unwrap();
    let table = schema.tables().iter().find(|t| t.name == "assets").unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    for repetition in 0..repetitions {
        let before = full_materializations_for_test();
        *owner_sent.borrow_mut() = 0;
        *foreground_sent.borrow_mut() = 0;
        let began = Instant::now();
        let mut read = Box::pin(foreground.all_serialized_query(
            &query,
            ReadOpts::default(),
            None,
            None,
            None,
            true,
            || began.elapsed().as_secs() >= 10,
            |attachment| foreground.detach_query(attachment),
        ));
        let mut owner_tick = None;
        let mut foreground_tick = None;
        let mut read_ms = 0.;
        let mut owner_ms = 0.;
        let mut foreground_ms = 0.;
        let mut turns = 0;
        let result = loop {
            turns += 1;
            let t = Instant::now();
            let result = read.as_mut().poll(&mut cx);
            read_ms += milliseconds(t.elapsed());
            if let Poll::Ready(result) = result {
                break result.unwrap();
            }
            if turns > 10000 {
                panic!(
                    "coverage did not complete: foreground={} owner={}",
                    foreground.query_delivery_diagnostics_for_test(),
                    owner.query_delivery_diagnostics_for_test()
                );
            }
            let t = Instant::now();
            let tick = foreground_tick.get_or_insert_with(|| Box::pin(foreground.tick()));
            if let Poll::Ready(result) = tick.as_mut().poll(&mut cx) {
                result.unwrap();
                foreground_tick = None;
            }
            foreground_ms += milliseconds(t.elapsed());
            let t = Instant::now();
            let tick = owner_tick.get_or_insert_with(|| Box::pin(owner.tick()));
            if let Poll::Ready(result) = tick.as_mut().poll(&mut cx) {
                result.unwrap();
                owner_tick = None;
            }
            owner_ms += milliseconds(t.elapsed());
        };
        let elapsed_ms = milliseconds(began.elapsed());
        drop(read);
        drop(owner_tick);
        drop(foreground_tick);
        let materializations = full_materializations_for_test() - before;
        let SerializedReadResult::Rows(rows) = result else {
            panic!("plain query returned relation");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_uuid(), id);
        assert_eq!(
            rows[0].cell(table, "contents").unwrap(),
            Value::Bytes(expected.to_vec())
        );
        println!(
            "{}",
            json!({"benchmark":"local_blob_reads", "phase":"cached_peer_coverage", "measurement_kind":if trace.is_some() { "attribution_only" } else { "wall_clock" }, "with_reference":std::env::var_os("JAZZ_BLOB_REFERENCE").is_some(),
            "account_identity":account_identity, "schema_tables":schema.tables().len(),
            "claim_count":claim_count, "background_queries":background_count, "bytes":expected.len(), "repetition":repetition, "elapsed_ms":elapsed_ms,
            "read_poll_ms":read_ms, "owner_tick_ms":owner_ms, "foreground_tick_ms":foreground_ms,
            "turns":turns, "owner_messages":*owner_sent.borrow(), "foreground_messages":*foreground_sent.borrow(),
            "full_materializations":materializations})
        );
        if let Some(trace) = &trace {
            trace
                .borrow_mut()
                .flush("read", expected.len(), Some(repetition));
        }
        // Finish releasing this one-shot before the next independently fresh read.
        for _ in 0..4 {
            block_on(foreground.tick()).unwrap();
            block_on(owner.tick()).unwrap();
        }
        if let Some(trace) = &trace {
            trace
                .borrow_mut()
                .flush("cleanup", expected.len(), Some(repetition));
        }
    }
    if let Some(trace) = &trace {
        trace
            .borrow_mut()
            .flush("final_cleanup", expected.len(), None);
    }
    for handle in background {
        foreground.detach_query(handle);
    }
    block_on(foreground.close()).unwrap();
    block_on(owner.close()).unwrap();
}

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let sizes =
        std::env::var("JAZZ_BLOB_BYTES").unwrap_or_else(|_| "65536,524288,4194304,16777216".into());
    let repetitions = std::env::var("JAZZ_BLOB_REPEATS").map_or(5, |v| v.parse::<usize>().unwrap());
    assert!(repetitions > 0);
    for size in sizes.split(',') {
        run(size.parse().unwrap(), repetitions);
    }
}
