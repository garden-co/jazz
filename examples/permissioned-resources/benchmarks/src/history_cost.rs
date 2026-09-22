//! Isolate the real receiver bulk installer from subscription/query work.
use super::*;
use jazz::protocol::VersionBundle;
use std::io::BufRead;

fn hex(v: &JsonValue) -> Vec<u8> {
    v.as_str()
        .unwrap()
        .as_bytes()
        .chunks_exact(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn read(path: &Path) -> Vec<VersionBundle> {
    let mut out = Vec::new();
    for line in std::io::BufReader::new(fs::File::open(path).unwrap()).lines() {
        let frame: JsonValue = serde_json::from_str(&line.unwrap()).unwrap();
        for b in frame["bundles"].as_array().unwrap() {
            out.push(VersionBundle {
                tx: postcard::from_bytes(&hex(&b["tx_hex"])).unwrap(),
                versions: b["versions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| postcard::from_bytes(&hex(&v["wire_hex"])).unwrap())
                    .collect(),
                scope: serde_json::from_value(b["scope"].clone()).unwrap(),
                fate: serde_json::from_value(b["fate"].clone()).unwrap(),
                global_time: serde_json::from_value(b["global_time"].clone()).unwrap(),
                durability: serde_json::from_value(b["durability"].clone()).unwrap(),
            });
        }
    }
    out
}
fn opts() -> ReadOpts {
    ReadOpts {
        propagation: jazz::db::Propagation::LocalOnly,
        ..ReadOpts::default()
    }
}
fn apply_complete_event(rows: &mut BTreeSet<RowUuid>, event: SubscriptionEvent) {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            terminal_operations,
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
            for op in terminal_operations {
                assert!(
                    op.path.is_empty(),
                    "fixture queries have no nested terminal paths"
                );
                match op.edit {
                    jazz::groove::ivm::TerminalEdit::Insert { value, .. }
                    | jazz::groove::ivm::TerminalEdit::Update { value, .. } => {
                        let value = op.root_descriptor.bind(&value).get_idx(0).unwrap();
                        let id = match value {
                            Value::Uuid(id) => id,
                            Value::Nullable(Some(v)) => match *v {
                                Value::Uuid(id) => id,
                                _ => panic!("root key must be UUID"),
                            },
                            _ => panic!("root key must be UUID"),
                        };
                        rows.insert(RowUuid(id));
                    }
                    jazz::groove::ivm::TerminalEdit::Move { .. } => {}
                    jazz::groove::ivm::TerminalEdit::Remove { .. } => {
                        panic!("append-only fixture unexpectedly removed a root")
                    }
                }
            }
        }
        SubscriptionEvent::Rejected { reason } => panic!("rejected: {reason:?}"),
        SubscriptionEvent::Closed => panic!("unexpected close"),
    }
}

pub fn run(root: &Path) {
    assert_ne!(
        storage_mode(),
        "rocks",
        "use all-memory to isolate CPU work"
    );
    let fixture: JsonValue =
        serde_json::from_reader(fs::File::open(root.join("fixture.json")).unwrap()).unwrap();
    let author = AuthorSubject::for_test_uuid(build_seed_plan(&Config::from_env()).ordinary_user.0);
    let mut expected = fixture["expected"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(t, ids)| {
            (
                t.clone(),
                ids.as_array()
                    .unwrap()
                    .iter()
                    .map(|id| RowUuid(uuid::Uuid::parse_str(id.as_str().unwrap()).unwrap()))
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if std::env::var_os("JAZZ_HISTORY_GROUP_ONLY").is_some() {
        expected.retain(|t, _| t == "group");
    }
    let fixture_cells = fixture["writes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| {
            (
                (
                    w["table"].as_str().unwrap().to_owned(),
                    RowUuid(uuid::Uuid::parse_str(w["id"].as_str().unwrap()).unwrap()),
                ),
                &w["cells"],
            )
        })
        .collect::<BTreeMap<_, _>>();
    let table_schema = schema();
    for dataset in ["core-edge", "edge-client"] {
        let mut bundles = read(&root.join(format!("{dataset}.jsonl")));
        if std::env::var_os("JAZZ_HISTORY_GROUP_ONLY").is_some()
            && std::env::var_os("JAZZ_HISTORY_ALL_BUNDLES").is_none()
        {
            bundles.retain(|b| b.versions.iter().all(|v| v.table() == "group"));
        }
        let unique = bundles
            .iter()
            .map(|b| b.tx.tx_id)
            .collect::<BTreeSet<_>>()
            .len();
        for round in 0..std::env::var("JAZZ_HISTORY_ROUNDS")
            .ok()
            .map(|v| v.parse::<u64>().unwrap())
            .unwrap_or(3)
        {
            for active in [false, true] {
                if active && std::env::var_os("JAZZ_HISTORY_NO_ACTIVE").is_some() {
                    continue;
                }
                if !active && std::env::var_os("JAZZ_HISTORY_ACTIVE_ONLY").is_some() {
                    continue;
                }
                let dir = Rc::new(tempfile::tempdir().unwrap());
                let db = block_on(Db::open_history_complete(DbConfig {
                    schema: schema(),
                    storage: open_receiver_storage(dir.path(), &schema()),
                    identity: DbIdentity {
                        node: node(70 + round),
                        author: AuthorSubject::SYSTEM,
                    },
                    id_source: None,
                }))
                .unwrap();
                let receiver = DbNode { _dir: dir, db };
                let mut streams = Vec::new();
                let prepare = Instant::now();
                if active {
                    for table in expected.keys().take(
                        std::env::var("JAZZ_HISTORY_QUERY_LIMIT")
                            .ok()
                            .map(|v| v.parse::<usize>().unwrap())
                            .unwrap_or(usize::MAX),
                    ) {
                        let q = receiver
                            .db
                            .prepare_query(&Query::from(table.as_str()))
                            .unwrap();
                        let stream =
                            block_on(receiver.db.subscribe_for_identity(&q, opts(), author))
                                .unwrap();
                        streams.push((table.clone(), stream, BTreeSet::new()));
                    }
                }
                if active {
                    let mut initial = BTreeSet::new();
                    for _ in 0..16 {
                        block_on(receiver.db.refresh_after_benchmark_ingest()).unwrap();
                        for (table, stream, rows) in &mut streams {
                            while let Some(event) = stream.try_next_event() {
                                if matches!(&event, SubscriptionEvent::Delta { reset: true, .. }) {
                                    initial.insert(table.clone());
                                }
                                apply_complete_event(rows, event);
                                assert!(rows.is_empty());
                            }
                        }
                        if initial.len() == streams.len() {
                            break;
                        }
                    }
                    assert_eq!(
                        initial.len(),
                        streams.len(),
                        "empty subscriptions must be initialized before ingest"
                    );
                }
                let subscription_setup_ms = prepare.elapsed().as_secs_f64() * 1000.;
                alloc_metrics::reset_and_start();
                work_budget::start();
                let t = Instant::now();
                let installed = block_on(
                    receiver
                        .db
                        .ingest_captured_reset_bundles_for_benchmark(&bundles),
                )
                .unwrap();
                let ingest_ms = t.elapsed().as_secs_f64() * 1000.;
                let allocations = alloc_metrics::stop();
                let budget = work_budget::stop();
                assert_eq!(installed, unique);
                if let Some(path) = std::env::var_os("JAZZ_CUSTOMER_WORK_BUDGET") {
                    fs::write(
                        format!(
                            "{}.{}.{round}.{active}.json",
                            Path::new(&path).display(),
                            dataset
                        ),
                        serde_json::to_vec(&budget).unwrap(),
                    )
                    .unwrap();
                }
                let t = Instant::now();
                if active {
                    for _ in 0..16 {
                        let refreshed =
                            block_on(receiver.db.refresh_after_benchmark_ingest()).unwrap();
                        block_on(receiver.db.tick()).unwrap();
                        if std::env::var_os("JAZZ_HISTORY_DEBUG").is_some() {
                            eprintln!("HISTORY_REFRESH changed={refreshed} tick=()");
                        }
                        for (table, stream, rows) in &mut streams {
                            while let Some(event) = stream.try_next_event() {
                                if table == "group"
                                    && std::env::var_os("JAZZ_HISTORY_DEBUG").is_some()
                                    && let SubscriptionEvent::Delta {
                                        reset,
                                        added,
                                        updated,
                                        removed,
                                        terminal_operations,
                                        publishable,
                                        settled,
                                        ..
                                    } = &event
                                {
                                    eprintln!(
                                        "HISTORY_GROUP reset={reset} add={} update={} remove={} terminal={} publishable={publishable} settled={settled}",
                                        added.len(),
                                        updated.len(),
                                        removed.len(),
                                        terminal_operations.len()
                                    );
                                }
                                apply_complete_event(rows, event);
                            }
                        }
                        if streams
                            .iter()
                            .all(|(table, _, rows)| rows == &expected[table])
                        {
                            break;
                        }
                    }
                    for (table, _, rows) in &streams {
                        assert_eq!(rows, &expected[table], "active {table}");
                    }
                }
                let delivery_ms = t.elapsed().as_secs_f64() * 1000.;
                // First one-shot over installed state, then a second pass, both
                // include query preparation and materialization at the facade.
                let mut query_ms = Vec::new();
                for _ in 0..2 {
                    let t = Instant::now();
                    let mut results = Vec::new();
                    for table in expected.keys().take(
                        std::env::var("JAZZ_HISTORY_QUERY_LIMIT")
                            .ok()
                            .map(|v| v.parse::<usize>().unwrap())
                            .unwrap_or(usize::MAX),
                    ) {
                        let q = receiver
                            .db
                            .prepare_query(&Query::from(table.as_str()))
                            .unwrap();
                        let rows =
                            block_on(receiver.db.all_for_identity(&q, opts(), author)).unwrap();
                        let columns = &table_schema
                            .tables
                            .iter()
                            .find(|t| &t.name == table)
                            .unwrap()
                            .columns;
                        let fields = rows
                            .iter()
                            .map(|row| {
                                (0..columns.len())
                                    .map(|i| row.cell_at(i).unwrap())
                                    .collect::<Vec<_>>()
                            })
                            .collect::<Vec<_>>();
                        std::hint::black_box(&fields);
                        results.push((table, rows, fields));
                    }
                    query_ms.push(t.elapsed().as_secs_f64() * 1000.);
                    for (table, rows, fields) in results {
                        let columns = &table_schema
                            .tables
                            .iter()
                            .find(|t| &t.name == table)
                            .unwrap()
                            .columns;
                        for (row, fields) in rows.iter().zip(fields) {
                            for (column, value) in columns.iter().zip(fields) {
                                assert_eq!(
                                    serde_json::to_value(value).unwrap(),
                                    fixture_cells[&(table.clone(), row.row_uuid())][column.name()]
                                );
                            }
                        }
                        assert_eq!(
                            rows.iter().map(|r| r.row_uuid()).collect::<BTreeSet<_>>(),
                            expected[table],
                            "one-shot {table}"
                        );
                    }
                }
                println!(
                    "{}",
                    json!({"dataset":dataset,"round":round,"active":active,"unique_transactions":unique,"ingest_ms":ingest_ms,"subscription_setup_ms":subscription_setup_ms,"delivery_ms":delivery_ms,"query_ms":query_ms,"ingest_allocations":allocations.allocs,"ingest_allocation_bytes":allocations.bytes})
                );
            }
        }
    }
}
