//! Staged Local reads through a resident client relay. Synthetic schema/data;
//! no browser, authorization, storage callback or document decoding costs.
use super::*;
use jazz::query::OrderDirection;
use std::collections::BTreeSet;

const AUX_ROWS: [usize; 15] = [1, 1, 4, 4, 250, 1, 4, 3, 4, 16, 2, 22, 0, 0, 8];
const FIELDS: usize = 36;

// Optional host wake ownership; this receipt's driver still runs each tick.
struct ManualHostScheduler;
impl jazz::db::TickScheduler for ManualHostScheduler {
    fn schedule_tick(&self, _: jazz::db::TickUrgency) {}
    fn schedule_tick_after(&self, _: u64) {}
    fn query_runtime_waker(&self) -> Option<std::task::Waker> {
        Some(noop_waker())
    }
}

// Counter collection is outside the measured endpoint.
fn runtime_work(db: &Db<MemoryStorage>) -> serde_json::Value {
    let stats = db.runtime_stats_for_test();
    json!({
        "graph_nodes": stats.graph_nodes,
        "hydration_memo_hits": stats.hydration_memo_hits,
        "hydration_memo_computes": stats.hydration_memo_computes,
        "hydration_memo_distinct_computed_nodes": stats.hydration_memo_distinct_computed_nodes,
        "arrangement_rows": stats.arrangement_rows,
        "arrangement_encoded_bytes": stats.arrangement_encoded_bytes,
        "eval_memo_bytes": stats.eval_memo_bytes,
    })
}

fn cells(table: &str, index: usize, width: usize) -> RowCells {
    let mut result = BTreeMap::from([
        ("sequence".to_owned(), Value::I64(index as i64)),
        ("category".to_owned(), Value::I64((index % 12) as i64)),
    ]);
    result.extend((0..FIELDS).map(|c| {
        (
            format!("field_{c}"),
            Value::String(format!("{table}-{index}-{c}-{}", "x".repeat(width))),
        )
    }));
    result
}

struct Case {
    label: String,
    query: Query,
    table: String,
    fields: Vec<String>,
    ids: BTreeSet<RowUuid>,
    result: BTreeMap<RowUuid, RowCells>,
    seen: bool,
    completed_ms: Option<f64>,
}

fn case(
    label: &str,
    table: &str,
    query: Query,
    ids: impl IntoIterator<Item = usize>,
    fields: Option<usize>,
) -> Case {
    let fields = fields.map_or_else(
        || {
            std::iter::once("category".to_owned())
                .chain(std::iter::once("sequence".to_owned()))
                .chain((0..FIELDS).map(|c| format!("field_{c}")))
                .collect()
        },
        |count| {
            std::iter::once("sequence".to_owned())
                .chain((0..count.saturating_sub(1)).map(|c| format!("field_{c}")))
                .collect::<Vec<_>>()
        },
    );
    Case {
        label: label.to_owned(),
        query: query.select(fields.clone()),
        table: table.to_owned(),
        fields,
        ids: ids.into_iter().map(row).collect(),
        result: BTreeMap::new(),
        seen: false,
        completed_ms: None,
    }
}
fn point(table: &str, index: usize) -> Query {
    Query::from(table)
        .filter(eq(col("id"), lit(row(index).0)))
        .limit(1)
}
fn ordered(query: Query) -> Query {
    query.order_by("sequence", OrderDirection::Desc)
}
fn plan(rows: usize) -> Vec<Case> {
    let aux = |n: usize| format!("aux_{n}");
    let mut cases = vec![
        case("boot_a", "aux_0", point("aux_0", 0), [0], None),
        case("boot_b", "aux_1", point("aux_1", 0), [0], None),
        case(
            "empty_list",
            "items",
            Query::from("items").filter(eq(col("category"), lit(-1_i64))),
            [],
            Some(20),
        ),
        case("small_keys", "aux_2", Query::from("aux_2"), 0..4, Some(1)),
        case(
            "filtered_list",
            "items",
            ordered(Query::from("items").filter(eq(col("category"), lit(1_i64)))),
            (0..rows).filter(|i| i % 12 == 1),
            Some(19),
        ),
        case(
            "small_projection",
            "aux_3",
            Query::from("aux_3"),
            0..4,
            Some(3),
        ),
        case(
            "filtered_keys",
            "items",
            Query::from("items").filter(eq(col("category"), lit(1_i64))),
            (0..rows).filter(|i| i % 12 == 1),
            Some(1),
        ),
        case("reference_list", "refs", Query::from("refs"), 0..60, None),
        case(
            "full_list",
            "items",
            ordered(Query::from("items")),
            0..rows,
            Some(19),
        ),
        case(
            "empty_ordered_list",
            "items",
            ordered(Query::from("items").filter(eq(col("category"), lit(-2_i64)))),
            [],
            Some(19),
        ),
        case(
            "ordered_reference_list",
            "refs",
            ordered(Query::from("refs")),
            0..60,
            None,
        ),
        case(
            "bounded_background",
            "aux_4",
            ordered(Query::from("aux_4")).limit(250),
            0..250,
            None,
        ),
        case("small_point", "aux_3", point("aux_3", 0), [0], Some(3)),
        case("small_singleton", "aux_5", point("aux_5", 0), [0], None),
        case("background_a", "aux_6", Query::from("aux_6"), 0..4, None),
        case("background_b", "aux_7", Query::from("aux_7"), 0..3, None),
        case(
            "background_keys",
            "aux_8",
            Query::from("aux_8").filter(jazz::query::gte(col("sequence"), lit(2_i64))),
            2..4,
            Some(1),
        ),
        case("background_c", "aux_9", Query::from("aux_9"), 0..16, None),
    ];
    for (n, count) in AUX_ROWS.iter().enumerate().skip(10) {
        let table = aux(n);
        cases.push(case(
            &format!("dependent_background_{n}"),
            &table,
            Query::from(&table),
            0..*count,
            (n == 10).then_some(5),
        ));
    }
    cases.push(case(
        "dependent_background_8",
        "aux_8",
        Query::from("aux_8"),
        0..4,
        None,
    ));
    cases.push(case(
        "detail",
        "items",
        point("items", rows - 1),
        [rows - 1],
        None,
    ));
    for n in 0..12 {
        cases.push(case(
            &format!("reference_{n}"),
            "refs",
            point("refs", n),
            [n],
            Some(6),
        ));
    }
    assert_eq!(cases.len(), 37);
    cases
}

#[derive(Default)]
struct Phase {
    prepare_ms: f64,
    subscribe_ms: f64,
    owner_ms: f64,
    foreground_ms: f64,
    extract_ms: f64,
}
struct Driver {
    cases: Vec<Case>,
    streams: Vec<SubscriptionStream>,
    phases: [Phase; 3],
    stage: usize,
    began: Instant,
}
impl Driver {
    fn open_until(&mut self, foreground: &Db<MemoryStorage>, end: usize) {
        for c in &self.cases[self.streams.len()..end] {
            let phase = Instant::now();
            let prepared = foreground.prepare_query(&c.query).unwrap();
            self.phases[self.stage].prepare_ms += phase.elapsed().as_secs_f64() * 1000.;
            let phase = Instant::now();
            self.streams.push(
                block_on(foreground.subscribe(
                    &prepared,
                    ReadOpts {
                        tier: jazz::tx::DurabilityTier::Local,
                        ..ReadOpts::default()
                    },
                ))
                .unwrap(),
            );
            self.phases[self.stage].subscribe_ms += phase.elapsed().as_secs_f64() * 1000.;
        }
    }
    fn pump_foreground(&mut self, foreground: &Db<MemoryStorage>, schema: &JazzSchema) {
        let phase = Instant::now();
        block_on(foreground.tick()).unwrap();
        self.phases[self.stage].foreground_ms += phase.elapsed().as_secs_f64() * 1000.;
        let phase = Instant::now();
        for (c, stream) in self.cases.iter_mut().zip(&mut self.streams) {
            let table = schema
                .tables()
                .iter()
                .find(|table| table.name == c.table)
                .unwrap();
            while let Some(event) = stream.try_next_event() {
                match event {
                    SubscriptionEvent::Delta {
                        reset,
                        added,
                        updated,
                        removed,
                        ..
                    } => {
                        c.seen = true;
                        if reset {
                            c.result.clear();
                        }
                        for r in removed {
                            c.result.remove(&r.row_uuid);
                        }
                        for r in added.into_iter().chain(updated) {
                            let cells = c
                                .fields
                                .iter()
                                .map(|name| {
                                    (name.clone(), r.cell(table, name).expect("projected cell"))
                                })
                                .collect();
                            c.result.insert(r.row_uuid(), cells);
                        }
                    }
                    SubscriptionEvent::Rejected { reason } => panic!("query rejected: {reason:?}"),
                    SubscriptionEvent::Closed => panic!("query closed"),
                }
            }
            if c.seen && c.result.len() == c.ids.len() {
                c.completed_ms
                    .get_or_insert_with(|| self.began.elapsed().as_secs_f64() * 1000.);
            }
        }
        self.phases[self.stage].extract_ms += phase.elapsed().as_secs_f64() * 1000.;
        if self.stage == 0 && self.cases[..2].iter().all(|c| c.completed_ms.is_some()) {
            self.stage = 1;
            self.open_until(foreground, 18);
        }
        if self.stage == 1 && self.cases[8].completed_ms.is_some() {
            self.stage = 2;
            self.open_until(foreground, self.cases.len());
        }
    }
}

pub(super) fn run(rows: usize) {
    assert!(rows >= 12);
    let width = support::env_usize("JAZZ_FAIR_MIXED_WIDTH", 256);
    let setup = Instant::now();
    let mut builder = SchemaBuilder::new();
    let tables = std::iter::once(("items".to_owned(), rows))
        .chain(std::iter::once(("refs".to_owned(), 60)))
        .chain(
            AUX_ROWS
                .iter()
                .enumerate()
                .map(|(n, count)| (format!("aux_{n}"), *count)),
        )
        .collect::<Vec<_>>();
    for (name, _) in &tables {
        let mut table = TableSchemaBuilder::new(name)
            .column("sequence", ColumnType::BigInt)
            .column("category", ColumnType::BigInt);
        for c in 0..FIELDS {
            table = table.column(&format!("field_{c}"), ColumnType::Text);
        }
        builder = builder.table(table);
    }
    let schema = JazzSchema::new(&builder.build()).unwrap();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = MemoryStorage::new(&refs).unwrap();
    let config = || {
        DbConfig::new(
            schema.clone(),
            storage.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0x73; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
    };
    let seed = block_on(Db::open_history_complete(config())).unwrap();
    for (table, count) in &tables {
        for index in 0..*count {
            let write = block_on(seed.insert(
                table,
                cells(table, index, if table == "items" { width } else { 16 }),
                InsertOptions {
                    row_id: Some(row(index)),
                    ..Default::default()
                },
            ))
            .unwrap();
            seed.finalize_local_mergeable_commit_for_test(write.mergeable_tx_id())
                .unwrap();
        }
    }
    block_on(seed.close()).unwrap();
    drop(seed);
    // SAFETY: fresh synthetic storage is owned exclusively by this fixture;
    // the only foreground is the admitted SYSTEM identity.
    let scope = unsafe {
        ClientRelayScope::from_admitted_storage_owner(
            "mixed-local-query-startup".to_owned(),
            AuthorSubject::SYSTEM,
        )
    };
    let owner = block_on(unsafe { Db::open_scope_isolated_client_relay(config(), scope) }).unwrap();
    let foreground = open(&schema, 0x74, false);
    foreground.set_non_durable_client();
    let host_scheduler = std::env::var_os("JAZZ_FAIR_HOST_SCHEDULER").is_some();
    if host_scheduler {
        owner.set_tick_scheduler(Some(Rc::new(ManualHostScheduler)));
        foreground.set_tick_scheduler(Some(Rc::new(ManualHostScheduler)));
    }
    let cases = plan(rows);
    let runtime_work_before = [runtime_work(&owner), runtime_work(&foreground)];
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
    let mut driver = Driver {
        cases,
        streams: Vec::new(),
        phases: std::array::from_fn(|_| Phase::default()),
        stage: 0,
        began,
    };
    driver.open_until(&foreground, 2);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut polls = Vec::new();
    for turn in 0..1024 {
        driver.pump_foreground(&foreground, &schema);
        if driver.cases.iter().all(|c| c.completed_ms.is_some()) {
            break;
        }
        assert!(turn < 1023, "all queries complete");
        let mut tick = Box::pin(owner.tick());
        for poll in 0..10000 {
            let phase = Instant::now();
            let result = tick.as_mut().poll(&mut cx);
            let elapsed = phase.elapsed().as_secs_f64() * 1000.;
            driver.phases[driver.stage].owner_ms += elapsed;
            polls.push(elapsed);
            if let Poll::Ready(result) = result {
                result.unwrap();
                break;
            }
            assert!(poll < 9999, "bounded owner progress");
            driver.pump_foreground(&foreground, &schema);
        }
    }
    let elapsed_ms = began.elapsed().as_secs_f64() * 1000.;
    for c in &driver.cases {
        assert_eq!(
            c.result.keys().copied().collect::<BTreeSet<_>>(),
            c.ids,
            "{} ids",
            c.label
        );
        for (id, actual) in &c.result {
            let index = u128::from_le_bytes(*id.0.as_bytes()) as usize - 1;
            let mut expected = cells(&c.table, index, if c.table == "items" { width } else { 16 });
            expected.retain(|name, _| c.fields.contains(name));
            assert_eq!(actual, &expected, "{} complete projection", c.label);
        }
    }
    let timing = timing.borrow();
    println!(
        "{}",
        json!({
            "benchmark":"publication_fairness", "layout":"mixed-local", "mode":"local-relay", "rows":rows, "width":width, "queries":driver.cases.len(),
            "host_scheduler":host_scheduler, "setup_ms":setup_ms, "elapsed_ms":elapsed_ms, "full_list_ms":driver.cases[8].completed_ms,
            "detail_ms":driver.cases.iter().find(|c| c.label == "detail").unwrap().completed_ms,
            "phases":driver.phases.iter().map(|p| json!({"prepare_ms":p.prepare_ms,"subscribe_ms":p.subscribe_ms,"owner_ms":p.owner_ms,"foreground_ms":p.foreground_ms,"extract_ms":p.extract_ms})).collect::<Vec<_>>(),
            "queries_completed":driver.cases.iter().map(|c| json!({"label":c.label,"rows":c.result.len(),"at_ms":c.completed_ms})).collect::<Vec<_>>(),
            "first_frame_sent_ms":timing.first_sent_ms, "first_frame_received_ms":timing.first_received_ms,
            "owner_poll_ms":polls, "frame_holds_ms":timing.frame_holds_ms,
            "result_signature":blake3::hash(&postcard::to_allocvec(&driver.cases.iter().map(|c| &c.result).collect::<Vec<_>>()).unwrap()).to_hex().to_string(),
            "compilations":[owner.query_program_compilations_for_test(),foreground.query_program_compilations_for_test()],
            "runtime_work_before":runtime_work_before,
            "runtime_work":[runtime_work(&owner),runtime_work(&foreground)]
        })
    );
}
