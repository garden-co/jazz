//! Memory and latency receipt for keyed joins: an include whose target is a
//! stored table read by the subscribers' keys versus arranged whole.
//!
//! Run each plan in a fresh process so RSS stays attributable:
//!
//! ```text
//! JAZZ_KEYED_JOINS=on  cargo bench --profile perf -p jazz --features testing \
//!   --bench keyed_join_memory --quiet
//! JAZZ_KEYED_JOINS=off cargo bench --profile perf -p jazz --features testing \
//!   --bench keyed_join_memory --quiet
//! ```
//!
//! `JAZZ_KJ_PROJECTS` sets the included table's size, `JAZZ_KJ_ROUTES` the
//! subscriber count (one team each) and `JAZZ_KJ_DOCS_PER_ROUTE` each
//! team's documents, which reference distinct projects. With
//! `JAZZ_KJ_SHARED=1` every team's documents reference the same projects.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

mod schema_fixture;

use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent, SubscriptionStream,
    block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, param};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

const PROJECTS: &str = "kj_projects";
const DOCUMENTS: &str = "kj_documents";
const WRITER: AuthorSubject = AuthorSubject::SYSTEM;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let keyed = std::env::var("JAZZ_KEYED_JOINS").map_or(true, |value| value != "off");
    let projects = env_usize("JAZZ_KJ_PROJECTS", 20_000);
    let routes = env_usize("JAZZ_KJ_ROUTES", 100);
    let docs_per_route = env_usize("JAZZ_KJ_DOCS_PER_ROUTE", 10);
    let shared = env_usize("JAZZ_KJ_SHARED", 0) != 0;
    assert!(routes * docs_per_route <= projects || shared);

    let db = open_db();
    db.set_keyed_joins_enabled_for_test(keyed);
    let seed_started = Instant::now();
    for project in 0..projects {
        insert_project(&db, project, "seeded");
    }
    for route in 0..routes {
        for doc in 0..docs_per_route {
            let target = if shared {
                doc
            } else {
                route * docs_per_route + doc
            };
            insert_document(&db, route, doc, target);
        }
    }
    let seed_us = micros(seed_started.elapsed());
    let rss_after_seed = proc_status_kib("VmRSS:");

    let query = Query::from(DOCUMENTS)
        .filter(eq(col("team"), param("team")))
        .include("project");
    let mut streams = Vec::with_capacity(routes);
    let mut subscribe = Vec::with_capacity(routes);
    let mut initial_rows = 0;
    for route in 0..routes {
        let started = Instant::now();
        let prepared = db
            .prepare_query_bound(
                &query,
                BTreeMap::from([("team".to_owned(), Value::Uuid(team_row(route).0))]),
            )
            .expect("prepare include binding");
        let mut stream =
            block_on(db.subscribe(&prepared, local_opts())).expect("subscribe include");
        initial_rows += take_initial_rows(&mut stream);
        subscribe.push(started.elapsed());
        streams.push(stream);
    }
    assert_eq!(initial_rows, routes * docs_per_route);
    let rss_after_hydration = proc_status_kib("VmRSS:");
    let stats = db.runtime_stats_for_test();

    // An included project changes: every subscriber that includes it hears.
    let started = Instant::now();
    insert_project(&db, 0, "renamed");
    let included_write_us = micros(started.elapsed());
    let included_events = drain(&mut streams);
    // A project nobody includes changes: nobody hears.
    let started = Instant::now();
    insert_project(&db, projects - 1, "renamed");
    let unrelated_write_us = micros(started.elapsed());
    let unrelated_events = drain(&mut streams);
    // A new document joins an existing project.
    let started = Instant::now();
    insert_document(&db, 0, docs_per_route, docs_per_route);
    let new_doc_write_us = micros(started.elapsed());
    let new_doc_events = drain(&mut streams);
    let expected_included = if shared { routes } else { 1 };
    let expected_unrelated = usize::from(!shared && routes * docs_per_route == projects);

    let ok = included_events == expected_included
        && unrelated_events == expected_unrelated
        && new_doc_events == 1;
    println!(
        "{}",
        serde_json::json!({
            "scenario": "keyed_join_memory",
            "keyed_joins": keyed,
            "projects": projects,
            "routes": routes,
            "docs_per_route": docs_per_route,
            "shared_targets": shared,
            "seed_us": seed_us,
            "subscribe_total_us": subscribe.iter().map(|d| micros(*d)).sum::<u64>(),
            "subscribe_p50_us": percentile(&subscribe, 50),
            "subscribe_p95_us": percentile(&subscribe, 95),
            "included_write_us": included_write_us,
            "unrelated_write_us": unrelated_write_us,
            "new_doc_write_us": new_doc_write_us,
            "keyed_join_inputs": stats.keyed_join_inputs,
            "arrangement_count": stats.arrangement_count,
            "arrangement_rows": stats.arrangement_rows,
            "arrangement_encoded_bytes": stats.arrangement_encoded_bytes,
            "lookup_count": stats.lookup_count,
            "lookup_keys": stats.lookup_keys,
            "lookup_rows": stats.lookup_rows,
            "lookup_encoded_bytes": stats.lookup_encoded_bytes,
            "graph_nodes": stats.graph_nodes,
            "rss_kib_after_seed": rss_after_seed,
            "rss_kib_after_hydration": rss_after_hydration,
            "peak_rss_kib": proc_status_kib("VmHWM:"),
            "events": [included_events, unrelated_events, new_doc_events],
            "ok": ok,
        })
    );
    assert!(ok, "keyed join receipt delivered unexpected events");
}

fn open_db() -> Db {
    let schema = schema_fixture::compile(
        SchemaBuilder::new()
            .table(TableSchemaBuilder::new(PROJECTS).column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new(DOCUMENTS)
                    .column("team", ColumnType::Uuid)
                    .fk_column("project", PROJECTS)
                    .column("title", ColumnType::Text),
            ),
    );
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&family_refs).expect("valid memory storage families"),
            DbIdentity {
                node: NodeUuid::from_bytes(0x4b4a_u128.to_be_bytes()),
                author: WRITER,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x4b4a)),
    ))
    .expect("open keyed join db")
}

fn insert_project(db: &Db, project: usize, name: &str) {
    let row = project_row(project);
    let values = BTreeMap::from([(
        "name".to_owned(),
        Value::String(format!("{name} project {project}")),
    )]);
    let exists = block_on(db.update(PROJECTS, row, values.clone(), Default::default())).is_ok();
    if !exists {
        block_on(db.insert(
            PROJECTS,
            values,
            jazz::db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ))
        .expect("insert project");
    }
}

fn insert_document(db: &Db, route: usize, doc: usize, project: usize) {
    block_on(db.insert(
        DOCUMENTS,
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team_row(route).0)),
            ("project".to_owned(), Value::Uuid(project_row(project).0)),
            (
                "title".to_owned(),
                Value::String(format!("team {route} document {doc}")),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(tagged_row(0x4b4c + route as u64, doc as u64)),
            ..Default::default()
        },
    ))
    .expect("insert document");
}

fn team_row(route: usize) -> RowUuid {
    tagged_row(0x4b4b, route as u64)
}

fn project_row(project: usize) -> RowUuid {
    tagged_row(0x4b4d, project as u64)
}

fn tagged_row(namespace: u64, value: u64) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&namespace.to_be_bytes());
    bytes[8..].copy_from_slice(&value.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn local_opts() -> ReadOpts {
    ReadOpts {
        tier: jazz::db::ReadTier::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn take_initial_rows(stream: &mut SubscriptionStream) -> usize {
    match stream
        .try_next_event()
        .expect("include subscription did not emit an initial reset")
    {
        SubscriptionEvent::Delta {
            reset: true, added, ..
        } => added.len(),
        other => panic!("expected initial reset, got {other:?}"),
    }
}

/// How many subscribers heard anything.
fn drain(streams: &mut [SubscriptionStream]) -> usize {
    streams
        .iter_mut()
        .map(|stream| {
            let mut heard = false;
            while let Some(event) = stream.try_next_event() {
                match event {
                    SubscriptionEvent::Delta { .. } => heard = true,
                    other => panic!("unexpected subscription event {other:?}"),
                }
            }
            heard
        })
        .filter(|heard| *heard)
        .count()
}

fn percentile(samples: &[Duration], percent: usize) -> u64 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted
        .get((sorted.len().saturating_sub(1)) * percent / 100)
        .map_or(0, |duration| micros(*duration))
}

fn micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn proc_status_kib(label: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix(label))
        .and_then(|value| value.split_whitespace().next()?.parse().ok())
}
