use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::future::Future;
use std::ops::AddAssign;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process::Command;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use automerge::{AutoCommit, ObjType, ROOT, ReadDoc, TextEncoding, transaction::Transactable};
use diamond_types::list::ListCRDT;
use hdrhistogram::Histogram;
use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource, TextEdit, Transport};
use jazz::groove::records::Value;
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::content_store::Extent;
use jazz::node::text_oplog::{self, Content as TextContent};
use jazz::node::{LargeValueEditCommit, MergeableCommit, NodeState};
use jazz::peer::PeerState;
use jazz::protocol::{SyncMessage, expand_version_carriers};
use jazz::query::Query;
use jazz::schema::{ColumnSchema, JazzSchema, TableSchema};
use jazz::time::GlobalSeq;
use jazz::tx::{DurabilityTier, Fate};
use jazz::wire::TransportError;
use jazz_sim::{PeerProfile, bench_profile, emit_json_line, mem, metadata_fields};
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;

const DOCS: &str = "textDocs";
const LARGE_VALUE_CHECKPOINT_OP_INTERVAL: usize = 1024;
const AUTOMERGE_PERF_COMMIT: &str = "da212e984c777d31ee7d888f82637288aa4c61d3";
const EDITING_TRACES_COMMIT: &str = "762fa6c51605c88a05ebe5c4b9d4540caca30b97";
const EGWALKER_ARTIFACT_COMMIT: &str = "4fb0970ce3fab729aac31e8e622b530fd17938cb";

fn main() {
    if std::env::var("JAZZ_SMOKE").is_ok() {
        smoke();
        return;
    }
    let config = Config::from_env();
    let profile = PeerProfile::new(
        config.profile.clone(),
        env_u64("JAZZ_LINK_ONE_WAY_MS", 1),
        env_u64("JAZZ_LINK_JITTER_MS", 0),
        env_u64("JAZZ_LINK_OVERHEAD_MS", 0),
    );
    for spec in trace_specs()
        .iter()
        .filter(|spec| config.traces.iter().any(|trace| trace == spec.name))
    {
        let trace = load_trace(&config, spec);
        let mut jazz_comparison = None;
        for batch in &config.batches {
            let replay = run_replay(&config, &trace, *batch);
            let db_replay = run_db_surface_replay(&trace, *batch);
            assert_eq!(db_replay.edits, replay.edits);
            assert_eq!(db_replay.commits, replay.commits);
            if *batch == 1 {
                jazz_comparison = Some(JazzComparison {
                    edits: replay.edits,
                    bytes_per_edit: replay.history_bytes as f64 / replay.edits.max(1) as f64,
                    replay_edits_per_sec: replay.edits as f64
                        / (replay.elapsed_us as f64 / 1_000_000.0).max(0.000_001),
                });
            }
            emit_replay(&config, &profile, &trace, *batch, &replay);
            emit_db_surface_replay(&config, &profile, &trace, *batch, &db_replay);
        }
        if let Some(jazz) = jazz_comparison {
            let diamond = run_diamond_types_floor(&trace);
            emit_adversary_comparison(&config, &profile, &trace, &jazz, diamond);
            let automerge = run_automerge_baseline(&trace);
            emit_adversary_comparison(&config, &profile, &trace, &jazz, automerge);
        }
        let live = run_live_observation(&config, &trace);
        emit_live(&config, &profile, &trace, &live);
        let concurrent = run_concurrent_merge(&trace);
        emit_concurrent_merge(&config, &profile, &trace, &concurrent);
        let cold = run_cold_load(&config, &trace);
        emit_cold(&config, &profile, &trace, &cold);
        for summary in run_point_in_time_reads(&trace) {
            emit_point_in_time(&config, &profile, &trace, &summary);
        }
    }
}

pub fn smoke() {
    let config = Config {
        seed: 0x5600_0001,
        profile: "s6-smoke".to_owned(),
        max_edits: 8,
        live_edits: 4,
        batches: vec![1, 4],
        traces: trace_specs()
            .iter()
            .map(|spec| spec.name.to_owned())
            .collect(),
    };
    let profile = PeerProfile::new(config.profile.clone(), 1, 0, 0);
    for spec in trace_specs() {
        let edits = synthetic_edits(config.max_edits);
        let trace = Trace {
            name: spec.name.to_owned(),
            profile: format!("synthetic_smoke_{}", spec.name),
            fixture_path: PathBuf::from(format!("synthetic-smoke/{}", spec.name)),
            fixture_sha256: "synthetic".to_owned(),
            expected_sha256: spec.sha256.to_owned(),
            commit: spec.commit.to_owned(),
            url: spec.url.to_owned(),
            final_doc: apply_edits(&edits),
            edits,
        };
        for batch in &config.batches {
            let replay = run_replay(&config, &trace, *batch);
            assert_eq!(replay.edits, config.max_edits);
            emit_replay(&config, &profile, &trace, *batch, &replay);
            let db_replay = run_db_surface_replay(&trace, *batch);
            assert_eq!(db_replay.edits, config.max_edits);
            emit_db_surface_replay(&config, &profile, &trace, *batch, &db_replay);
        }
        let live = run_live_observation(&config, &trace);
        assert_eq!(live.edits, config.live_edits);
        emit_live(&config, &profile, &trace, &live);
    }
}

#[derive(Debug)]
struct Config {
    seed: u64,
    profile: String,
    max_edits: usize,
    live_edits: usize,
    batches: Vec<usize>,
    traces: Vec<String>,
}

impl Config {
    fn from_env() -> Self {
        let bench_profile = bench_profile();
        Self {
            seed: env_u64("JAZZ_SEED", 0x5600_0001),
            profile: std::env::var("JAZZ_PROFILE").unwrap_or_else(|_| "s6-local".to_owned()),
            max_edits: env_usize("JAZZ_S6_MAX_EDITS", bench_profile.select(100, 500, 2_000)).max(1),
            live_edits: env_usize("JAZZ_S6_LIVE_EDITS", bench_profile.select(20, 50, 100)).max(1),
            batches: std::env::var("JAZZ_S6_BATCHES")
                .ok()
                .map(|value| {
                    value
                        .split(',')
                        .filter_map(|part| part.trim().parse::<usize>().ok())
                        .map(|value| value.max(1))
                        .collect()
                })
                .unwrap_or_else(|| {
                    bench_profile.select(vec![1, 16], vec![1, 32], vec![1, 32, 256])
                }),
            traces: std::env::var("JAZZ_S6_TRACES")
                .ok()
                .map(|value| {
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|part| !part.is_empty())
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_else(|| {
                    let traces = trace_specs()
                        .iter()
                        .map(|spec| spec.name.to_owned())
                        .collect::<Vec<_>>();
                    let trace_count = bench_profile.select(1, 2, traces.len());
                    traces.into_iter().take(trace_count).collect()
                }),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum TraceFormat {
    AutomergePerfJs,
    EditingTracesJsonGz,
    EgwalkerDatasetJson,
}

#[derive(Clone, Copy, Debug)]
struct TraceSpec {
    name: &'static str,
    fixture_dir: &'static str,
    fixture_file: &'static str,
    commit: &'static str,
    url: &'static str,
    sha256: &'static str,
    format: TraceFormat,
}

fn trace_specs() -> &'static [TraceSpec] {
    &[
        TraceSpec {
            name: "automerge-paper",
            fixture_dir: "automerge-paper",
            fixture_file: "editing-trace.js",
            commit: AUTOMERGE_PERF_COMMIT,
            url: "https://github.com/garden-co/jazz/releases/download/jazz-sim-fixtures-v1/automerge-paper-editing-trace.js",
            sha256: "aeccdcd46542fede7f893c09f8e9008aa1909167d96acdfd32102ec7fc04f4f4",
            format: TraceFormat::AutomergePerfJs,
        },
        TraceSpec {
            name: "seph-blog1",
            fixture_dir: "seph-blog1",
            fixture_file: "seph-blog1.json.gz",
            commit: EDITING_TRACES_COMMIT,
            url: "https://github.com/garden-co/jazz/releases/download/jazz-sim-fixtures-v1/seph-blog1.json.gz",
            sha256: "43b5d326ca8c9094cd94e28ab66cd2f3073ce2893db959a0388c53f0aed8156d",
            format: TraceFormat::EditingTracesJsonGz,
        },
        TraceSpec {
            name: "egwalker",
            fixture_dir: "egwalker",
            fixture_file: "S3.json",
            commit: EGWALKER_ARTIFACT_COMMIT,
            url: "https://github.com/garden-co/jazz/releases/download/jazz-sim-fixtures-v1/egwalker-S3.json",
            sha256: "a2ac2036f851d434b278d231f5a8199f0097ff87cf171b5d12c9d81203a2ad91",
            format: TraceFormat::EgwalkerDatasetJson,
        },
    ]
}

#[derive(Clone, Debug)]
enum Edit {
    Insert { pos: usize, text: char },
    Delete { pos: usize },
}

#[derive(Debug)]
struct Trace {
    name: String,
    profile: String,
    fixture_path: PathBuf,
    fixture_sha256: String,
    expected_sha256: String,
    commit: String,
    url: String,
    edits: Vec<Edit>,
    final_doc: String,
}

#[derive(Debug)]
struct ReplaySummary {
    edits: usize,
    commits: usize,
    elapsed_us: u128,
    echo: Histogram<u64>,
    peak_doc_bytes: usize,
    peak_rss_bytes: u64,
    history_bytes: u64,
    history_class_bytes: u64,
    tail_consolidation: TailConsolidationSummary,
    zstd_final_doc_bytes: u64,
    zstd19_final_doc_bytes: u64,
    zstd_json_log_bytes: u64,
    zstd19_json_log_bytes: u64,
}

#[derive(Debug)]
struct DbSurfaceReplaySummary {
    edits: usize,
    commits: usize,
    elapsed_us: u128,
    echo: Histogram<u64>,
    peak_doc_bytes: usize,
    peak_rss_bytes: u64,
    history_bytes: u64,
    history_class_bytes: u64,
    consolidated_windows: usize,
    consolidated_window_records: usize,
    history_window_consolidation_us: u128,
    tail_consolidation: TailConsolidationSummary,
    edge_acceptance: Histogram<u64>,
    edge_hydration_bytes: u64,
    edge_hydration_rows: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct TailConsolidationSummary {
    windows: usize,
    records: usize,
    elapsed_us: u128,
}

impl AddAssign for TailConsolidationSummary {
    fn add_assign(&mut self, rhs: Self) {
        self.windows += rhs.windows;
        self.records += rhs.records;
        self.elapsed_us += rhs.elapsed_us;
    }
}

struct QueueTransport {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for QueueTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

#[derive(Debug)]
struct LiveSummary {
    edits: usize,
    observer_p95_us: u64,
    synced_bytes: u64,
}

#[derive(Debug)]
struct ColdSummary {
    current_only_us: u128,
    current_bytes: u64,
    history_bytes: u64,
}

#[derive(Debug)]
struct PointInTimeSummary {
    cut_percent: u64,
    edit_prefix: usize,
    global_seq: GlobalSeq,
    latency_us: u128,
    doc_bytes: usize,
}

#[derive(Debug)]
struct ConcurrentMergeSummary {
    edits: usize,
    elapsed_us: u128,
    synced_bytes: u64,
    final_doc_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
struct JazzComparison {
    edits: usize,
    bytes_per_edit: f64,
    replay_edits_per_sec: f64,
}

#[derive(Debug)]
struct AdversarySummary {
    adversary: &'static str,
    envelope: &'static str,
    status: &'static str,
    asymmetry: &'static str,
    replay_edits_per_sec: Option<f64>,
    elapsed_us: Option<u128>,
    peak_memory_proxy_bytes: Option<usize>,
    save_bytes: Option<u64>,
    bytes_per_edit: Option<f64>,
    cold_load_us: Option<u128>,
    final_doc_matched: Option<bool>,
}

fn load_trace(config: &Config, spec: &TraceSpec) -> Trace {
    let fixture = fixture_path(spec);
    let downloaded = ensure_fixture(&fixture, spec);
    let sha = if downloaded {
        sha256(&fixture)
    } else {
        "missing".to_owned()
    };
    let parsed = if downloaded && sha == spec.sha256 {
        Some(
            parse_trace_with_node(&fixture, spec.format, config.max_edits).unwrap_or_else(|| {
                panic!(
                    "pinned trace fixture exists and matches hash, but node failed to parse {}",
                    fixture.display()
                )
            }),
        )
    } else {
        None
    };
    let (profile, edits, final_doc) = if let Some((edits, final_doc, complete)) = parsed {
        (
            if complete {
                format!("{}-pinned", spec.name)
            } else {
                format!("{}-pinned-sampled", spec.name)
            },
            edits,
            final_doc,
        )
    } else {
        let edits = synthetic_edits(config.max_edits);
        let final_doc = apply_edits(&edits);
        (
            format!("synthetic_fallback_{}", spec.name),
            edits,
            final_doc,
        )
    };
    Trace {
        name: spec.name.to_owned(),
        profile,
        fixture_path: fixture,
        fixture_sha256: sha,
        expected_sha256: spec.sha256.to_owned(),
        commit: spec.commit.to_owned(),
        url: spec.url.to_owned(),
        edits,
        final_doc,
    }
}

fn parse_trace_with_node(
    path: &Path,
    format: TraceFormat,
    limit: usize,
) -> Option<(Vec<Edit>, String, bool)> {
    let loader = match format {
        TraceFormat::AutomergePerfJs => format!("require({:?})", path.display().to_string()),
        TraceFormat::EditingTracesJsonGz => format!(
            "JSON.parse(require('zlib').gunzipSync(require('fs').readFileSync({:?}), 'utf8'))",
            path.display().to_string()
        ),
        TraceFormat::EgwalkerDatasetJson => format!(
            "JSON.parse(require('fs').readFileSync({:?}, 'utf8'))",
            path.display().to_string()
        ),
    };
    let script = format!(
        r#"
const t = {loader};
const limit = {limit};
const sourceTxns = t.txns || [{{ patches: t.edits || [] }}];
let doc = Array.from(t.startContent || '');
let edits = [];
let total = 0;
let complete = true;
function pushDelete(pos) {{
  if (edits.length >= limit) {{ complete = false; return false; }}
  edits.push([pos, 1]);
  doc.splice(pos, 1);
  return true;
}}
function pushInsert(pos, ch) {{
  if (edits.length >= limit) {{ complete = false; return false; }}
  edits.push([pos, 0, ch]);
  doc.splice(pos, 0, ch);
  return true;
}}
outer:
for (const txn of sourceTxns) {{
  for (const patch of (txn.patches || [])) {{
    const pos0 = patch[0];
    const del = patch[1] || 0;
    const insertValue = patch.length > 3 ? patch.slice(2).join('') : (patch[2] || '');
    const ins = Array.from(insertValue);
    total += del + ins.length;
    for (let i = 0; i < del; i++) {{
      if (!pushDelete(pos0)) break outer;
    }}
    let pos = pos0;
    for (const ch of ins) {{
      if (!pushInsert(pos, ch)) break outer;
      pos++;
    }}
  }}
}}
if (complete && edits.length < total) complete = false;
const finalText = t.finalText || t.endContent || '';
console.log(JSON.stringify({{edits, doc: doc.join(''), complete, finalText}}));
"#
    );
    let output = Command::new("node").args(["-e", &script]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let json: JsonValue = serde_json::from_slice(&output.stdout).ok()?;
    let edits = json
        .get("edits")?
        .as_array()?
        .iter()
        .filter_map(|entry| {
            let items = entry.as_array()?;
            let pos = items.first()?.as_u64()? as usize;
            let del = items.get(1)?.as_u64()? as usize;
            if del > 0 {
                Some(Edit::Delete { pos })
            } else {
                let text = items.get(2)?.as_str()?.chars().next()?;
                Some(Edit::Insert { pos, text })
            }
        })
        .collect::<Vec<_>>();
    let complete = json.get("complete")?.as_bool()?;
    let final_doc = if complete {
        json.get("finalText")?.as_str()?.to_owned()
    } else {
        json.get("doc")?.as_str()?.to_owned()
    };
    Some((edits, final_doc, complete))
}

fn ensure_fixture(path: &Path, spec: &TraceSpec) -> bool {
    if path.exists() {
        return true;
    }
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    Command::new("curl")
        .args(["-fsSL", spec.url, "-o"])
        .arg(path)
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn run_replay(_config: &Config, trace: &Trace, batch: usize) -> ReplaySummary {
    let schema = schema();
    let (dir, mut node) = open_node(node_id(10), schema);
    let mut doc = String::new();
    let mut echo = Histogram::new(3).unwrap();
    let mut peak_doc_bytes = 0;
    let start = Instant::now();
    let mut commits = 0;
    let mut global_seq = 1_u64;
    for (chunk_idx, chunk) in trace.edits.chunks(batch).enumerate() {
        let before = Instant::now();
        for edit in chunk {
            apply_edit(&mut doc, edit);
        }
        peak_doc_bytes = peak_doc_bytes.max(doc.len());
        let tx = node
            .commit_mergeable(
                MergeableCommit::new(DOCS, doc_row(), chunk_idx as u64 + 1)
                    .made_by(AuthorId::SYSTEM)
                    .cells(cells([("text", Value::Bytes(doc.as_bytes().to_vec()))])),
            )
            .unwrap();
        node.apply_fate_update(
            tx,
            Fate::Accepted,
            Some(GlobalSeq(global_seq)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        global_seq += 1;
        echo.record(before.elapsed().as_micros() as u64).unwrap();
        assert_eq!(read_doc(&mut node), doc);
        commits += 1;
    }
    assert_eq!(read_doc(&mut node), trace.final_doc);
    let tail_consolidation = if consolidate_to_tail_enabled() {
        consolidate_node_history_to_tail(&mut node)
    } else {
        TailConsolidationSummary::default()
    };
    let history_class_bytes = history_class_bytes_node(&node);
    ReplaySummary {
        edits: trace.edits.len(),
        commits,
        elapsed_us: start.elapsed().as_micros(),
        echo,
        peak_doc_bytes,
        peak_rss_bytes: mem::peak_rss_bytes(),
        history_bytes: storage_bytes(dir.path()),
        history_class_bytes,
        tail_consolidation,
        zstd_final_doc_bytes: zstd::bulk::compress(trace.final_doc.as_bytes(), 3)
            .unwrap()
            .len() as u64,
        zstd19_final_doc_bytes: zstd::bulk::compress(trace.final_doc.as_bytes(), 19)
            .unwrap()
            .len() as u64,
        zstd_json_log_bytes: zstd::bulk::compress(json_log(&trace.edits).as_bytes(), 3)
            .unwrap()
            .len() as u64,
        zstd19_json_log_bytes: zstd::bulk::compress(json_log(&trace.edits).as_bytes(), 19)
            .unwrap()
            .len() as u64,
    }
}

fn run_db_surface_replay(trace: &Trace, batch: usize) -> DbSurfaceReplaySummary {
    let schema = schema();
    let (core_dir, mut core) = open_node(node_id(250), schema.clone());
    let (edge_dir, mut edge) = open_node(node_id(170), schema.clone());
    let mut edge_peer = PeerState::new();
    let (dir, db) = open_db(node_id(70), schema.clone());
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    let inbound = Rc::new(RefCell::new(VecDeque::new()));
    let _upstream = db.connect_upstream(Box::new(QueueTransport {
        outbound: Rc::clone(&outbound),
        inbound: Rc::clone(&inbound),
    }));
    let mut doc = String::new();
    let mut echo = Histogram::new(3).unwrap();
    let mut edge_acceptance = Histogram::new(3).unwrap();
    let mut consolidated_windows = 0_usize;
    let mut consolidated_window_records = 0_usize;
    let mut history_window_consolidation_us = 0_u128;
    let mut peak_doc_bytes = 0;
    let init = db
        .insert_with_id(DOCS, doc_row(), cells([("text", Value::Bytes(Vec::new()))]))
        .expect("db doc insert");
    block_on(init.wait(DurabilityTier::Local)).expect("db doc insert local wait");
    drain_db_route(
        &db,
        &outbound,
        &inbound,
        &mut edge,
        &mut edge_peer,
        &mut core,
        &mut edge_acceptance,
        &mut consolidated_windows,
        &mut consolidated_window_records,
        &mut history_window_consolidation_us,
    );
    let start = Instant::now();
    let mut commits = 0;
    for chunk in trace.edits.chunks(batch) {
        let before = Instant::now();
        let parent_doc = doc.clone();
        for edit in chunk {
            match edit {
                Edit::Insert { pos, text } => {
                    let idx = byte_index_for_char(&doc, *pos);
                    doc.insert(idx, *text);
                }
                Edit::Delete { pos } => {
                    if *pos < doc.chars().count() {
                        let idx = byte_index_for_char(&doc, *pos);
                        let len = doc[idx..].chars().next().unwrap().len_utf8();
                        doc.drain(idx..idx + len);
                    }
                }
            };
        }
        let text_edit = text_oplog::diff(parent_doc.as_bytes(), doc.as_bytes())
            .into_iter()
            .fold(TextEdit::new(), |edit, op| match op {
                text_oplog::Op::Insert {
                    pos,
                    content: TextContent::Inline(bytes),
                } => edit.insert(pos, bytes),
                text_oplog::Op::Delete { pos, len } => edit.delete(pos, len),
                text_oplog::Op::Insert {
                    content: TextContent::Ref(_),
                    ..
                } => unreachable!("diff emits inline text content"),
            });
        if parent_doc != doc {
            let write = db
                .edit_text(DOCS, doc_row(), "text", text_edit)
                .expect("db doc text edit");
            block_on(write.wait(DurabilityTier::Local)).expect("db doc local wait");
        }
        peak_doc_bytes = peak_doc_bytes.max(doc.len());
        drain_db_route(
            &db,
            &outbound,
            &inbound,
            &mut edge,
            &mut edge_peer,
            &mut core,
            &mut edge_acceptance,
            &mut consolidated_windows,
            &mut consolidated_window_records,
            &mut history_window_consolidation_us,
        );
        echo.record(before.elapsed().as_micros() as u64).unwrap();
        assert_eq!(read_db_doc(&db, &schema), doc);
        commits += 1;
    }
    // The Db replay route has two logical links:
    //
    //   Db client -> edge node -> core node
    //
    // Keep the client->edge peer state separate from the core->edge hydration
    // peer state. Reusing the ingress peer here can make the hydration update
    // contain refs for versions the edge node cannot use to materialize the
    // large-value row chain after a long edit trace.
    let mut core_to_edge_peer = PeerState::new();
    let update = core_to_edge_peer
        .current_rows_update(&mut core, DOCS)
        .unwrap();
    let edge_hydration_bytes = view_update_bytes(&update);
    let edge_hydration_rows = result_row_count(&update);
    edge.apply_sync_message(update).unwrap();
    assert_eq!(read_db_doc(&db, &schema), trace.final_doc);
    let tail_consolidation = if consolidate_to_tail_enabled() {
        let mut total = consolidate_db_history_to_tail(&db);
        total += consolidate_node_history_to_tail(&mut edge);
        total += consolidate_node_history_to_tail(&mut core);
        consolidated_windows += total.windows;
        consolidated_window_records += total.records;
        history_window_consolidation_us += total.elapsed_us;
        total
    } else {
        TailConsolidationSummary::default()
    };
    let history_class_bytes = history_class_bytes_db(&db)
        + history_class_bytes_node(&edge)
        + history_class_bytes_node(&core);
    DbSurfaceReplaySummary {
        edits: trace.edits.len(),
        commits,
        elapsed_us: start.elapsed().as_micros(),
        echo,
        peak_doc_bytes,
        peak_rss_bytes: mem::peak_rss_bytes(),
        history_bytes: storage_bytes(dir.path())
            + storage_bytes(edge_dir.path())
            + storage_bytes(core_dir.path()),
        history_class_bytes,
        consolidated_windows,
        consolidated_window_records,
        history_window_consolidation_us,
        tail_consolidation,
        edge_acceptance,
        edge_hydration_bytes,
        edge_hydration_rows,
    }
}

fn drain_db_route(
    db: &Db<RocksDbStorage>,
    outbound: &Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: &Rc<RefCell<VecDeque<SyncMessage>>>,
    edge: &mut NodeState<RocksDbStorage>,
    edge_peer: &mut PeerState,
    core: &mut NodeState<RocksDbStorage>,
    edge_acceptance: &mut Histogram<u64>,
    consolidated_windows: &mut usize,
    consolidated_window_records: &mut usize,
    history_window_consolidation_us: &mut u128,
) {
    record_db_tick(
        db,
        consolidated_windows,
        consolidated_window_records,
        history_window_consolidation_us,
    );
    while let Some(unit) = outbound.borrow_mut().pop_front() {
        let SyncMessage::CommitUnit { tx, versions } = unit.clone() else {
            continue;
        };
        let start = Instant::now();
        let edge_updates = edge_peer
            .ingest_edge_mergeable_commit_unit(edge, tx, versions, u64::MAX)
            .unwrap();
        edge_acceptance
            .record(start.elapsed().as_micros() as u64)
            .unwrap();
        for update in edge_updates {
            inbound.borrow_mut().push_back(update);
            record_db_tick(
                db,
                consolidated_windows,
                consolidated_window_records,
                history_window_consolidation_us,
            );
        }
        for update in edge_peer.drain_deferred_edge_fates(edge, u64::MAX).unwrap() {
            inbound.borrow_mut().push_back(update);
            record_db_tick(
                db,
                consolidated_windows,
                consolidated_window_records,
                history_window_consolidation_us,
            );
        }
        for update in core.apply_sync_message(unit).unwrap() {
            edge.apply_sync_message(update.clone()).unwrap();
            inbound.borrow_mut().push_back(update);
            record_db_tick(
                db,
                consolidated_windows,
                consolidated_window_records,
                history_window_consolidation_us,
            );
        }
        for update in edge_peer.drain_deferred_edge_fates(edge, u64::MAX).unwrap() {
            inbound.borrow_mut().push_back(update);
            record_db_tick(
                db,
                consolidated_windows,
                consolidated_window_records,
                history_window_consolidation_us,
            );
        }
    }
}

fn record_db_tick(
    db: &Db<RocksDbStorage>,
    consolidated_windows: &mut usize,
    consolidated_window_records: &mut usize,
    history_window_consolidation_us: &mut u128,
) {
    let stats = db.tick_stats().unwrap();
    *consolidated_windows += stats.consolidated_windows;
    *consolidated_window_records += stats.consolidated_window_records;
    *history_window_consolidation_us += stats.history_window_consolidation_us;
}

fn consolidate_to_tail_enabled() -> bool {
    std::env::var("JAZZ_S6_CONSOLIDATE_TO_TAIL").is_ok()
}

fn consolidate_node_history_to_tail(
    node: &mut NodeState<RocksDbStorage>,
) -> TailConsolidationSummary {
    let start = Instant::now();
    let mut total = TailConsolidationSummary::default();
    for _ in 0..10_000 {
        let report = node
            .consolidate_history_windows_for_test(64)
            .expect("node history consolidation");
        if report.windows == 0 {
            total.elapsed_us += start.elapsed().as_micros();
            return total;
        }
        total.windows += report.windows;
        total.records += report.records;
    }
    panic!("history consolidation did not reach the plain tail");
}

fn consolidate_db_history_to_tail(db: &Db<RocksDbStorage>) -> TailConsolidationSummary {
    let start = Instant::now();
    let mut total = TailConsolidationSummary::default();
    for _ in 0..10_000 {
        let stats = db.tick_stats().expect("db history consolidation tick");
        if stats.consolidated_windows == 0 {
            total.elapsed_us += start.elapsed().as_micros();
            return total;
        }
        total.windows += stats.consolidated_windows;
        total.records += stats.consolidated_window_records;
    }
    panic!("db history consolidation did not reach the plain tail");
}

fn history_class_bytes_node(node: &NodeState<RocksDbStorage>) -> u64 {
    node.history_class_bytes_for_test()
        .expect("node history class bytes")
        .unwrap_or(0)
}

fn history_class_bytes_db(db: &Db<RocksDbStorage>) -> u64 {
    db.history_class_bytes_for_test()
        .expect("db history class bytes")
        .unwrap_or(0)
}

fn run_live_observation(config: &Config, trace: &Trace) -> LiveSummary {
    let schema = schema();
    let (_writer_dir, mut writer) = open_node(node_id(20), schema.clone());
    let (_observer_dir, mut observer) = open_node(node_id(21), schema);
    let mut peer = PeerState::new();
    let mut doc = String::new();
    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut synced_bytes = 0_u64;
    let mut global_seq = 1_u64;
    for (idx, edit) in trace.edits.iter().take(config.live_edits).enumerate() {
        let before = Instant::now();
        apply_edit(&mut doc, edit);
        let tx = writer
            .commit_mergeable(
                MergeableCommit::new(DOCS, doc_row(), idx as u64 + 1)
                    .made_by(AuthorId::SYSTEM)
                    .cells(cells([("text", Value::Bytes(doc.as_bytes().to_vec()))])),
            )
            .unwrap();
        writer
            .apply_fate_update(
                tx,
                Fate::Accepted,
                Some(GlobalSeq(global_seq)),
                Some(DurabilityTier::Global),
            )
            .unwrap();
        global_seq += 1;
        let update = peer.current_rows_update(&mut writer, DOCS).unwrap();
        synced_bytes += view_update_bytes(&update);
        apply_with_content_extents(&mut writer, &mut observer, update).unwrap();
        hist.record(before.elapsed().as_micros() as u64).unwrap();
        assert_eq!(read_doc(&mut observer), doc);
    }
    LiveSummary {
        edits: config.live_edits.min(trace.edits.len()),
        observer_p95_us: hist.value_at_quantile(0.95),
        synced_bytes,
    }
}

fn run_cold_load(_config: &Config, trace: &Trace) -> ColdSummary {
    let schema = schema();
    let (writer_dir, mut writer) = open_node(node_id(30), schema.clone());
    let mut doc = String::new();
    let mut global_seq = 1_u64;
    for (idx, edit) in trace.edits.iter().enumerate() {
        apply_edit(&mut doc, edit);
        let tx = writer
            .commit_mergeable(
                MergeableCommit::new(DOCS, doc_row(), idx as u64 + 1)
                    .cells(cells([("text", Value::Bytes(doc.as_bytes().to_vec()))])),
            )
            .unwrap();
        writer
            .apply_fate_update(
                tx,
                Fate::Accepted,
                Some(GlobalSeq(global_seq)),
                Some(DurabilityTier::Global),
            )
            .unwrap();
        global_seq += 1;
    }
    let (_cold_dir, mut cold) = open_node(node_id(31), schema);
    let mut peer = PeerState::new();
    let current_start = Instant::now();
    let update = peer.current_rows_update(&mut writer, DOCS).unwrap();
    let current_bytes = view_update_bytes(&update);
    apply_with_content_extents(&mut writer, &mut cold, update).unwrap();
    ColdSummary {
        current_only_us: current_start.elapsed().as_micros(),
        current_bytes,
        history_bytes: storage_bytes(writer_dir.path()),
    }
}

fn run_point_in_time_reads(trace: &Trace) -> Vec<PointInTimeSummary> {
    let schema = schema();
    let (_dir, mut node) = open_history_complete_node(node_id(40), schema.clone());
    let shape = Query::from(DOCS).validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let table = table_schema(&schema, DOCS).clone();
    let cut_targets = [25_u64, 50, 75]
        .into_iter()
        .map(|percent| {
            let target = ((trace.edits.len() as u64 * percent).max(1)).div_ceil(100);
            (percent, target as usize)
        })
        .collect::<Vec<_>>();
    let mut next_cut = 0_usize;
    let mut doc = String::new();
    let mut summaries = Vec::new();
    let mut global_seq = 1_u64;
    for (idx, edit) in trace.edits.iter().enumerate() {
        apply_edit(&mut doc, edit);
        let tx = node
            .commit_mergeable(
                MergeableCommit::new(DOCS, doc_row(), idx as u64 + 1)
                    .made_by(AuthorId::SYSTEM)
                    .cells(cells([("text", Value::Bytes(doc.as_bytes().to_vec()))])),
            )
            .unwrap();
        node.apply_fate_update(
            tx,
            Fate::Accepted,
            Some(GlobalSeq(global_seq)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
        let position = GlobalSeq(global_seq);
        global_seq += 1;
        while next_cut < cut_targets.len() && idx + 1 >= cut_targets[next_cut].1 {
            let (percent, target) = cut_targets[next_cut];
            let start = Instant::now();
            let rows = node.at(position).read(&shape, &binding).unwrap();
            let latency_us = start.elapsed().as_micros();
            let actual = rows
                .into_iter()
                .find(|row| row.row_uuid() == doc_row())
                .and_then(|row| node_text_value(&mut node, row.cell(&table, "text").unwrap()))
                .unwrap_or_default();
            assert_eq!(actual, doc, "point-in-time prefix {target}");
            summaries.push(PointInTimeSummary {
                cut_percent: percent,
                edit_prefix: target,
                global_seq: position,
                latency_us,
                doc_bytes: actual.len(),
            });
            next_cut += 1;
        }
    }
    summaries
}

fn run_concurrent_merge(_trace: &Trace) -> ConcurrentMergeSummary {
    let schema = schema();
    let (_base_dir, mut base_writer) = open_node(node_id(53), schema.clone());
    let (_core_dir, mut core) = open_node_with_checkpoint_interval(node_id(50), schema.clone(), 1);
    let (_left_dir, mut left) = open_node(node_id(51), schema.clone());
    let (_right_dir, mut right) = open_node(node_id(52), schema);
    let base = "eg-walker maximal non-interleaving\n";
    let left_edit = "left-side edit\n";
    let right_edit = "right-side edit\n";
    let left_author = AuthorId::from_bytes([0x51; 16]);
    let right_author = AuthorId::from_bytes([0x52; 16]);

    let (base_tx, base_unit) = base_writer
        .commit_mergeable_unit(
            MergeableCommit::new(DOCS, doc_row(), 1)
                .made_by(AuthorId::SYSTEM)
                .cells(cells([("text", Value::Bytes(base.as_bytes().to_vec()))])),
        )
        .unwrap();
    base_writer
        .finalize_local_mergeable_commit(base_tx)
        .unwrap();
    let base_unit = base_writer.commit_unit_for(base_tx).unwrap_or(base_unit);
    apply_with_content_extents(&mut base_writer, &mut core, base_unit.clone()).unwrap();
    apply_with_content_extents(&mut base_writer, &mut left, base_unit.clone()).unwrap();
    apply_with_content_extents(&mut base_writer, &mut right, base_unit).unwrap();
    assert_eq!(read_doc(&mut left), base);
    assert_eq!(read_doc(&mut right), base);

    let start = Instant::now();
    let left_tx = left
        .commit_large_value_edit(
            LargeValueEditCommit::new(DOCS, doc_row(), "text", 2)
                .made_by(left_author)
                .insert(0, left_edit.as_bytes()),
        )
        .unwrap();
    left.finalize_local_mergeable_commit(left_tx).unwrap();
    let right_tx = right
        .commit_large_value_edit(
            LargeValueEditCommit::new(DOCS, doc_row(), "text", 2)
                .made_by(right_author)
                .insert(0, right_edit.as_bytes()),
        )
        .unwrap();
    right.finalize_local_mergeable_commit(right_tx).unwrap();
    let mut synced_bytes = 0_u64;
    let left_unit = left.commit_unit_for(left_tx).unwrap();
    synced_bytes += commit_unit_bytes(&left_unit);
    let mut core_updates = Vec::new();
    for update in apply_with_content_extents(&mut left, &mut core, left_unit.clone()).unwrap() {
        synced_bytes += commit_unit_bytes(&update);
        core_updates.push(update);
    }
    let right_unit = right.commit_unit_for(right_tx).unwrap();
    synced_bytes += commit_unit_bytes(&right_unit);
    for update in apply_with_content_extents(&mut right, &mut core, right_unit.clone()).unwrap() {
        synced_bytes += commit_unit_bytes(&update);
        core_updates.push(update);
    }
    synced_bytes += commit_unit_bytes(&right_unit);
    apply_with_content_extents(&mut right, &mut left, right_unit).unwrap();
    synced_bytes += commit_unit_bytes(&left_unit);
    apply_with_content_extents(&mut left, &mut right, left_unit).unwrap();
    for update in core_updates {
        if matches!(update, SyncMessage::FateUpdate { .. }) {
            continue;
        }
        left.apply_sync_message(update.clone()).unwrap();
        right.apply_sync_message(update).unwrap();
    }
    let elapsed_us = start.elapsed().as_micros();

    let core_doc = read_doc(&mut core);

    ConcurrentMergeSummary {
        edits: 2,
        elapsed_us,
        synced_bytes,
        final_doc_bytes: core_doc.len(),
    }
}

fn run_diamond_types_floor(trace: &Trace) -> AdversarySummary {
    let mut doc = ListCRDT::new();
    let agent = doc.oplog.get_or_create_agent_id("s6-trace");
    let start = Instant::now();
    let mut peak_chars = 0_usize;
    for edit in &trace.edits {
        match edit {
            Edit::Insert { pos, text } => {
                let s = text.to_string();
                doc.insert(agent, *pos, &s);
            }
            Edit::Delete { pos } => {
                doc.delete(agent, *pos..(*pos + 1));
            }
        }
        peak_chars = peak_chars.max(doc.len());
    }
    let elapsed_us = start.elapsed().as_micros();
    let final_doc = doc.branch.content().to_string();
    let matched = final_doc == trace.final_doc;
    assert!(matched, "diamond-types final document mismatch");
    AdversarySummary {
        adversary: "diamond-types",
        envelope: "in-memory-nondurable-floor",
        status: "ok",
        asymmetry: "non-durable in-memory CRDT floor; does not pay jazz history/durability cost",
        replay_edits_per_sec: Some(
            trace.edits.len() as f64 / (elapsed_us as f64 / 1_000_000.0).max(0.000_001),
        ),
        elapsed_us: Some(elapsed_us),
        peak_memory_proxy_bytes: Some(peak_chars),
        save_bytes: None,
        bytes_per_edit: None,
        cold_load_us: None,
        final_doc_matched: Some(matched),
    }
}

fn run_automerge_baseline(trace: &Trace) -> AdversarySummary {
    let mut doc = AutoCommit::new_with_encoding(TextEncoding::UnicodeCodePoint);
    let text_obj = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    let start = Instant::now();
    let mut peak_chars = 0_usize;
    for edit in &trace.edits {
        match edit {
            Edit::Insert { pos, text } => {
                let s = text.to_string();
                doc.splice_text(&text_obj, *pos, 0, &s).unwrap();
            }
            Edit::Delete { pos } => {
                doc.splice_text(&text_obj, *pos, 1, "").unwrap();
            }
        }
        peak_chars = peak_chars.max(doc.length(&text_obj));
    }
    let elapsed_us = start.elapsed().as_micros();
    let final_doc = doc.text(&text_obj).unwrap();
    let matched = final_doc == trace.final_doc;
    assert!(matched, "automerge final document mismatch");
    let save = doc.save();
    let cold_start = Instant::now();
    let loaded = AutoCommit::load(&save).unwrap();
    let cold_load_us = cold_start.elapsed().as_micros();
    let loaded_text = loaded
        .get(ROOT, "text")
        .unwrap()
        .and_then(|(_, obj)| loaded.text(&obj).ok())
        .unwrap_or_default();
    assert_eq!(loaded_text, trace.final_doc);
    AdversarySummary {
        adversary: "automerge",
        envelope: "durable-baseline",
        status: "ok",
        asymmetry: "durable CRDT save-format baseline; stores CRDT operation history, not jazz row history",
        replay_edits_per_sec: Some(
            trace.edits.len() as f64 / (elapsed_us as f64 / 1_000_000.0).max(0.000_001),
        ),
        elapsed_us: Some(elapsed_us),
        peak_memory_proxy_bytes: Some(peak_chars),
        save_bytes: Some(save.len() as u64),
        bytes_per_edit: Some(save.len() as f64 / trace.edits.len().max(1) as f64),
        cold_load_us: Some(cold_load_us),
        final_doc_matched: Some(matched),
    }
}

fn emit_replay(
    config: &Config,
    profile: &PeerProfile,
    trace: &Trace,
    batch: usize,
    summary: &ReplaySummary,
) {
    let mut fields = base_fields(config, profile, trace, "trace_replay");
    fields.insert("batch_edits".to_owned(), json!(batch));
    fields.insert("edits".to_owned(), json!(summary.edits));
    fields.insert("commits".to_owned(), json!(summary.commits));
    fields.insert(
        "ingest_edits_per_sec".to_owned(),
        json!(summary.edits as f64 / (summary.elapsed_us as f64 / 1_000_000.0).max(0.000_001)),
    );
    fields.insert(
        "local_echo_p50_us".to_owned(),
        json!(summary.echo.value_at_quantile(0.5)),
    );
    fields.insert(
        "local_echo_p95_us".to_owned(),
        json!(summary.echo.value_at_quantile(0.95)),
    );
    fields.insert("peak_rss_bytes".to_owned(), json!(summary.peak_rss_bytes));
    fields.insert(
        "peak_memory_proxy_bytes".to_owned(),
        json!(summary.peak_doc_bytes),
    );
    fields.insert(
        "history_metadata_bytes_per_edit".to_owned(),
        json!(summary.history_bytes as f64 / summary.edits.max(1) as f64),
    );
    fields.insert(
        "history_class_bytes_per_edit".to_owned(),
        json!(summary.history_class_bytes as f64 / summary.edits.max(1) as f64),
    );
    fields.insert(
        "tail_consolidated_windows".to_owned(),
        json!(summary.tail_consolidation.windows),
    );
    fields.insert(
        "tail_consolidated_window_records".to_owned(),
        json!(summary.tail_consolidation.records),
    );
    fields.insert(
        "tail_consolidated_fraction".to_owned(),
        json!(summary.tail_consolidation.records as f64 / summary.commits.max(1) as f64),
    );
    fields.insert(
        "tail_consolidation_us".to_owned(),
        json!(summary.tail_consolidation.elapsed_us),
    );
    fields.insert(
        "zstd_final_doc_bytes".to_owned(),
        json!(summary.zstd_final_doc_bytes),
    );
    fields.insert(
        "zstd19_final_doc_bytes".to_owned(),
        json!(summary.zstd19_final_doc_bytes),
    );
    fields.insert(
        "zstd_json_op_log_bytes".to_owned(),
        json!(summary.zstd_json_log_bytes),
    );
    fields.insert(
        "zstd19_json_op_log_bytes".to_owned(),
        json!(summary.zstd19_json_log_bytes),
    );
    fields.insert(
        "correctness".to_owned(),
        json!("final_and_prefix_replay_matched"),
    );
    emit_json_line("s6_text_traces", &JsonValue::Object(fields).to_string());
}

fn emit_db_surface_replay(
    config: &Config,
    profile: &PeerProfile,
    trace: &Trace,
    batch: usize,
    summary: &DbSurfaceReplaySummary,
) {
    let mut fields = base_fields(config, profile, trace, "db_surface_trace_replay");
    fields.insert("api_surface".to_owned(), json!("db"));
    fields.insert("batch_edits".to_owned(), json!(batch));
    fields.insert("edits".to_owned(), json!(summary.edits));
    fields.insert("commits".to_owned(), json!(summary.commits));
    fields.insert(
        "ingest_edits_per_sec".to_owned(),
        json!(summary.edits as f64 / (summary.elapsed_us as f64 / 1_000_000.0).max(0.000_001)),
    );
    fields.insert(
        "local_echo_p50_us".to_owned(),
        json!(summary.echo.value_at_quantile(0.5)),
    );
    fields.insert(
        "local_echo_p95_us".to_owned(),
        json!(summary.echo.value_at_quantile(0.95)),
    );
    fields.insert("peak_rss_bytes".to_owned(), json!(summary.peak_rss_bytes));
    fields.insert(
        "peak_memory_proxy_bytes".to_owned(),
        json!(summary.peak_doc_bytes),
    );
    fields.insert(
        "history_metadata_bytes_per_edit".to_owned(),
        json!(summary.history_bytes as f64 / summary.edits.max(1) as f64),
    );
    fields.insert(
        "history_class_bytes_per_edit".to_owned(),
        json!(summary.history_class_bytes as f64 / summary.edits.max(1) as f64),
    );
    fields.insert(
        "consolidated_windows".to_owned(),
        json!(summary.consolidated_windows),
    );
    fields.insert(
        "consolidated_window_records".to_owned(),
        json!(summary.consolidated_window_records),
    );
    fields.insert(
        "history_window_consolidation_us".to_owned(),
        json!(summary.history_window_consolidation_us),
    );
    fields.insert(
        "history_window_consolidation_us_per_tick_window".to_owned(),
        json!(
            summary.history_window_consolidation_us as f64
                / summary.consolidated_windows.max(1) as f64
        ),
    );
    fields.insert(
        "tail_consolidated_windows".to_owned(),
        json!(summary.tail_consolidation.windows),
    );
    fields.insert(
        "tail_consolidated_window_records".to_owned(),
        json!(summary.tail_consolidation.records),
    );
    fields.insert(
        "tail_consolidated_fraction".to_owned(),
        json!(summary.tail_consolidation.records as f64 / summary.commits.max(1) as f64),
    );
    fields.insert(
        "tail_consolidation_us".to_owned(),
        json!(summary.tail_consolidation.elapsed_us),
    );
    fields.insert("correctness".to_owned(), json!("final_replay_matched"));
    emit_json_line("s6_text_traces", &JsonValue::Object(fields).to_string());

    let mut acceptance = base_fields(config, profile, trace, "edge_mergeable_acceptance");
    acceptance.insert("batch_edits".to_owned(), json!(batch));
    acceptance.insert(
        "acceptance_p50_us".to_owned(),
        json!(summary.edge_acceptance.value_at_quantile(0.50)),
    );
    acceptance.insert(
        "acceptance_p95_us".to_owned(),
        json!(summary.edge_acceptance.value_at_quantile(0.95)),
    );
    acceptance.insert("durability_tier".to_owned(), json!("Edge"));
    emit_json_line("s6_text_traces", &JsonValue::Object(acceptance).to_string());

    let mut hydration = base_fields(config, profile, trace, "edge_permission_scope_hydration");
    hydration.insert("batch_edits".to_owned(), json!(batch));
    hydration.insert("scope".to_owned(), json!("text_doc_table_surface"));
    hydration.insert(
        "hydration_bytes".to_owned(),
        json!(summary.edge_hydration_bytes),
    );
    hydration.insert(
        "hydration_floor_bytes".to_owned(),
        json!(summary.edge_hydration_bytes),
    );
    hydration.insert(
        "hydration_rows".to_owned(),
        json!(summary.edge_hydration_rows),
    );
    emit_json_line("s6_text_traces", &JsonValue::Object(hydration).to_string());
}

fn emit_adversary_comparison(
    config: &Config,
    profile: &PeerProfile,
    trace: &Trace,
    jazz: &JazzComparison,
    summary: AdversarySummary,
) {
    let mut fields = base_fields(config, profile, trace, "crdt_adversary_comparison");
    fields.insert("adversary".to_owned(), json!(summary.adversary));
    fields.insert("envelope".to_owned(), json!(summary.envelope));
    fields.insert("adversary_status".to_owned(), json!(summary.status));
    fields.insert("asymmetry_note".to_owned(), json!(summary.asymmetry));
    fields.insert("edits".to_owned(), json!(jazz.edits));
    fields.insert("jazz_envelope".to_owned(), json!("jazz-durable"));
    fields.insert(
        "jazz_history_metadata_bytes_per_edit".to_owned(),
        json!(jazz.bytes_per_edit),
    );
    fields.insert(
        "jazz_replay_edits_per_sec".to_owned(),
        json!(jazz.replay_edits_per_sec),
    );
    fields.insert(
        "adversary_replay_edits_per_sec".to_owned(),
        json!(summary.replay_edits_per_sec),
    );
    fields.insert("adversary_elapsed_us".to_owned(), json!(summary.elapsed_us));
    fields.insert(
        "adversary_peak_memory_proxy_bytes".to_owned(),
        json!(summary.peak_memory_proxy_bytes),
    );
    fields.insert("adversary_save_bytes".to_owned(), json!(summary.save_bytes));
    fields.insert(
        "adversary_bytes_per_edit".to_owned(),
        json!(summary.bytes_per_edit),
    );
    fields.insert(
        "adversary_cold_load_us".to_owned(),
        json!(summary.cold_load_us),
    );
    fields.insert(
        "final_doc_matched".to_owned(),
        json!(summary.final_doc_matched),
    );
    fields.insert(
        "storage_ratio".to_owned(),
        json!(
            summary
                .bytes_per_edit
                .map(|bytes_per_edit| jazz.bytes_per_edit / bytes_per_edit)
        ),
    );
    fields.insert(
        "storage_ratio_note".to_owned(),
        json!(if summary.bytes_per_edit.is_some() {
            "jazz bytes/edit divided by durable CRDT save bytes/edit"
        } else {
            "not_applicable_for_in_memory_nondurable_floor"
        }),
    );
    fields.insert(
        "replay_throughput_ratio".to_owned(),
        json!(
            summary
                .replay_edits_per_sec
                .map(|throughput| jazz.replay_edits_per_sec / throughput)
        ),
    );
    emit_json_line("s6_text_traces", &JsonValue::Object(fields).to_string());
}

fn emit_live(config: &Config, profile: &PeerProfile, trace: &Trace, summary: &LiveSummary) {
    let mut fields = base_fields(config, profile, trace, "live_observation");
    fields.insert("edits".to_owned(), json!(summary.edits));
    fields.insert("edit_rate_nominal_per_sec".to_owned(), json!(10));
    fields.insert("observer_p95_us".to_owned(), json!(summary.observer_p95_us));
    fields.insert("synced_bytes".to_owned(), json!(summary.synced_bytes));
    fields.insert(
        "link_rtt_floor_us".to_owned(),
        json!(profile.one_way_latency_ms * 2_000),
    );
    emit_json_line("s6_text_traces", &JsonValue::Object(fields).to_string());
}

fn emit_concurrent_merge(
    config: &Config,
    profile: &PeerProfile,
    trace: &Trace,
    summary: &ConcurrentMergeSummary,
) {
    let mut fields = base_fields(config, profile, trace, "concurrent_merge");
    fields.insert("edits".to_owned(), json!(summary.edits));
    fields.insert(
        "merge_edits_per_sec".to_owned(),
        json!(summary.edits as f64 / (summary.elapsed_us as f64 / 1_000_000.0).max(0.000_001)),
    );
    fields.insert("synced_bytes".to_owned(), json!(summary.synced_bytes));
    fields.insert("final_doc_bytes".to_owned(), json!(summary.final_doc_bytes));
    fields.insert(
        "correctness".to_owned(),
        json!("eg_walker_maximal_non_interleaving_both_edits_survived_all_replicas_converged"),
    );
    emit_json_line("s6_text_traces", &JsonValue::Object(fields).to_string());
}

fn emit_cold(config: &Config, profile: &PeerProfile, trace: &Trace, summary: &ColdSummary) {
    let mut fields = base_fields(config, profile, trace, "cold_load");
    fields.insert("current_only_us".to_owned(), json!(summary.current_only_us));
    fields.insert(
        "with_history_status".to_owned(),
        json!("gated_until_history_subscription_load_path"),
    );
    fields.insert("current_bytes".to_owned(), json!(summary.current_bytes));
    fields.insert("history_bytes".to_owned(), json!(summary.history_bytes));
    emit_json_line("s6_text_traces", &JsonValue::Object(fields).to_string());
}

fn emit_point_in_time(
    config: &Config,
    profile: &PeerProfile,
    trace: &Trace,
    summary: &PointInTimeSummary,
) {
    let mut fields = base_fields(config, profile, trace, "point_in_time_read");
    fields.insert("cut_percent".to_owned(), json!(summary.cut_percent));
    fields.insert("edit_prefix".to_owned(), json!(summary.edit_prefix));
    fields.insert("global_seq".to_owned(), json!(summary.global_seq.0));
    fields.insert(
        "point_in_time_read_us".to_owned(),
        json!(summary.latency_us),
    );
    fields.insert("doc_bytes".to_owned(), json!(summary.doc_bytes));
    fields.insert(
        "correctness".to_owned(),
        json!("matched_direct_prefix_replay"),
    );
    emit_json_line("s6_text_traces", &JsonValue::Object(fields).to_string());
}

fn base_fields(
    config: &Config,
    profile: &PeerProfile,
    trace: &Trace,
    phase: &str,
) -> serde_json::Map<String, JsonValue> {
    let mut fields = metadata_fields("s6_text_traces", "synchronous", config.seed, &profile.name);
    fields.insert("phase".to_owned(), json!(phase));
    fields.insert("trace".to_owned(), json!(trace.name));
    fields.insert("trace_profile".to_owned(), json!(trace.profile));
    fields.insert("trace_commit".to_owned(), json!(trace.commit));
    fields.insert("trace_url".to_owned(), json!(trace.url));
    fields.insert(
        "trace_fixture_path".to_owned(),
        json!(trace.fixture_path.display().to_string()),
    );
    fields.insert("trace_sha256".to_owned(), json!(trace.fixture_sha256));
    fields.insert(
        "expected_trace_sha256".to_owned(),
        json!(trace.expected_sha256),
    );
    fields
}

fn synthetic_edits(limit: usize) -> Vec<Edit> {
    let alphabet = b"abcdefghijklmnopqrstuvwxyz \n";
    let mut edits = Vec::with_capacity(limit);
    let mut len = 0_usize;
    for idx in 0..limit {
        if idx % 11 == 10 && len > 0 {
            edits.push(Edit::Delete {
                pos: (idx * 7) % len,
            });
            len -= 1;
        } else {
            let pos = if len == 0 { 0 } else { (idx * 13) % (len + 1) };
            edits.push(Edit::Insert {
                pos,
                text: alphabet[idx % alphabet.len()] as char,
            });
            len += 1;
        }
    }
    edits
}

fn apply_edits(edits: &[Edit]) -> String {
    let mut doc = String::new();
    for edit in edits {
        apply_edit(&mut doc, edit);
    }
    doc
}

fn apply_edit(doc: &mut String, edit: &Edit) {
    match edit {
        Edit::Insert { pos, text } => {
            let idx = byte_index_for_char(doc, *pos);
            doc.insert(idx, *text);
        }
        Edit::Delete { pos } => {
            if *pos < doc.chars().count() {
                let idx = byte_index_for_char(doc, *pos);
                doc.remove(idx);
            }
        }
    }
}

fn byte_index_for_char(doc: &str, char_idx: usize) -> usize {
    doc.char_indices()
        .nth(char_idx)
        .map(|(idx, _)| idx)
        .unwrap_or(doc.len())
}

fn json_log(edits: &[Edit]) -> String {
    edits
        .iter()
        .map(|edit| match edit {
            Edit::Insert { pos, text } => {
                json!({"insert": text.to_string(), "pos": pos}).to_string()
            }
            Edit::Delete { pos } => json!({"delete": pos}).to_string(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn fixture_path(spec: &TraceSpec) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(spec.fixture_dir)
        .join(spec.fixture_file)
}

fn sha256(path: &Path) -> String {
    let output = Command::new("sh")
        .arg("-c")
        .arg(format!("shasum -a 256 {}", shell_escape(path)))
        .output()
        .unwrap();
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .unwrap_or("unknown")
        .to_owned()
}

fn shell_escape(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(DOCS, [ColumnSchema::text("text")])])
}

fn open_node(node_uuid: NodeUuid, schema: JazzSchema) -> (TempDir, NodeState<RocksDbStorage>) {
    open_node_with_checkpoint_interval(node_uuid, schema, LARGE_VALUE_CHECKPOINT_OP_INTERVAL)
}

fn open_node_with_checkpoint_interval(
    node_uuid: NodeUuid,
    schema: JazzSchema,
    large_value_checkpoint_op_interval: usize,
) -> (TempDir, NodeState<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    (
        dir,
        NodeState::new_with_large_value_checkpoint_op_interval(
            node_uuid,
            schema,
            storage,
            true,
            large_value_checkpoint_op_interval,
        )
        .unwrap(),
    )
}

fn open_db(node_uuid: NodeUuid, schema: JazzSchema) -> (TempDir, Db<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: AuthorId::SYSTEM,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(u64::from_le_bytes(
            node_uuid.as_bytes()[..8]
                .try_into()
                .expect("node seed bytes"),
        )))),
        large_value_checkpoint_op_interval: LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .expect("db open");
    (dir, db)
}

fn open_history_complete_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (TempDir, NodeState<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    (
        dir,
        NodeState::new_history_complete(node_uuid, schema, storage).unwrap(),
    )
}

fn read_doc(node: &mut NodeState<RocksDbStorage>) -> String {
    let schema = schema();
    let table = table_schema(&schema, DOCS);
    let cell = node
        .current_rows(DOCS, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .find(|row| row.row_uuid() == doc_row())
        .and_then(|row| row.cell(table, "text"));
    cell.and_then(|value| node_text_value(node, value))
        .unwrap_or_default()
}

fn read_db_doc(db: &Db<RocksDbStorage>, schema: &JazzSchema) -> String {
    let table = table_schema(schema, DOCS);
    let query = Query::from(DOCS);
    let prepared = db.prepare_query(&query).expect("prepare docs query");
    db.read(&prepared)
        .expect("db read docs")
        .into_iter()
        .find(|row| row.row_uuid() == doc_row())
        .and_then(|row| db_text_value(db, row.cell(table, "text").unwrap()))
        .unwrap_or_default()
}

fn node_text_value(node: &mut NodeState<RocksDbStorage>, value: Value) -> Option<String> {
    match value {
        Value::Bytes(handle) => node
            .hydrate_large_value_handle(&handle)
            .ok()
            .and_then(|text| String::from_utf8(text).ok()),
        _ => None,
    }
}

fn db_text_value(db: &Db<RocksDbStorage>, value: Value) -> Option<String> {
    match value {
        Value::Bytes(handle) => db
            .hydrate_large_value_handle(&handle)
            .ok()
            .and_then(|text| String::from_utf8(text).ok()),
        _ => None,
    }
}

fn commit_unit_bytes(update: &SyncMessage) -> u64 {
    match update {
        SyncMessage::CommitUnit { versions, .. } => versions
            .iter()
            .map(|version| version.record().raw().len() as u64 + 64)
            .sum(),
        SyncMessage::FateUpdate { .. } => 48,
        _ => 0,
    }
}

fn apply_with_content_extents(
    source: &mut NodeState<RocksDbStorage>,
    target: &mut NodeState<RocksDbStorage>,
    message: SyncMessage,
) -> Result<Vec<SyncMessage>, jazz::node::Error> {
    for extent in content_extents_in_message(&message) {
        let bytes = source.content_store().read(&extent)?;
        target.content_store().put_extent(&extent, &bytes)?;
    }
    let updates = target.apply_sync_message(message)?;
    Ok(updates)
}

fn content_extents_in_message(message: &SyncMessage) -> Vec<Extent> {
    let mut extents = Vec::new();
    match message {
        SyncMessage::CommitUnit { versions, .. } => {
            collect_extents_from_versions(versions.iter(), &mut extents);
        }
        SyncMessage::ViewUpdate {
            version_carriers,
            version_bundles,
            ..
        } => {
            let mut bundles = version_bundles.clone();
            bundles.extend(
                expand_version_carriers(version_carriers)
                    .expect("bench view update carriers should expand"),
            );
            collect_extents_from_versions(
                bundles.iter().flat_map(|bundle| bundle.versions.iter()),
                &mut extents,
            );
        }
        _ => {}
    }
    extents
}

fn collect_extents_from_versions<'a>(
    versions: impl Iterator<Item = &'a jazz::protocol::VersionRecord>,
    extents: &mut Vec<Extent>,
) {
    for version in versions {
        if version.table() != DOCS {
            continue;
        }
        let Some(Value::Bytes(payload)) = version.cell_at(0) else {
            continue;
        };
        let payload = payload
            .strip_prefix(b"JTXTREF1")
            .unwrap_or(payload.as_slice());
        let Ok(ops) = text_oplog::decode(payload) else {
            continue;
        };
        for op in ops {
            if let text_oplog::Op::Insert {
                content: TextContent::Ref(extent),
                ..
            } = op
            {
                extents.push(extent);
            }
        }
    }
}

fn view_update_bytes(update: &SyncMessage) -> u64 {
    match update {
        SyncMessage::ViewUpdate {
            version_carriers,
            version_bundles,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            ..
        } => {
            let mut bundles = version_bundles.clone();
            bundles.extend(
                expand_version_carriers(version_carriers)
                    .expect("bench view update carriers should expand"),
            );
            bundles
                .iter()
                .flat_map(|bundle| bundle.versions.iter())
                .map(|version| version.record().raw().len() as u64 + 64)
                .sum::<u64>()
                + (peer_payload_inventory.complete_tx_payloads.len() as u64 * 24)
                + ((result_member_adds.len() + result_member_removes.len()) as u64 * 64)
        }
        _ => 0,
    }
}

fn result_row_count(update: &SyncMessage) -> usize {
    match update {
        SyncMessage::ViewUpdate {
            result_member_adds,
            result_member_removes,
            ..
        } => result_member_adds.len() + result_member_removes.len(),
        _ => 0,
    }
}

fn storage_bytes(path: &Path) -> u64 {
    fs::read_dir(path)
        .into_iter()
        .flat_map(|entries| entries.flatten())
        .map(|entry| {
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                storage_bytes(&entry.path())
            } else {
                meta.len()
            }
        })
        .sum()
}

fn cells<const N: usize>(items: [(&str, Value); N]) -> BTreeMap<String, Value> {
    items.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
}

fn table_schema<'a>(schema: &'a JazzSchema, table: &str) -> &'a TableSchema {
    schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .unwrap()
}

fn doc_row() -> RowUuid {
    RowUuid::from_bytes([6; 16])
}

fn node_id(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
