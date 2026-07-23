//! RocksDB-focused ingestion + point-read benchmark on the USDA plants dataset,
//! across three topologies, all RocksDB-backed:
//!
//! * `raw` — RocksDB driven directly (no Jazz).
//! * `local` — a serverless `jazz_tools::JazzClient` (no upstream).
//! * `server` — a `JazzClient` synced over a real localhost websocket into a
//!   `jazz_tools::server::JazzServer` (the real deployment path).
//!
//! Each plant is assigned a UUID; the same sampled ids are fetched back by id.
//! Reports write time, throughput, point-lookup latency, on-disk size + write
//! amplification, and (for `server`) rows confirmed by the server.
//!
//! The server topology waits for each batch to settle at the server; large
//! batches can exceed the default 25s `wait_for_batch` deadline, so run with
//! `JAZZ_TOOLS_WAIT_FOR_BATCH_TIMEOUT_SECS=300`.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use jazz_tools::server::JazzServer;
use jazz_tools::{
    AppContext, ClientStorage, ColumnType, DurabilityTier, JazzClient, PolicyExpr, QueryBuilder,
    Schema, SchemaBuilder, SchemaHash, TablePolicies, TableSchema, Value,
};
use rocksdb::{DB as RawDb, DBCompressionType, Options as RawOptions, WriteBatch};
use uuid::Uuid;

const BATCH: u64 = 1000;
const SAMPLE: usize = 500;
/// Point lookups on the (unindexed) Jazz path are full scans, so we time a small
/// sub-sample and report per-lookup latency rather than all `SAMPLE`.
const PROBE: usize = 20;
const FIELD_SEP: u8 = 0x1f;

// ---------------------------------------------------------------------------
// Dataset
// ---------------------------------------------------------------------------

struct Plant {
    id: String,
    fields: [String; 5],
}

impl Plant {
    fn logical_len(&self) -> usize {
        self.id.len() + self.fields.iter().map(String::len).sum::<usize>()
    }
    fn cells(&self) -> HashMap<String, Value> {
        HashMap::from([
            ("plant_id".to_string(), Value::Text(self.id.clone())),
            ("symbol".to_string(), Value::Text(self.fields[0].clone())),
            (
                "synonym_symbol".to_string(),
                Value::Text(self.fields[1].clone()),
            ),
            (
                "scientific_name".to_string(),
                Value::Text(self.fields[2].clone()),
            ),
            (
                "common_name".to_string(),
                Value::Text(self.fields[3].clone()),
            ),
            ("family".to_string(), Value::Text(self.fields[4].clone())),
        ])
    }
    fn raw_value(&self) -> Vec<u8> {
        self.fields
            .iter()
            .map(String::as_bytes)
            .collect::<Vec<_>>()
            .join(&[FIELD_SEP][..])
    }
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else {
            match c {
                '"' => in_quotes = true,
                ',' => fields.push(std::mem::take(&mut cur)),
                _ => cur.push(c),
            }
        }
    }
    fields.push(cur);
    fields
}

fn load_plants(limit: usize) -> Vec<Plant> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/data/plantlst.txt");
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {path}: {e}\nrun scripts/setup.sh first"));
    let mut out = Vec::new();
    for line in text.lines().skip(1) {
        if line.is_empty() {
            continue;
        }
        let mut c = parse_csv_line(line);
        c.resize(5, String::new());
        let idx = out.len() as u64;
        out.push(Plant {
            id: Uuid::from_u128((idx + 1) as u128).to_string(),
            fields: [
                std::mem::take(&mut c[0]),
                std::mem::take(&mut c[1]),
                std::mem::take(&mut c[2]),
                std::mem::take(&mut c[3]),
                std::mem::take(&mut c[4]),
            ],
        });
        if out.len() >= limit {
            break;
        }
    }
    out
}

fn sample_ids(plants: &[Plant], count: usize) -> Vec<String> {
    let count = count.min(plants.len());
    let mut chosen = std::collections::BTreeSet::new();
    let mut state = 0x5eed_u64;
    while chosen.len() < count {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chosen.insert((state as usize) % plants.len());
    }
    chosen.into_iter().map(|i| plants[i].id.clone()).collect()
}

fn logical_bytes(plants: &[Plant]) -> u64 {
    plants.iter().map(|p| p.logical_len() as u64).sum()
}

fn dir_size(path: &Path) -> u64 {
    let Ok(md) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if md.is_dir() {
        std::fs::read_dir(path)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| dir_size(&e.path()))
            .sum()
    } else {
        md.len()
    }
}

// ---------------------------------------------------------------------------
// Result rows
// ---------------------------------------------------------------------------

struct Row {
    topology: &'static str,
    write: Duration,
    rows: usize,
    get_by_id: String,
    size: String,
    synced: String,
}

fn size_cell(physical: u64, logical: u64) -> String {
    format!(
        "{:.1} MB ({:.1}× logical)",
        physical as f64 / 1e6,
        physical as f64 / logical.max(1) as f64
    )
}

// ---------------------------------------------------------------------------
// Jazz schema / helpers
// ---------------------------------------------------------------------------

fn schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("plants")
                .column("plant_id", ColumnType::Text)
                .column("symbol", ColumnType::Text)
                .column("synonym_symbol", ColumnType::Text)
                .column("scientific_name", ColumnType::Text)
                .column("common_name", ColumnType::Text)
                .column("family", ColumnType::Text),
        )
        .build()
}

/// Insert every plant in batched transactions on a `JazzClient`. When
/// `wait_tier` is set, block on server settlement after each batch (server
/// topology); otherwise just commit locally. Returns (write time, batch count).
async fn write_plants(
    client: &JazzClient,
    plants: &[Plant],
    wait_tier: Option<DurabilityTier>,
) -> Duration {
    let n = plants.len() as u64;
    let t = Instant::now();
    let mut i = 0u64;
    while i < n {
        let tx = client.begin_transaction().expect("begin transaction");
        for _ in 0..BATCH.min(n - i) {
            tx.insert("plants", plants[i as usize].cells())
                .expect("staged insert");
            i += 1;
        }
        let batch = tx.commit().expect("commit transaction");
        if let Some(tier) = wait_tier {
            client
                .wait_for_batch(batch, tier)
                .await
                .expect("wait for batch");
            if std::env::var("JZ_PROGRESS").is_ok() {
                eprintln!(
                    "    batch {i}/{n} synced  elapsed={:.1}s",
                    t.elapsed().as_secs_f64()
                );
            }
        }
    }
    t.elapsed()
}

/// Average per-lookup latency of a point query by `plant_id` at `tier`, over a
/// small sub-sample (Jazz reads are full scans, so `SAMPLE` would be too slow).
async fn jazz_point_lookup(client: &JazzClient, ids: &[String], tier: DurabilityTier) -> Duration {
    let probe = ids.iter().take(PROBE);
    let t = Instant::now();
    let mut count = 0u32;
    for id in probe {
        let q = QueryBuilder::new("plants")
            .filter_eq("plant_id", Value::Text(id.clone()))
            .build();
        let _ = client.query(q, Some(tier)).await.expect("point query");
        count += 1;
    }
    t.elapsed() / count.max(1)
}

async fn publish_allow_all(base_url: &str, app_id: &str, admin_secret: &str, schema: &Schema) {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    let body = serde_json::json!({
        "schemaHash": SchemaHash::compute(schema).to_string(),
        "permissions": { "plants": serde_json::to_value(&policies).expect("serialize policies") },
        "expectedParentBundleObjectId": Option::<String>::None,
    });
    let http = reqwest::Client::new();
    let url = format!("{base_url}/apps/{app_id}/admin/permissions");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let resp = http
            .post(&url)
            .header("X-Jazz-Admin-Secret", admin_secret)
            .json(&body)
            .send()
            .await
            .expect("publish permissions request");
        match resp.status() {
            reqwest::StatusCode::CREATED => return,
            reqwest::StatusCode::NOT_FOUND if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            status => {
                let text = resp.text().await.unwrap_or_default();
                panic!("publish permissions failed: {status} {text}");
            }
        }
    }
}

async fn wait_edge_ready(client: &JazzClient) {
    let q = QueryBuilder::new("plants").build();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if client
            .query(q.clone(), Some(DurabilityTier::EdgeServer))
            .await
            .is_ok()
        {
            return;
        }
        assert!(Instant::now() < deadline, "edge query never became ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------
// Topologies
// ---------------------------------------------------------------------------

fn run_raw(plants: &[Plant], ids: &[String], logical: u64) -> Row {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut opts = RawOptions::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::Lz4);
    let db = RawDb::open(&opts, dir.path()).expect("open raw rocksdb");

    let t = Instant::now();
    for chunk in plants.chunks(BATCH as usize) {
        let mut wb = WriteBatch::default();
        for p in chunk {
            wb.put(p.id.as_bytes(), p.raw_value());
        }
        db.write(&wb).expect("write batch");
    }
    db.flush().expect("flush");
    let write = t.elapsed();

    let t = Instant::now();
    let mut found = 0;
    for id in ids {
        if db.get(id.as_bytes()).expect("get").is_some() {
            found += 1;
        }
    }
    let read = t.elapsed();
    let physical = dir_size(dir.path());

    Row {
        topology: "raw RocksDB",
        write,
        rows: plants.len(),
        get_by_id: format!("{:.1} ms ({found} point gets)", read.as_secs_f64() * 1e3),
        size: size_cell(physical, logical),
        synced: "—".to_string(),
    }
}

async fn run_local(plants: &[Plant], ids: &[String], logical: u64) -> Row {
    let schema = schema();
    let dir = tempfile::tempdir().expect("client tempdir");
    // Serverless client: empty server_url => no upstream; Persistent => RocksDB.
    let mut ctx = AppContext::test(schema.clone());
    ctx.storage = ClientStorage::Persistent;
    ctx.data_dir = dir.path().to_path_buf();
    let client = JazzClient::connect(ctx)
        .await
        .expect("connect local client");

    let write = write_plants(&client, plants, None).await;
    let per = jazz_point_lookup(&client, ids, DurabilityTier::Local).await;
    client.shutdown().await.expect("shutdown local client");
    let physical = dir_size(dir.path());

    Row {
        topology: "Jazz + RocksDB (local)",
        write,
        rows: plants.len(),
        get_by_id: format!("{:.0} ms/lookup (Local)", per.as_secs_f64() * 1e3),
        size: size_cell(physical, logical),
        synced: "—".to_string(),
    }
}

async fn run_server(plants: &[Plant], ids: &[String], logical: u64) -> Row {
    let schema = schema();
    let server_dir = tempfile::tempdir().expect("server tempdir");
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .with_rocksdb_storage()
        .with_data_dir(server_dir.path())
        .start()
        .await;
    publish_allow_all(
        &server.base_url(),
        &server.app_id().to_string(),
        server.admin_secret(),
        &schema,
    )
    .await;

    let mut ctx = server.make_client_context_for_user(schema.clone(), "bench");
    ctx.storage = ClientStorage::Persistent;
    let client = JazzClient::connect(ctx).await.expect("connect client");
    wait_edge_ready(&client).await;

    let write = write_plants(&client, plants, Some(DurabilityTier::EdgeServer)).await;
    let per = jazz_point_lookup(&client, ids, DurabilityTier::EdgeServer).await;

    // Confirm the server durably holds every row.
    let synced = server_row_count(&client).await;
    let physical = dir_size(server.data_dir());
    server.shutdown().await;

    Row {
        topology: "Jazz → Jazz Server (RocksDB)",
        write,
        rows: plants.len(),
        get_by_id: format!("{:.0} ms/lookup (EdgeServer)", per.as_secs_f64() * 1e3),
        size: size_cell(physical, logical),
        synced: format!(
            "{synced} / {} {}",
            plants.len(),
            if synced == plants.len() { "✓" } else { "✗" }
        ),
    }
}

async fn server_row_count(client: &JazzClient) -> usize {
    let q = QueryBuilder::new("plants").build();
    client
        .query(q, Some(DurabilityTier::EdgeServer))
        .await
        .expect("count query")
        .len()
}

// ---------------------------------------------------------------------------
// Table rendering
// ---------------------------------------------------------------------------

fn fmt_write(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s < 1.0 {
        format!("{:.3} s", s)
    } else {
        format!("{:.1} s", s)
    }
}

fn fmt_throughput(rows: usize, d: Duration) -> String {
    let r = rows as f64 / d.as_secs_f64().max(1e-9);
    if r >= 1e6 {
        format!("{:.2}M rows/s", r / 1e6)
    } else if r >= 1e3 {
        format!("{:.1}k rows/s", r / 1e3)
    } else {
        format!("{:.0} rows/s", r)
    }
}

fn render(rows: &[Row]) {
    let headers = [
        "topology",
        "write all",
        "throughput",
        "get 500 by id",
        "size",
        "synced",
    ];
    let cells: Vec<[String; 6]> = rows
        .iter()
        .map(|r| {
            [
                r.topology.to_string(),
                fmt_write(r.write),
                fmt_throughput(r.rows, r.write),
                r.get_by_id.clone(),
                r.size.clone(),
                r.synced.clone(),
            ]
        })
        .collect();

    let ncol = headers.len();
    let mut w = [0usize; 6];
    for (wi, h) in w.iter_mut().zip(headers.iter()) {
        *wi = h.chars().count();
    }
    for row in &cells {
        for (wi, cell) in w.iter_mut().zip(row.iter()) {
            *wi = (*wi).max(cell.chars().count());
        }
    }
    let border = |l: &str, m: &str, r: &str| {
        let mut s = String::from(l);
        for (i, width) in w.iter().enumerate() {
            s.push_str(&"─".repeat(width + 2));
            s.push_str(if i + 1 < ncol { m } else { r });
        }
        s
    };
    let data_row = |cells: &[String]| {
        let mut s = String::from("│");
        for (width, cell) in w.iter().zip(cells.iter()) {
            s.push(' ');
            s.push_str(cell);
            s.push_str(&" ".repeat(width - cell.chars().count()));
            s.push_str(" │");
        }
        s
    };

    println!("{}", border("┌", "┬", "┐"));
    println!(
        "{}",
        data_row(&headers.iter().map(|h| h.to_string()).collect::<Vec<_>>())
    );
    for row in &cells {
        println!("{}", border("├", "┼", "┤"));
        println!("{}", data_row(row));
    }
    println!("{}", border("└", "┴", "┘"));
}

// ---------------------------------------------------------------------------

async fn run() {
    let n = std::env::var("N")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(15_000);
    let plants = load_plants(n);
    let ids = sample_ids(&plants, SAMPLE);
    let logical = logical_bytes(&plants);
    println!(
        "dataset {} rows | logical payload {:.2} MB | sample {}\n",
        plants.len(),
        logical as f64 / 1e6,
        ids.len()
    );

    let raw = run_raw(&plants, &ids, logical);
    let local = run_local(&plants, &ids, logical).await;
    let server = run_server(&plants, &ids, logical).await;
    render(&[raw, local, server]);
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tokio::task::LocalSet::new().run_until(run()).await;
}
