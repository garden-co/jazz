//! Real concurrent requests through public clients, WebSockets and a WAL server.
//! Fixture construction is outside timing. No sleeps or runtime hooks make a
//! query slow: a deliberately expensive inline-text predicate does real work.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, SeededRowIdSource, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, contains, eq, lit, lte};
use jazz::records::Value as CoreValue;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, PolicyExpr, ReadTier, Schema, SchemaBuilder,
    TablePolicies, TableSchema, Value,
};
use jazz_native_transport::NativeWebSocketConnector;
use jazz_server::JazzServer;
use serde_json::{Value as JsonValue, json};
use tokio::sync::Barrier;
use uuid::Uuid;

#[global_allocator]
static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
const TIMEOUT: Duration = Duration::from_secs(60);
const PROBE: Uuid = Uuid::from_u128(1);
static NEXT_PROBE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct Config {
    rows: usize,
    payload_bytes: usize,
    readers: usize,
    requests: usize,
    background: usize,
    rounds: usize,
    revision: String,
    smoke: bool,
}

impl Config {
    fn parse() -> Result<Self> {
        let smoke = std::env::args().any(|arg| arg == "--smoke");
        let mut config = Self {
            rows: if smoke { 32 } else { 16_000 },
            payload_bytes: if smoke { 128 } else { 2048 },
            readers: if smoke { 2 } else { 4 },
            requests: if smoke { 3 } else { 40 },
            background: 1,
            rounds: if smoke { 2 } else { 4 },
            revision: "unspecified".to_owned(),
            smoke,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            if arg == "--smoke" {
                continue;
            }
            if arg == "--help" || arg == "-h" {
                println!(
                    "server_read_contention [--smoke] [--rows N] [--payload-bytes N] [--readers N] [--requests N] [--background N] [--rounds N] [--revision LABEL]"
                );
                std::process::exit(0);
            }
            let value = args.next().ok_or("each option requires a value")?;
            match arg.as_str() {
                "--rows" => config.rows = value.parse()?,
                "--payload-bytes" => config.payload_bytes = value.parse()?,
                "--readers" => config.readers = value.parse()?,
                "--requests" => config.requests = value.parse()?,
                "--background" => config.background = value.parse()?,
                "--rounds" => config.rounds = value.parse()?,
                "--revision" => config.revision = value,
                _ => return Err(format!("unknown option: {arg}").into()),
            }
        }
        if config.rows == 0
            || config.readers == 0
            || config.requests == 0
            || config.background == 0
            || config.rounds == 0
            || !(128..=2048).contains(&config.payload_bytes)
        {
            return Err(
                "counts must be positive; payload-bytes must be 128..=2048 (inline text)".into(),
            );
        }
        if cfg!(debug_assertions) && !smoke {
            return Err(
                "timing requires --release; use --smoke for a correctness-only debug run".into(),
            );
        }
        Ok(config)
    }
}

fn policies() -> TablePolicies {
    TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
}

fn schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("events")
                .column("payload", ColumnType::Text)
                .policies(policies()),
        )
        .table(
            TableSchema::builder("probes")
                .column("n", ColumnType::BigInt)
                .policies(policies()),
        )
        .table(
            TableSchema::builder("writes")
                .column("n", ColumnType::BigInt)
                .policies(policies()),
        )
        .build()
}

/// Build synthetic rows through the public engine API, then close the file
/// before the server opens it. All measured writes use WebSocket clients.
fn seed(path: &std::path::Path, schema: &Schema, config: &Config) -> Result<()> {
    let db = block_on(Db::open_at(
        DbConfig::new(
            JazzSchema::new(schema)?,
            DbIdentity {
                node: NodeUuid::from_bytes([0x71; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x51_17e)),
        path,
        FileOptions::default(),
    ))?;
    let payload = "a".repeat(config.payload_bytes);
    for start in (0..config.rows).step_by(128) {
        block_on(db.transaction(async |tx| {
            for _ in start..(start + 128).min(config.rows) {
                tx.insert(
                    "events",
                    BTreeMap::from([("payload".to_owned(), CoreValue::String(payload.clone()))]),
                    InsertOptions::default(),
                )
                .await?;
            }
            Ok(())
        }))?;
    }
    block_on(db.insert(
        "probes",
        BTreeMap::from([("n".to_owned(), CoreValue::I64(7))]),
        InsertOptions {
            row_id: Some(RowUuid(PROBE)),
            ..Default::default()
        },
    ))?;
    let count = db.read(&db.prepare_query(&Query::from("events").count())?)?;
    if count.len() != 1 || count[0].cell("count") != Some(CoreValue::U64(config.rows as u64)) {
        return Err("seed row-count oracle failed".into());
    }
    block_on(db.close())?;
    Ok(())
}

async fn connect(server: &JazzServer, schema: &Schema, user: usize) -> Result<JazzClient> {
    let mut context = server.make_client_context_for_user(
        schema.clone(),
        Uuid::from_u128(100 + user as u128).to_string(),
    );
    // Exercise ordinary authenticated sessions and normal row policies.
    context.backend_secret = None;
    let http = reqwest::Client::new();
    let base = format!("{}/apps/{}/accounts", context.server_url, context.app_id);
    let token = context
        .jwt_token
        .as_deref()
        .ok_or("fixture JWT is missing")?;
    let mut response = http
        .post(format!("{base}/login"))
        .bearer_auth(token)
        .send()
        .await?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        response = http
            .post(format!("{base}/register"))
            .bearer_auth(token)
            .send()
            .await?;
    }
    #[derive(serde::Deserialize)]
    struct Admission {
        account: jazz::account_registry::AccountId,
    }
    context.account_id = Some(
        response
            .error_for_status()?
            .json::<Admission>()
            .await?
            .account,
    );
    Ok(
        JazzClient::connect_with_native_transport(context, Arc::new(NativeWebSocketConnector))
            .await?,
    )
}

async fn probe(client: &JazzClient) -> Result<()> {
    // The indexed id selects the same fixture row. A fresh, true bound on
    // its known value makes each invocation a new server opening, so a warm
    // live group cannot answer it without executing this request's SQL.
    let bound = i64::try_from(NEXT_PROBE.fetch_add(1, Ordering::Relaxed))?
        .checked_add(7)
        .ok_or("point-read nonce exhausted")?;
    let query = Query::from("probes")
        .filter(eq(col("id"), lit(CoreValue::Uuid(PROBE))))
        .filter(lte(col("n"), lit(CoreValue::I64(bound))));
    let rows = tokio::time::timeout(TIMEOUT, client.query(query, ReadTier::Remote)).await??;
    if rows.len() != 1
        || rows[0].key.row_id().map(|id| *id.uuid()) != Some(PROBE)
        || rows[0].get("n") != Some(&Value::BigInt(7))
    {
        return Err(format!("point-read oracle failed: {rows:?}").into());
    }
    Ok(())
}

async fn expensive(client: &JazzClient, nonce: u64) -> Result<()> {
    // Every candidate matches a long prefix of the needle, but no row
    // contains its final 'b'. Distinct bindings avoid live-group reuse.
    let needle = format!("{}b{nonce}", "a".repeat(96));
    let query = Query::from("events")
        .filter(contains(col("payload"), lit(needle)))
        .limit(1);
    let rows = tokio::time::timeout(TIMEOUT, client.query(query, ReadTier::Remote)).await??;
    if !rows.is_empty() {
        return Err("expensive-filter oracle failed: expected no matching rows".into());
    }
    Ok(())
}

async fn write(client: &JazzClient, n: usize) -> Result<()> {
    let (_, _, tx) = client.insert(
        "writes",
        HashMap::from([("n".to_owned(), Value::BigInt(n as i64))]),
    )?;
    tokio::time::timeout(
        TIMEOUT,
        client.wait_for_transaction(
            tx.ok_or("insert did not commit")?,
            DurabilityTier::GlobalServer,
        ),
    )
    .await??;
    Ok(())
}

fn summary(mut samples: Vec<f64>) -> JsonValue {
    samples.sort_by(f64::total_cmp);
    if samples.is_empty() {
        return json!({"count": 0});
    }
    let percentile = |p: usize| samples[(samples.len() * p).div_ceil(100).saturating_sub(1)];
    json!({"count": samples.len(), "p50_ms": percentile(50), "p95_ms": percentile(95),
        "p99_ms": percentile(99), "max_ms": samples[samples.len() - 1], "samples_ms": samples})
}

async fn arm(
    server: &JazzServer,
    schema: &Schema,
    config: &Config,
    round: usize,
) -> Result<JsonValue> {
    // ABBA: idle, contended, contended, idle, then repeat. Each arm uses
    // fresh client stores and connections. Fixture and warm-up are untimed.
    let contended = matches!(round % 4, 1 | 2);
    let mut readers = Vec::new();
    for n in 0..config.readers {
        let reader = connect(server, schema, n).await?;
        probe(&reader).await?;
        readers.push(reader);
    }
    let writer = connect(server, schema, config.readers).await?;
    let mut backgrounds = Vec::new();
    let mut isolated = Vec::new();
    if contended {
        for n in 0..config.background {
            let client = connect(server, schema, config.readers + 1 + n).await?;
            let visible = tokio::time::timeout(
                TIMEOUT,
                client.query(
                    Query::from("events")
                        .filter(contains(col("payload"), lit("aaaa")))
                        .limit(1),
                    ReadTier::Remote,
                ),
            )
            .await??;
            if visible.len() != 1 {
                return Err("background client cannot read the fixture".into());
            }
            let started = Instant::now();
            expensive(&client, (round * 10_000 + n) as u64).await?;
            isolated.push(started.elapsed().as_secs_f64() * 1000.0);
            backgrounds.push(client);
        }
    }
    let barrier = Arc::new(Barrier::new(readers.len() + backgrounds.len() + 2));
    let stop = Arc::new(AtomicBool::new(false));
    let mut background_tasks = Vec::new();
    for (n, client) in backgrounds.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let stop = Arc::clone(&stop);
        background_tasks.push(tokio::task::spawn_local(async move {
            barrier.wait().await;
            let mut samples = Vec::new();
            let mut nonce = ((round + 1) as u64) << 48 | (n as u64) << 32;
            while !stop.load(Ordering::Acquire) {
                let started = Instant::now();
                expensive(&client, nonce).await?;
                samples.push(started.elapsed().as_secs_f64() * 1000.0);
                nonce += 1;
            }
            Ok::<_, Error>((samples, client))
        }));
    }
    let mut reader_tasks = Vec::new();
    for client in readers {
        let barrier = Arc::clone(&barrier);
        let requests = config.requests;
        reader_tasks.push(tokio::task::spawn_local(async move {
            barrier.wait().await;
            let mut samples = Vec::new();
            for _ in 0..requests {
                let started = Instant::now();
                probe(&client).await?;
                samples.push(started.elapsed().as_secs_f64() * 1000.0);
            }
            Ok::<_, Error>((samples, client))
        }));
    }
    let writer_barrier = Arc::clone(&barrier);
    let requests = config.requests;
    let writer_task = tokio::task::spawn_local(async move {
        writer_barrier.wait().await;
        let mut samples = Vec::new();
        for n in 0..requests {
            let started = Instant::now();
            write(&writer, round * requests + n).await?;
            samples.push(started.elapsed().as_secs_f64() * 1000.0);
        }
        Ok::<_, Error>((samples, writer))
    });
    let started = Instant::now();
    barrier.wait().await;
    let mut reads = Vec::new();
    let mut clients = Vec::new();
    for task in reader_tasks {
        let (samples, client) = task.await??;
        reads.extend(samples);
        clients.push(client);
    }
    let (writes, writer) = writer_task.await??;
    clients.push(writer);
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
    stop.store(true, Ordering::Release);
    let mut background_samples = Vec::new();
    for task in background_tasks {
        let (samples, client) = task.await??;
        background_samples.extend(samples);
        clients.push(client);
    }
    for client in clients {
        client.shutdown().await?;
    }
    Ok(
        json!({"round": round, "mode": if contended {"contended"} else {"idle"},
        "foreground_wall_ms": wall_ms,
        "foreground_ops_per_second": (reads.len() + writes.len()) as f64 * 1000.0 / wall_ms,
        "remote_read": summary(reads), "global_write": summary(writes),
        "background_query": summary(background_samples), "background_isolated": summary(isolated)}),
    )
}

async fn run(config: Config) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let schema = schema();
    eprintln!("Seeding {} synthetic rows outside measurement", config.rows);
    seed(&dir.path().join("jazz.sqlite"), &schema, &config)?;
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .with_persistent_storage()
        .with_data_dir(dir.path())
        .start()
        .await?;
    println!(
        "{}",
        json!({"benchmark": "server_read_contention", "revision": config.revision,
        "smoke": config.smoke, "debug_assertions": cfg!(debug_assertions),
        "allocator": jazz_benchmark_guard::ALLOCATOR_NAME, "rows": config.rows,
        "payload_bytes": config.payload_bytes, "readers": config.readers,
        "requests_per_client": config.requests, "background_clients": config.background,
        "rounds": config.rounds, "topology": "authenticated WebSocket clients -> durable WAL server",
        "timing": "request through remote response or global write acknowledgement; client setup and fixture excluded"})
    );
    for round in 0..config.rounds {
        eprintln!("Round {}/{}", round + 1, config.rounds);
        println!("{}", arm(&server, &schema, &config, round).await?);
    }
    // Independent-reader oracle: every measured write reached the server,
    // exactly once. A Local-tier acknowledgement would not prove this.
    let verifier = connect(&server, &schema, config.readers + config.background + 10).await?;
    let rows = tokio::time::timeout(
        TIMEOUT,
        verifier.query(Query::from("writes"), ReadTier::Remote),
    )
    .await??;
    let actual: std::collections::BTreeSet<_> = rows
        .iter()
        .map(|row| match row.get("n") {
            Some(Value::BigInt(n)) => Ok(*n),
            other => Err(format!("unexpected written value: {other:?}")),
        })
        .collect::<std::result::Result<_, _>>()?;
    let expected = config.rounds * config.requests;
    if rows.len() != expected || actual != (0..expected as i64).collect() {
        return Err("global-write oracle failed: missing, duplicate or unexpected writes".into());
    }
    verifier.shutdown().await?;
    server.shutdown().await;
    println!(
        "{}",
        json!({"verified_writes": expected, "oracles": "passed"})
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let config = Config::parse()?;
    tokio::task::LocalSet::new().run_until(run(config)).await
}
