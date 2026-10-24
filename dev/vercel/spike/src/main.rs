//! Load and durability client for the Vercel spike.
//!
//! Every subcommand prints one JSON object per line, so runs can be
//! collected with `jq` and compared across hosts.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use jazz::tools::{
    AppContext, AppId, ClientStorage, ColumnType, DurabilityTier, JazzClient, ReadTier,
    SchemaBuilder, TableSchema, Value,
};
use serde_json::json;

#[derive(Parser)]
struct Cli {
    /// Jazz server base URL (http(s)://host[:port]).
    #[arg(long, env = "SPIKE_URL")]
    url: String,
    #[arg(
        long,
        env = "SPIKE_APP_ID",
        default_value = "00000000-0000-0000-0000-00000000f00d"
    )]
    app_id: String,
    #[arg(long, env = "SPIKE_ADMIN_SECRET")]
    admin_secret: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Publish the spike schema with allow-all permissions.
    Setup,
    /// Insert rows one by one and wait for each to reach the server.
    /// Prints one line per acknowledged row.
    Write {
        #[arg(long, default_value_t = 100)]
        count: usize,
        /// Pause between writes, in milliseconds.
        #[arg(long, default_value_t = 0)]
        interval_ms: u64,
        /// Run label stored in every row, to find the rows again.
        #[arg(long, default_value = "run")]
        label: String,
    },
    /// Read every row from the server and report which labels and
    /// sequence numbers it holds.
    Count {
        #[arg(long)]
        label: Option<String>,
    },
    /// Connect a fresh client and time the first server-settled read.
    FirstSync,
    /// Subscribe to every row and stay connected, printing one line per
    /// delta. Models an idle client that only listens.
    Watch {
        #[arg(long, default_value_t = 600)]
        seconds: u64,
    },
}

fn schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("events")
                .column("label", ColumnType::Text)
                .column("seq", ColumnType::Integer)
                .column("body", ColumnType::Text),
        )
        .build()
}

async fn connect(cli: &Cli, data_dir: PathBuf) -> JazzClient {
    let context = AppContext {
        app_id: AppId::from_string(&cli.app_id).expect("app id is a UUID"),
        client_id: None,
        schema: schema(),
        server_url: cli.url.clone(),
        data_dir,
        storage: ClientStorage::Memory,
        account_id: None,
        jwt_token: None,
        backend_secret: None,
        admin_secret: Some(cli.admin_secret.clone()),
    };
    JazzClient::connect_with_native_transport(
        context,
        Arc::new(jazz_native_transport::NativeWebSocketConnector),
    )
    .await
    .expect("connect")
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis()
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

async fn run(cli: Cli) {
    let dir = tempfile::tempdir().expect("temp dir");
    match &cli.command {
        Command::Setup => {
            let started = Instant::now();
            jazz_testkit::publish_allow_all_permissions(
                &cli.url,
                &cli.app_id,
                &cli.admin_secret,
                &schema(),
            )
            .await;
            println!("{}", json!({"event": "setup", "ms": ms(started.elapsed())}));
        }
        Command::Write {
            count,
            interval_ms,
            label,
        } => {
            let started = Instant::now();
            let client = connect(&cli, dir.path().to_path_buf()).await;
            println!(
                "{}",
                json!({"event": "connected", "ms": ms(started.elapsed())})
            );
            for seq in 0..*count {
                let write_started = Instant::now();
                let (_, _, tx) = client
                    .insert(
                        "events",
                        HashMap::from([
                            ("label".to_owned(), Value::Text(label.clone())),
                            ("seq".to_owned(), Value::Integer(seq as i32)),
                            ("body".to_owned(), Value::Text("x".repeat(200))),
                        ]),
                    )
                    .expect("insert");
                let acked = tokio::time::timeout(
                    Duration::from_secs(300),
                    client.wait_for_transaction(tx.expect("commits"), DurabilityTier::GlobalServer),
                )
                .await;
                let event = match acked {
                    Ok(Ok(())) => "ack",
                    Ok(Err(_)) => "rejected",
                    Err(_) => "timeout",
                };
                println!(
                    "{}",
                    json!({"event": event, "label": label, "seq": seq, "ms": ms(write_started.elapsed()), "t": now_ms()})
                );
                if *interval_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(*interval_ms)).await;
                }
            }
        }
        Command::Count { label } => {
            let client = connect(&cli, dir.path().to_path_buf()).await;
            let rows = client
                .query(jazz::query::Query::from("events"), ReadTier::Remote)
                .await
                .expect("remote read");
            let rows = jazz::tools::test_support::ordinary_rows(rows);
            let mut by_label: HashMap<String, BTreeSet<i64>> = HashMap::new();
            for (_, values) in rows {
                let (Some(Value::Text(l)), Some(Value::Integer(seq))) =
                    (values.first(), values.get(1))
                else {
                    continue;
                };
                by_label.entry(l.clone()).or_default().insert(*seq as i64);
            }
            for (l, seqs) in by_label {
                if label.as_ref().is_some_and(|want| want != &l) {
                    continue;
                }
                let max = seqs.iter().next_back().copied().unwrap_or(-1);
                let missing: Vec<i64> = (0..=max).filter(|s| !seqs.contains(s)).collect();
                println!(
                    "{}",
                    json!({"event": "count", "label": l, "rows": seqs.len(), "max_seq": max, "gaps": missing})
                );
            }
        }
        Command::FirstSync => {
            let started = Instant::now();
            let client = connect(&cli, dir.path().to_path_buf()).await;
            let connected = started.elapsed();
            let rows = client
                .query(jazz::query::Query::from("events"), ReadTier::Remote)
                .await
                .expect("remote read");
            println!(
                "{}",
                json!({
                    "event": "first_sync",
                    "connect_ms": ms(connected),
                    "settled_ms": ms(started.elapsed()),
                    "rows": jazz::tools::test_support::ordinary_rows(rows).len(),
                })
            );
        }
        Command::Watch { seconds } => {
            let started = Instant::now();
            let client = connect(&cli, dir.path().to_path_buf()).await;
            let mut stream = client
                .subscribe(jazz::query::Query::from("events"))
                .await
                .expect("subscribe");
            let deadline = tokio::time::sleep(Duration::from_secs(*seconds));
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    _ = &mut deadline => break,
                    item = stream.next() => match item {
                        Some(jazz::tools::SubscriptionStreamItem::Delta(delta)) => println!(
                            "{}",
                            json!({
                                "event": "delta",
                                "added": delta.added.len(),
                                "removed": delta.removed.len(),
                                "updated": delta.updated.len(),
                                "pending": delta.pending,
                                "since_ms": ms(started.elapsed()),
                                "t": now_ms(),
                            })
                        ),
                        Some(jazz::tools::SubscriptionStreamItem::Rejected { reason }) => {
                            println!("{}", json!({"event": "rejected", "reason": format!("{reason:?}"), "t": now_ms()}));
                        }
                        None => break,
                    },
                }
            }
        }
    }
}

fn main() {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, run(cli));
}
