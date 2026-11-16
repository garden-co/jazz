//! Spike: latency of thin-client transactions run in the core today versus
//! run in an in-memory Jazz client outside the core (a worker proxy or a
//! Vercel function holding the thin session), which uploads the finished
//! transaction and waits for the core's verdict.
//!
//! Not a regression test: it is `#[ignore]`d and prints a table. Run with
//!
//! ```text
//! cargo test -p jazz-app-support --profile perf --test thin_latency -- --ignored --nocapture
//! ```
//!
//! Topology (all in one process, over real TCP with injected delay):
//!
//! ```text
//! today:        driver --thin WS--> [delay RTT_t] --> jazz-server core
//! out-of-core:  driver --(sleep RTT_c per request)--> Jazz client (in-memory)
//!                        --sync WS--> [delay RTT_p] --> jazz-server core
//! ```
//!
//! The thin hop into the proxy is simulated by sleeping one RTT per thin
//! request, since the proxy would answer each from local state (or after
//! its own round trip to the core, which is measured).

#[path = "bindings/todos.rs"]
mod bindings;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bindings::{NewTodo, Todo, TodoUpdate, todos};
use futures::{SinkExt, StreamExt};
use jazz_app_support::jazz;
use jazz_app_support::{AppId, Backend, Config, DurabilityTier, Jazz, ObjectId, ReadTier};
use jazz_server::JazzServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message as WsMessage;

const PERMISSIONS: &[u8] = include_bytes!("fixtures/todos/jazz.permissions.json");

// ---------------------------------------------------------------------------
// Delay proxy: forwards TCP to `target`, delaying each direction by
// `one_way_us` (so the round trip adds 2 × one_way_us), keeping order.

async fn delay_proxy(target: SocketAddr, one_way_us: Arc<AtomicU64>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((inbound, _)) = listener.accept().await else {
                return;
            };
            let delay = Arc::clone(&one_way_us);
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect(target).await else {
                    return;
                };
                let _ = inbound.set_nodelay(true);
                let _ = outbound.set_nodelay(true);
                let (ir, iw) = inbound.into_split();
                let (or, ow) = outbound.into_split();
                tokio::spawn(pump(ir, ow, Arc::clone(&delay)));
                tokio::spawn(pump(or, iw, delay));
            });
        }
    });
    addr
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    one_way_us: Arc<AtomicU64>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
    let writer = tokio::spawn(async move {
        while let Some((due, bytes)) = rx.recv().await {
            // Timers tick in whole milliseconds: skip them when due.
            if due > tokio::time::Instant::now() {
                tokio::time::sleep_until(due).await;
            }
            if to.write_all(&bytes).await.is_err() {
                return;
            }
        }
        let _ = to.shutdown().await;
    });
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                let due = tokio::time::Instant::now()
                    + Duration::from_micros(one_way_us.load(Ordering::Relaxed));
                if tx.send((due, buf[..n].to_vec())).is_err() {
                    break;
                }
            }
        }
    }
    drop(tx);
    let _ = writer.await;
}

fn set_rtt(knob: &AtomicU64, rtt_ms: f64) {
    knob.store((rtt_ms * 500.0) as u64, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// A minimal thin client over the real WebSocket route.

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

struct Thin {
    ws: Ws,
    next_id: u64,
}

impl Thin {
    async fn connect(
        addr: SocketAddr,
        app_id: AppId,
        admin_secret: &str,
        bundle: &str,
        schema: Vec<u8>,
    ) -> Self {
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/apps/{app_id}/ws"))
            .await
            .expect("connect thin websocket");
        let _ = ws.get_ref();
        // A trusted (admin) link served as SYSTEM, pinned to the bundle the
        // backend published: the brief says to run both paths as one
        // system identity.
        let prelude = serde_json::json!({
            "peer_identity": r#"["urn:jazz:system","system"]"#,
            "auth": { "admin_secret": admin_secret },
            "bundle": bundle,
        });
        ws.send(WsMessage::Binary(prelude.to_string().into_bytes().into()))
            .await
            .unwrap();
        let hello = jazz::wire::WireFrame::Hello(jazz::wire::WireHello::current(
            jazz::wire::WirePeerRole::Client,
            jazz::wire::FEATURE_THIN_RESULTS,
        ));
        let batch = vec![jazz::wire::encode_frame(&hello).unwrap()];
        ws.send(WsMessage::Binary(
            postcard::to_allocvec(&batch).unwrap().into(),
        ))
        .await
        .unwrap();
        let reply = ws.next().await.unwrap().unwrap();
        let WsMessage::Binary(bytes) = reply else {
            panic!("expected server hello, got {reply:?}")
        };
        let frames: Vec<Vec<u8>> = postcard::from_bytes(&bytes).unwrap();
        match jazz::wire::decode_frame(&frames[0]).unwrap() {
            jazz::wire::WireFrame::Hello(_) => {}
            other => panic!("expected server hello, got {other:?}"),
        }
        let mut thin = Thin { ws, next_id: 1 };
        let body = thin.call(jazz::thin::ThinOp::Schema { schema }).await;
        assert_eq!(body, jazz::thin::ThinBody::Ok, "schema bound");
        thin
    }

    /// One request, awaited: a thin client that awaits each call.
    async fn call(&mut self, op: jazz::thin::ThinOp) -> jazz::thin::ThinBody {
        let id = self.next_id;
        self.next_id += 1;
        let frame = jazz::wire::encode_frame(&jazz::wire::WireFrame::ThinRequest(
            jazz::thin::ThinRequest { id, op },
        ))
        .unwrap();
        for part in jazz::thin::thin_wire_frames(frame).unwrap() {
            let batch = postcard::to_allocvec(&vec![part]).unwrap();
            self.ws.send(WsMessage::Binary(batch.into())).await.unwrap();
        }
        loop {
            let message = tokio::time::timeout(Duration::from_secs(30), self.ws.next())
                .await
                .expect("thin answer in time")
                .unwrap()
                .unwrap();
            let WsMessage::Binary(bytes) = message else {
                continue;
            };
            let frames: Vec<Vec<u8>> = postcard::from_bytes(&bytes).unwrap();
            for frame in frames {
                if let jazz::wire::WireFrame::ThinResponse(response) =
                    jazz::wire::decode_frame(&frame).unwrap()
                    && response.id == id
                {
                    return response.body;
                }
            }
        }
    }
}

fn todo_cells(title: &str, done: bool) -> Vec<u8> {
    use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
    let descriptor =
        RecordDescriptor::new([("title", ValueType::String), ("done", ValueType::Bool)]);
    let record = OwnedRecord::new(
        descriptor
            .create(&[Value::String(title.to_owned()), Value::Bool(done)])
            .unwrap(),
        descriptor,
    );
    jazz::binding_codec::encode_named_cells(&record).unwrap()
}

fn done_patch(done: bool) -> Vec<u8> {
    use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
    let descriptor = RecordDescriptor::new([("done", ValueType::Bool)]);
    let record = OwnedRecord::new(descriptor.create(&[Value::Bool(done)]).unwrap(), descriptor);
    jazz::binding_codec::encode_named_cells(&record).unwrap()
}

fn point_query(id: ObjectId) -> Vec<u8> {
    let query = todos::query().id(id).limit(1).into_untyped().unwrap();
    postcard::to_allocvec(&query).unwrap()
}

fn row_count(body: &jazz::thin::ThinBody) -> usize {
    let jazz::thin::ThinBody::Rows { rows, .. } = body else {
        panic!("expected rows, got {body:?}")
    };
    jazz::binding_codec::decode_rows(rows)
        .unwrap()
        .iter()
        .map(|batch| batch.rows.len())
        .sum()
}

// ---------------------------------------------------------------------------
// Workloads and timings.

#[derive(Clone, Copy, Debug, PartialEq)]
enum Workload {
    /// k inserts.
    WriteOnly,
    /// k × (point read of an existing row, then update it).
    ReadWrite,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Mergeable,
    Serializable,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Mergeable => "mergeable",
            Kind::Serializable => "serializable",
        }
    }
}

#[derive(Default, Clone, Copy)]
struct Timing {
    begin: Duration,
    reads: Duration,
    writes: Duration,
    commit: Duration,
}

impl Timing {
    fn total(&self) -> Duration {
        self.begin + self.reads + self.writes + self.commit
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

async fn thin_tx(
    thin: &mut Thin,
    workload: Workload,
    kind: Kind,
    k: usize,
    rows: &[ObjectId],
    seq: &mut u64,
) -> Timing {
    use jazz::thin::{ThinBody, ThinOp};
    let mut t = Timing::default();
    *seq += 1;
    let tx = format!(
        "{:032x}",
        0x0190_f3e4_0000_7000_8000_0000_0000_0000u128 + *seq as u128
    );
    let start = Instant::now();
    let begun = thin
        .call(ThinOp::Begin {
            transaction: tx.clone(),
            kind: kind.name().to_owned(),
        })
        .await;
    assert_eq!(begun, ThinBody::Ok, "begin");
    t.begin = start.elapsed();
    for i in 0..k {
        match workload {
            Workload::WriteOnly => {
                let s = Instant::now();
                let body = thin
                    .call(ThinOp::Insert {
                        table: "todos".into(),
                        cells: todo_cells(&format!("thin {i}"), false),
                        options: Vec::new(),
                        transaction: Some(tx.clone()),
                    })
                    .await;
                assert!(matches!(body, ThinBody::Staged { .. }), "{body:?}");
                t.writes += s.elapsed();
            }
            Workload::ReadWrite => {
                let id = rows[i];
                let s = Instant::now();
                let body = thin
                    .call(ThinOp::Query {
                        query: point_query(id),
                        options: Vec::new(),
                        transaction: Some(tx.clone()),
                    })
                    .await;
                assert_eq!(row_count(&body), 1, "thin read sees the seeded row");
                t.reads += s.elapsed();
                let s = Instant::now();
                let body = thin
                    .call(ThinOp::Update {
                        table: "todos".into(),
                        row: *id.uuid().as_bytes(),
                        patch: done_patch(*seq % 2 == 0),
                        options: Vec::new(),
                        transaction: Some(tx.clone()),
                    })
                    .await;
                assert!(matches!(body, ThinBody::Staged { .. }), "{body:?}");
                t.writes += s.elapsed();
            }
        }
    }
    let s = Instant::now();
    let body = thin.call(ThinOp::Commit { transaction: tx }).await;
    assert!(matches!(body, ThinBody::Written { .. }), "commit: {body:?}");
    t.commit = s.elapsed();
    t
}

/// How the out-of-core client serves a mergeable read of a row it may not
/// hold: a client's mergeable transaction reads only local data, so the
/// proxy fetches the row from the core first (a remote read outside the
/// transaction, which lands it in the replica), then reads in the
/// transaction.
async fn proxy_tx(
    proxy: &Jazz,
    workload: Workload,
    kind: Kind,
    k: usize,
    rows: &[ObjectId],
    seq: &mut u64,
    client_rtt: Duration,
    check_ryow: bool,
) -> Timing {
    let hop = || async {
        if !client_rtt.is_zero() {
            tokio::time::sleep(client_rtt).await;
        }
    };
    *seq += 1;
    let mut t = Timing::default();
    let durable = proxy.durable(DurabilityTier::GlobalServer);
    let s = Instant::now();
    hop().await;
    let tx = match kind {
        Kind::Mergeable => durable.begin_transaction().await.unwrap(),
        Kind::Serializable => durable.begin_serializable_transaction().await.unwrap(),
    };
    t.begin = s.elapsed();
    for i in 0..k {
        match workload {
            Workload::WriteOnly => {
                let s = Instant::now();
                hop().await;
                tx.insert(NewTodo::new(format!("proxy {i}"), false))
                    .await
                    .unwrap();
                t.writes += s.elapsed();
            }
            Workload::ReadWrite => {
                let id = rows[i];
                let s = Instant::now();
                hop().await;
                if kind == Kind::Mergeable {
                    // Cold: the row is not in the proxy's replica yet.
                    let fetched = proxy
                        .all_with(todos::query().id(id).limit(1), ReadTier::Remote)
                        .await
                        .unwrap();
                    assert_eq!(fetched.len(), 1, "remote fetch finds the seeded row");
                }
                let read = tx.get::<Todo>(id).await.unwrap();
                assert!(
                    read.is_some(),
                    "{kind:?} read in the proxy's transaction sees the row"
                );
                t.reads += s.elapsed();
                let s = Instant::now();
                hop().await;
                tx.update(
                    id,
                    TodoUpdate {
                        done: Some(*seq % 2 == 0),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                t.writes += s.elapsed();
                if check_ryow {
                    // Read-your-own-write, served by the proxy alone.
                    let s = Instant::now();
                    let again = tx.get::<Todo>(id).await.unwrap().unwrap();
                    assert_eq!(again.done, *seq % 2 == 0);
                    eprintln!(
                        "    ryow ({kind:?}): {:.2} ms in-proxy (no client hop counted)",
                        ms(s.elapsed())
                    );
                }
            }
        }
    }
    let s = Instant::now();
    hop().await;
    tx.commit().await.expect("core accepted the upload");
    t.commit = s.elapsed();
    t
}

fn median(mut v: Vec<Timing>) -> Timing {
    v.sort_by_key(|t| t.total());
    v[v.len() / 2]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "latency spike: run by hand with --ignored --nocapture"]
async fn thin_transaction_latency_in_core_vs_out_of_core() {
    let reps: usize = std::env::var("REPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let ks: Vec<usize> = std::env::var("KS")
        .ok()
        .map(|v| v.split(',').map(|k| k.parse().unwrap()).collect())
        .unwrap_or(vec![1, 10, 100]);
    // (client↔edge RTT, edge↔core RTT) in ms.
    let topologies: Vec<(f64, f64)> = std::env::var("TOPOLOGIES")
        .ok()
        .map(|v| {
            v.split(';')
                .map(|p| {
                    let (a, b) = p.split_once(',').unwrap();
                    (a.parse().unwrap(), b.parse().unwrap())
                })
                .collect()
        })
        .unwrap_or(vec![
            (0.0, 0.0),
            (20.0, 1.0),
            (20.0, 5.0),
            (20.0, 20.0),
            (80.0, 1.0),
            (80.0, 20.0),
        ]);

    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let server_addr: SocketAddr = server
        .base_url()
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .parse()
        .unwrap();

    // Seed the core directly (no delay).
    let seeder = bindings::open(
        Config::new(app_id).server(server.base_url()).backend(
            Backend::new(server.backend_secret(), server.admin_secret(), "seeder")
                .permissions(PERMISSIONS),
        ),
    )
    .await
    .unwrap();
    let per_rep_rows: usize = ks.iter().sum::<usize>();
    let needed = per_rep_rows * reps * topologies.len() * 2 + 100;
    let mut seeded = Vec::with_capacity(needed);
    let t0 = Instant::now();
    while seeded.len() < needed {
        let batch = (needed - seeded.len()).min(500);
        let ids = seeder
            .transaction(move |tx| async move {
                let mut ids = Vec::new();
                for i in 0..batch {
                    ids.push(tx.insert(NewTodo::new(format!("seed {i}"), false))?.id);
                }
                Ok(ids)
            })
            .await
            .unwrap();
        seeded.extend(ids);
    }
    seeder.wait_for_sync().await.unwrap();
    eprintln!(
        "seeded {} rows in {:.1}s",
        seeded.len(),
        t0.elapsed().as_secs_f64()
    );
    let bundle = seeder.bundle().expect("a backend pins a bundle").to_owned();
    let schema = seeder
        .with_client(|client| async move { Ok(serde_json::to_vec(&client.schema()?).unwrap()) })
        .await
        .unwrap();

    // Baseline thin link and out-of-core client, each behind its own delay.
    let thin_knob = Arc::new(AtomicU64::new(0));
    let thin_addr = delay_proxy(server_addr, Arc::clone(&thin_knob)).await;
    let mut thin = Thin::connect(thin_addr, app_id, server.admin_secret(), &bundle, schema).await;

    let core_knob = Arc::new(AtomicU64::new(0));
    let core_addr = delay_proxy(server_addr, Arc::clone(&core_knob)).await;
    let proxy = bindings::open(
        Config::new(app_id)
            .server(format!("http://{core_addr}"))
            .backend(
                Backend::new(server.backend_secret(), server.admin_secret(), "thin-proxy")
                    .permissions(PERMISSIONS),
            ),
    )
    .await
    .unwrap();

    // Warm up both paths (connections, statement caches). WARM_RTT sets
    // the proxy↔core RTT during warm-up, to see where read-your-own-write
    // is served.
    if let Some(rtt) = std::env::var("WARM_RTT").ok().and_then(|v| v.parse().ok()) {
        set_rtt(&core_knob, rtt);
    }
    let mut seq = 0u64;
    let mut cursor = needed - 100;
    for _ in 0..3 {
        for kind in [Kind::Mergeable, Kind::Serializable] {
            thin_tx(
                &mut thin,
                Workload::ReadWrite,
                kind,
                3,
                &seeded[cursor..],
                &mut seq,
            )
            .await;
            proxy_tx(
                &proxy,
                Workload::ReadWrite,
                kind,
                3,
                &seeded[cursor..],
                &mut seq,
                Duration::ZERO,
                true,
            )
            .await;
            cursor += 3;
            thin_tx(&mut thin, Workload::WriteOnly, kind, 3, &[], &mut seq).await;
            proxy_tx(
                &proxy,
                Workload::WriteOnly,
                kind,
                3,
                &[],
                &mut seq,
                Duration::ZERO,
                false,
            )
            .await;
        }
    }

    let mut cold = 0usize; // next never-read seeded row
    println!(
        "| client↔edge | edge↔core | workload | kind | k | ops | today: thin direct (RTT=c) | today: thin via forwarder (RTT=c+p) | out-of-core | out-of-core: begin / reads / writes / commit |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    for &(c, p) in &topologies {
        for workload in [Workload::WriteOnly, Workload::ReadWrite] {
            for kind in [Kind::Mergeable, Kind::Serializable] {
                for &k in &ks {
                    let ops = match workload {
                        Workload::WriteOnly => k,
                        Workload::ReadWrite => 2 * k,
                    };
                    let mut direct = Vec::new();
                    let mut forwarded = Vec::new();
                    let mut out = Vec::new();
                    for _ in 0..reps {
                        // Thin reads may reuse rows: the core holds them all.
                        set_rtt(&thin_knob, c);
                        direct.push(thin_tx(&mut thin, workload, kind, k, &seeded, &mut seq).await);
                        set_rtt(&thin_knob, c + p);
                        forwarded
                            .push(thin_tx(&mut thin, workload, kind, k, &seeded, &mut seq).await);
                        set_rtt(&core_knob, p);
                        let rows = &seeded[cold..];
                        if workload == Workload::ReadWrite {
                            cold += k;
                        }
                        out.push(
                            proxy_tx(
                                &proxy,
                                workload,
                                kind,
                                k,
                                rows,
                                &mut seq,
                                Duration::from_secs_f64(c / 1000.0),
                                false,
                            )
                            .await,
                        );
                    }
                    let (d, f, o) = (median(direct), median(forwarded), median(out));
                    println!(
                        "| {c} | {p} | {workload:?} | {} | {k} | {ops} | {:.1} | {:.1} | {:.1} | {:.1} / {:.1} / {:.1} / {:.1} |",
                        kind.name(),
                        ms(d.total()),
                        ms(f.total()),
                        ms(o.total()),
                        ms(o.begin),
                        ms(o.reads),
                        ms(o.writes),
                        ms(o.commit),
                    );
                    eprintln!(
                        "    thin forwarded split: begin {:.1} reads {:.1} writes {:.1} commit {:.1}",
                        ms(f.begin),
                        ms(f.reads),
                        ms(f.writes),
                        ms(f.commit)
                    );
                }
            }
        }
    }
    assert!(cold <= needed - 100, "ran out of cold rows");
}
