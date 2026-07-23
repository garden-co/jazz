//! Topology 3: a Jazz client that syncs over a real localhost WebSocket into a
//! RocksDB-backed Jazz Server (`jazz_server::LoopbackWebSocketServer`). Writes
//! are timed through sync settlement; the by-id read runs locally on the client.
//!
//! NOTE: the server shell's sync ingestion is super-linear in rows already
//! stored, so cap this topology at `--limit 25000` (~4.5 min, fully synced);
//! the full 93k would take ~40 min. That per-batch cost is the server ingest
//! path, not this harness.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use jazz::db::{Db, DbIdentity, WireTransportAdapter};
use jazz::groove::storage::RocksDbStorage;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::query::Query;
use jazz_server::loopback_websocket::{LoopbackWebSocketServer, LoopbackWebSocketServerConfig};
use tungstenite::stream::MaybeTlsStream;

use crate::bench::{Metrics, jazz_read_by_id, open_rocks_db, open_rocks_db_as, schema};
use crate::dataset::{Plant, TABLE};
use crate::ws_transport::WsClientTransport;

pub(crate) fn run_server(plants: &[Plant], ids: &[String], batch: usize) -> Metrics {
    if plants.len() > 25_000 {
        eprintln!(
            "  note: the server topology's sync ingestion is super-linear; {} rows will take \
             far longer than the ~4.5 min for 25k (full 93k ≈ 40 min). Consider --limit 25000.",
            plants.len()
        );
    }
    let schema = schema();

    // --- Start a RocksDB-backed Jazz Server on an ephemeral localhost port. ---
    let server_dir = tempfile::tempdir().expect("server tempdir");
    let config = LoopbackWebSocketServerConfig::persistent_data_dir(
        schema.clone(),
        DbIdentity {
            node: NodeUuid::from_bytes([9u8; 16]),
            author: AuthorId::SYSTEM,
        },
        server_dir.path(),
    );
    let server = LoopbackWebSocketServer::start_with_config(config).expect("start jazz server");
    let addr = server.local_addr();

    // --- Client Db (RocksDB) connected upstream to the server via WebSocket.
    // The client authors commits under the *same* id the server admits for the
    // anonymous session, so its uploads are trusted (and not dropped). ---
    let client_author = jazz_server::auth_admission::author_id_from_subject("anonymous");
    let client_dir = tempfile::tempdir().expect("client tempdir");
    let client = open_rocks_db_as(&schema, client_dir.path(), [2u8; 16], client_author);

    let (socket, _resp) = tungstenite::connect(format!("ws://{addr}/sync")).expect("connect ws");
    if let MaybeTlsStream::Plain(stream) = socket.get_ref() {
        stream
            .set_nonblocking(true)
            .expect("set client socket non-blocking");
    }
    let sent = Arc::new(AtomicU64::new(0));
    let recv = Arc::new(AtomicU64::new(0));
    let transport = WsClientTransport {
        socket,
        inbox: VecDeque::new(),
        sent: Arc::clone(&sent),
        recv: Arc::clone(&recv),
        closed: false,
    };

    // Connect upstream and let the catalogue handshake settle before writing, so
    // schema negotiation completes against the empty server first.
    let _upstream = client.connect_upstream(Box::new(WireTransportAdapter::current(transport)));
    for _ in 0..50 {
        client.tick().expect("handshake tick");
        std::thread::sleep(Duration::from_millis(2));
    }

    // --- Write and ship incrementally: commit a batch, then tick to stream it to
    // the server before the next batch. This paces uploads to the server's ingest
    // speed (the reliable transport applies backpressure) instead of blasting the
    // whole dataset in one burst. Timed through final sync settlement so the
    // server durably holds every row. ---
    let t = Instant::now();
    let total_batches = plants.len().div_ceil(batch);
    let progress = std::env::var("JZ_PROGRESS").is_ok();
    for (i, chunk) in plants.chunks(batch).enumerate() {
        client
            .transaction(|tx| {
                for plant in chunk {
                    tx.insert(TABLE, plant.cells())?;
                }
                Ok(())
            })
            .expect("commit batch");
        client.tick().expect("ship batch");
        if progress && (i + 1).is_multiple_of(5) {
            eprintln!(
                "    write: batch {}/{} shipped  sent={} recv={}  elapsed={:.0}s",
                i + 1,
                total_batches,
                sent.load(Ordering::Relaxed),
                recv.load(Ordering::Relaxed),
                t.elapsed().as_secs_f64()
            );
        }
    }
    // Drive remaining sync to quiescence.
    drain_sync(&client, &sent, &recv);
    let write = t.elapsed();

    // --- Read the sampled ids locally on the client after sync (warm). ---
    let (read_500, found, per_lookup) = jazz_read_by_id(&client, ids);

    client.close().expect("close client db");
    server.shutdown();

    // Self-check: reopen the server's RocksDB directory and confirm it durably
    // holds every uploaded row (proves the write path really reached the server).
    let verify = open_rocks_db(&schema, server_dir.path());
    let synced = verify
        .read(
            &verify
                .prepare_query(&Query::from(TABLE))
                .expect("prepare scan"),
        )
        .expect("scan server")
        .len();
    verify.close().expect("close verify db");

    Metrics {
        label: "Jazz client -> Jazz Server (RocksDB)".to_owned(),
        rows: plants.len(),
        found,
        read_kind: "warm in_list",
        write,
        flush: Duration::ZERO,
        read_500,
        per_lookup: Some(per_lookup),
        synced_to_server: Some(synced),
    }
}

/// Tick the client until sync is quiescent: no frame sent or received for
/// `IDLE_TICKS` consecutive ticks (or a real stall past `STALL_TIMEOUT`).
fn drain_sync(client: &Db<RocksDbStorage>, sent: &Arc<AtomicU64>, recv: &Arc<AtomicU64>) {
    // Quiescence: no frame moves for `IDLE_TICKS` ticks. The server-shell sync
    // path is slow (~seconds per 1000-row batch) but keeps acking, so frames
    // move well within this window; the guard below only trips on a real stall.
    const IDLE_TICKS: u32 = 150;
    const STALL_TIMEOUT: Duration = Duration::from_secs(120);
    let mut last = (sent.load(Ordering::Relaxed), recv.load(Ordering::Relaxed));
    let mut idle = 0u32;
    let start = Instant::now();
    let mut last_progress = Instant::now();
    let mut last_log = Instant::now();
    loop {
        client.tick().expect("drain tick");
        std::thread::sleep(Duration::from_millis(10));
        let now = (sent.load(Ordering::Relaxed), recv.load(Ordering::Relaxed));
        if now == last {
            idle += 1;
        } else {
            idle = 0;
            last_progress = Instant::now();
        }
        last = now;
        if std::env::var("JZ_PROGRESS").is_ok() && last_log.elapsed() > Duration::from_secs(1) {
            eprintln!(
                "    drain: sent={} recv={} idle={idle} elapsed={:.0}s",
                now.0,
                now.1,
                start.elapsed().as_secs_f64()
            );
            last_log = Instant::now();
        }
        if idle >= IDLE_TICKS {
            break;
        }
        if last_progress.elapsed() > STALL_TIMEOUT {
            eprintln!("  warning: sync stalled (no progress for {STALL_TIMEOUT:?}); giving up");
            break;
        }
    }
}
