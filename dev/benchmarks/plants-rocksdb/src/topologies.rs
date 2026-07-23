//! The three benchmarked topologies and the dispatcher over the selected set.

use std::time::Instant;

use jazz_tools::server::JazzServer;
use jazz_tools::{AppContext, ClientStorage, DurabilityTier, JazzClient};
use rocksdb::{DB as RawDb, DBCompressionType, Options as RawOptions, WriteBatch};

use crate::config::{BATCH, Config, Topology};
use crate::dataset::{Plant, dir_size};
use crate::jazz::{
    point_lookup, publish_allow_all, row_count, schema, wait_edge_ready, write_plants,
};
use crate::report::Row;

pub(crate) async fn run_selected(
    cfg: &Config,
    plants: &[Plant],
    ids: &[String],
    logical: u64,
) -> Vec<Row> {
    let mut out = Vec::new();
    for topology in Topology::ALL {
        if !cfg.runs(topology) {
            continue;
        }
        out.push(match topology {
            Topology::Raw => run_raw(plants, ids, logical),
            Topology::Local => run_local(plants, ids, logical, cfg.progress).await,
            Topology::Server => run_server(plants, ids, logical, cfg.progress).await,
        });
    }
    out
}

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
    for id in ids {
        let _ = db.get(id.as_bytes()).expect("get");
    }
    let lookup = t.elapsed() / ids.len().max(1) as u32;

    Row {
        topology: "raw RocksDB",
        write,
        rows: plants.len(),
        lookup,
        tier: None,
        physical: dir_size(dir.path()),
        logical,
    }
}

async fn run_local(plants: &[Plant], ids: &[String], logical: u64, progress: bool) -> Row {
    let dir = tempfile::tempdir().expect("client tempdir");
    // Serverless client: empty server_url => no upstream; Persistent => RocksDB.
    let mut ctx = AppContext::test(schema());
    ctx.storage = ClientStorage::Persistent;
    ctx.data_dir = dir.path().to_path_buf();
    let client = JazzClient::connect(ctx)
        .await
        .expect("connect local client");

    let write = write_plants(&client, plants, None, progress).await;
    let lookup = point_lookup(&client, ids, DurabilityTier::Local).await;
    client.shutdown().await.expect("shutdown local client");

    Row {
        topology: "Jazz + RocksDB (local)",
        write,
        rows: plants.len(),
        lookup,
        tier: Some("Local"),
        physical: dir_size(dir.path()),
        logical,
    }
}

async fn run_server(plants: &[Plant], ids: &[String], logical: u64, progress: bool) -> Row {
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

    let mut ctx = server.make_client_context_for_user(schema, "bench");
    ctx.storage = ClientStorage::Persistent;
    let client = JazzClient::connect(ctx).await.expect("connect client");
    wait_edge_ready(&client).await;

    let write = write_plants(&client, plants, Some(DurabilityTier::EdgeServer), progress).await;
    let lookup = point_lookup(&client, ids, DurabilityTier::EdgeServer).await;

    // Confirm the server durably holds every row.
    let synced = row_count(&client, DurabilityTier::EdgeServer).await;
    assert_eq!(
        synced,
        plants.len(),
        "server persisted {synced}/{} rows — sync did not fully settle",
        plants.len()
    );
    let physical = dir_size(server.data_dir());
    server.shutdown().await;

    Row {
        topology: "Jazz → Jazz Server (RocksDB)",
        write,
        rows: plants.len(),
        lookup,
        tier: Some("EdgeServer"),
        physical,
        logical,
    }
}
