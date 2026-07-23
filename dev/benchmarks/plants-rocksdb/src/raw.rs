//! Topology 1: RocksDB driven directly, no Jazz. One `put` per row, 500 native
//! point `get`s for the read.

use std::time::Instant;

use rocksdb::{DB as RawDb, DBCompressionType, Options as RawOptions, WriteBatch};

use crate::bench::Metrics;
use crate::dataset::Plant;

pub(crate) fn run_raw(plants: &[Plant], ids: &[String], batch: usize) -> Metrics {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut opts = RawOptions::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::Lz4);
    let db = RawDb::open(&opts, dir.path()).expect("open raw rocksdb");

    let t = Instant::now();
    for chunk in plants.chunks(batch) {
        let mut wb = WriteBatch::default();
        for plant in chunk {
            wb.put(plant.id.as_bytes(), plant.raw_value());
        }
        db.write(&wb).expect("write batch");
    }
    let write = t.elapsed();

    let t = Instant::now();
    db.flush().expect("flush");
    let flush = t.elapsed();

    // 500 native point gets.
    let t = Instant::now();
    let mut found = 0;
    for id in ids {
        if db.get(id.as_bytes()).expect("get").is_some() {
            found += 1;
        }
    }
    let read_500 = t.elapsed();

    Metrics {
        label: "raw RocksDB".to_owned(),
        rows: plants.len(),
        found,
        read_kind: "500 point gets",
        write,
        flush,
        read_500,
        per_lookup: None,
        synced_to_server: None,
    }
}
