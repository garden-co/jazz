//! Topology 2: Jazz over RocksDB. Batched transactions, then the sampled ids
//! fetched with one membership read (plus a per-id point-lookup probe).

use std::time::Instant;

use crate::bench::{EbsDelay, Metrics, jazz_read_by_id, open_rocks_db, schema};
use crate::dataset::{Plant, TABLE};

pub(crate) fn run_jazz(plants: &[Plant], ids: &[String], batch: usize, ebs: EbsDelay) -> Metrics {
    let schema = schema();
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();

    let db = open_rocks_db(&schema, &path);
    let t = Instant::now();
    for chunk in plants.chunks(batch) {
        db.transaction(|tx| {
            for plant in chunk {
                tx.insert(TABLE, plant.cells())?;
            }
            Ok(())
        })
        .expect("commit batch");
        ebs.charge();
    }
    let write = t.elapsed();

    let t = Instant::now();
    db.close().expect("close db");
    let flush = t.elapsed();
    drop(db);

    // Reopen cold and read the sampled ids from cold storage.
    let cold = open_rocks_db(&schema, &path);
    let (read_500, found, per_lookup) = jazz_read_by_id(&cold, ids);
    cold.close().expect("close cold db");

    Metrics {
        label: "Jazz + RocksDB".to_owned(),
        rows: plants.len(),
        found,
        read_kind: "cold in_list",
        write,
        flush,
        read_500,
        per_lookup: Some(per_lookup),
        synced_to_server: None,
    }
}
