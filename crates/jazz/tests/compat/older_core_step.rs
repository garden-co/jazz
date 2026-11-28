//! One step of the older-core round trip (jazz#6367), shared by this tree
//! and an older release's: `dev/gates/commit-seq-older-core.mjs` includes
//! this file into an older tag's `tests/durable_file.rs` and alternates the
//! two builds on one store. Only APIs both have: `durable_file.rs`'s own
//! `config`, `item` and `row`, `Db::open_at`, `insert`, `seq_high_water`.
//!
//! With `JAZZ_OLDER_CORE_STORE` set, it opens that store, inserts
//! `JAZZ_OLDER_CORE_COUNT` rows from `JAZZ_OLDER_CORE_FROM` with the clock
//! `JAZZ_OLDER_CORE_CLOCK_AHEAD_MS` ahead of the wall clock, closes it,
//! and prints one line with what it saw. Without it, it does nothing.

use super::*;

fn env(name: &str) -> u64 {
    std::env::var(name).map_or(0, |value| value.parse().unwrap())
}

fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[test]
fn older_core_round_trip_step() {
    let Ok(path) = std::env::var("JAZZ_OLDER_CORE_STORE") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let (from, count) = (env("JAZZ_OLDER_CORE_FROM"), env("JAZZ_OLDER_CORE_COUNT"));
    let ahead = env("JAZZ_OLDER_CORE_CLOCK_AHEAD_MS");
    let mut config = config(1);
    if ahead > 0 {
        config.clock = Some(Box::new(move || wall_ms() + ahead));
    }
    let (open, last) = {
        let db = block_on(Db::open_at(config, &path, FileOptions::default())).unwrap();
        let open = db.seq_high_water();
        let mut last = 0;
        for n in from..from + count {
            last = block_on(db.insert(
                "items",
                item(n),
                InsertOptions {
                    row_id: Some(row(n)),
                    ..Default::default()
                },
            ))
            .unwrap()
            .mergeable_tx_id();
        }
        (open, last)
    };
    let conn = rusqlite::Connection::open(&path).unwrap();
    let seq_row: i64 = conn
        .query_row(
            "SELECT COALESCE((SELECT v FROM _jazz_meta WHERE k = 'commit_seq.v1'), -1)",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let triggers: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name GLOB '*__seq_v1_*'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    println!("JAZZ_OLDER_CORE_STEP open={open} last={last} row={seq_row} triggers={triggers}");
}
