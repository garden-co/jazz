//! Fan-out through the sync path: N client Dbs over duplex
//! transports to one server Db (benches/fanout_wire_scenario.rs). Reports
//! the server's tick time per commit (apply the upload, invalidate, build
//! every affected client's update), the clients' apply time, and the total.
//!
//! `USERS=200 cargo bench --bench fanout_wire`

use std::time::Duration;

#[path = "../tests/common/mod.rs"]
#[allow(dead_code)]
mod common;

pub struct Config {
    pub users: usize,
    pub rooms: usize,
    pub rooms_per_user: usize,
    pub messages_per_room: usize,
    pub ops: usize,
}

pub struct WireReport {
    pub clients: usize,
    pub subscriptions: usize,
    pub sync_time: Duration,
    pub rss_clients_kib: u64,
    pub rss_synced_kib: u64,
    /// (name, server tick, client ticks, total, deltas per commit)
    pub ops: Vec<(&'static str, Duration, Duration, Duration, usize)>,
}

pub fn rss_kib() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    statm
        .split_whitespace()
        .nth(1)
        .and_then(|p| p.parse::<u64>().ok())
        .unwrap_or(0)
        * 4
}

mod engine {
    use jazz::db;
    use jazz::db::SyncMessage as SyncMsg;
    use jazz::db::TransportError;
    include!("fanout_wire_scenario.rs");
}

fn main() {
    let env = |k: &str, d: usize| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    let users = env("USERS", 200);
    let cfg = Config {
        users,
        rooms: env("ROOMS", users),
        rooms_per_user: env("ROOMS_PER_USER", 5),
        messages_per_room: env("MESSAGES_PER_ROOM", 20),
        ops: env("OPS", 30),
    };
    let report = engine::run(&cfg);
    println!(
        "wire: clients={} subs={} initial sync {:?}; RSS: {} KiB for client Dbs, +{} KiB after sync ({} B/sub)",
        report.clients,
        report.subscriptions,
        report.sync_time,
        report.rss_clients_kib,
        report.rss_synced_kib,
        report.rss_synced_kib * 1024 / report.subscriptions as u64,
    );
    for (name, server, clients, total, events) in &report.ops {
        println!(
            "  {name:<22} server {server:>11.3?}  clients {clients:>11.3?}  total {total:>11.3?}  deltas {events}"
        );
    }
}
