//! Server fan-out under load: many identities subscribed on one Db, then
//! commits that touch a few, some or all of them. Built twice from one
//! scenario (benches/fanout_scenario.rs).
//! Reports subscribe cost, memory, and per-commit
//! latency including invalidation and delta delivery.
//!
//! `USERS=1000 cargo bench --bench fanout`

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

pub struct Report {
    pub subscriptions: usize,
    pub subscribe_time: Duration,
    pub rss_delta_kib: u64,
    pub ops: Vec<(&'static str, Duration, usize)>,
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
    include!("fanout_scenario.rs");
}

fn main() {
    let env = |k: &str, d: usize| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    let users = env("USERS", 1000);
    let cfg = Config {
        users,
        rooms: env("ROOMS", users),
        rooms_per_user: env("ROOMS_PER_USER", 5),
        messages_per_room: env("MESSAGES_PER_ROOM", 20),
        ops: env("OPS", 50),
    };
    let report = engine::run(&cfg);
    println!(
        "users={} rooms={} subs={} subscribe_total={:?} ({:?}/sub) rss_delta={} KiB ({} B/sub)",
        cfg.users,
        cfg.rooms,
        report.subscriptions,
        report.subscribe_time,
        report.subscribe_time / report.subscriptions as u32,
        report.rss_delta_kib,
        report.rss_delta_kib * 1024 / report.subscriptions as u64
    );
    for (name, median, events) in &report.ops {
        println!("  {name:<22} median {median:>12.3?}  deltas/commit {events}");
    }
}
