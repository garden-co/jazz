//! A session's claims changing re-runs only that session's results, not
//! every live result on the node.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{compile_schema, read_and_allow_all_writes, session_eq};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("documents")
                    .column("region", ColumnType::Text)
                    .policies(read_and_allow_all_writes(session_eq(
                        "region",
                        &["claims", "region"],
                    ))),
            )
            .build(),
    )
}

fn claims(region: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "\0claims:region".to_owned(),
        Value::String(region.to_owned()),
    )])
}

fn user(n: u32) -> AuthorSubject {
    let mut bytes = [0x80u8; 16];
    bytes[12..].copy_from_slice(&n.to_be_bytes());
    AuthorSubject::for_test_bytes(bytes)
}

fn setup(users: u32) -> (Db, Vec<jazz::db::SubscriptionStream>) {
    let schema = schema();
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    let db = block_on(Db::open(config)).unwrap();
    for n in 0..500u32 {
        let mut bytes = [0u8; 16];
        bytes[12..].copy_from_slice(&n.to_be_bytes());
        let region = format!("r{}", n % 10);
        db.seed_settled_mergeable_for_bootstrap(
            "documents",
            RowUuid::from_bytes(bytes),
            AuthorSubject::SYSTEM,
            [("region".to_owned(), Value::String(region))].into(),
        )
        .unwrap();
    }
    let prepared = db.prepare_query(&Query::from("documents")).unwrap();
    let mut streams = Vec::new();
    for n in 0..users {
        db.set_identity_claims(user(n), claims(&format!("r{}", n % 10)));
        streams.push(
            block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), user(n))).unwrap(),
        );
    }
    for stream in &mut streams {
        while stream.try_next_event().is_some() {}
    }
    (db, streams)
}

fn deltas(stream: &mut jazz::db::SubscriptionStream) -> (usize, usize) {
    let (mut added, mut removed) = (0, 0);
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta {
            added: a,
            removed: r,
            ..
        } = event
        {
            added += a.len();
            removed += r.len();
        }
    }
    (added, removed)
}

#[test]
fn only_the_changed_sessions_results_move() {
    let (db, mut streams) = setup(20);
    db.set_identity_claims(user(3), claims("r7"));
    assert_eq!(
        deltas(&mut streams[3]),
        (50, 50),
        "user 3 now sees region r7's 50 documents instead of r3's"
    );
    for (n, stream) in streams.iter_mut().enumerate() {
        if n != 3 {
            assert_eq!(deltas(stream), (0, 0), "user {n} is untouched");
        }
    }
}

/// Timing probe (run with `--release -- --nocapture`): one claims change
/// with 1,000 live per-identity results of 50 rows each.
#[test]
fn claims_change_cost_with_many_sessions() {
    let (db, mut streams) = setup(1000);
    let mut times = Vec::new();
    for round in 0..20u32 {
        let started = std::time::Instant::now();
        db.set_identity_claims(user(round), claims(&format!("r{}", (round + 1) % 10)));
        times.push(started.elapsed());
        assert_eq!(deltas(&mut streams[round as usize]), (50, 50));
    }
    times.sort();
    eprintln!(
        "claims change with 1,000 live results: median {:?}",
        times[times.len() / 2]
    );
}
