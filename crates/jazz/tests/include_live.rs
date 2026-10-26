//! A live result with an include sees its include targets change, and a
//! big unbounded view re-checks only the rows that reference the changed
//! target. Not ported from Jazz.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("users")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("senderId", "users")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn id(kind: u8, n: u32) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[12..].copy_from_slice(&n.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn setup(messages: u32) -> Db {
    let schema = schema();
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    let db = block_on(Db::open(config)).unwrap();
    for u in 0..50 {
        db.seed_settled_mergeable_for_bootstrap(
            "users",
            id(1, u),
            AuthorSubject::SYSTEM,
            [("name".to_owned(), Value::String(format!("u{u}")))].into(),
        )
        .unwrap();
    }
    for n in 0..messages {
        let cells: BTreeMap<String, Value> = [
            ("senderId".to_owned(), Value::Uuid(id(1, n % 50).0)),
            ("body".to_owned(), Value::String(format!("m{n}"))),
        ]
        .into();
        db.seed_settled_mergeable_for_bootstrap("messages", id(2, n), AuthorSubject::SYSTEM, cells)
            .unwrap();
    }
    db
}

fn updated(stream: &mut jazz::db::SubscriptionStream) -> usize {
    let mut n = 0;
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta { updated, .. } = event {
            n += updated.len();
        }
    }
    n
}

#[test]
fn renaming_a_sender_updates_exactly_their_messages() {
    let db = setup(500);
    let prepared = db
        .prepare_query(&Query::from("messages").include("senderId"))
        .unwrap();
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    updated(&mut stream);
    block_on(db.update(
        "users",
        id(1, 7),
        [("name".to_owned(), Value::String("renamed".into()))].into(),
        Default::default(),
    ))
    .unwrap();
    block_on(db.tick()).unwrap();
    assert_eq!(updated(&mut stream), 10, "the 10 messages user 7 sent");
}

/// Timing probe (run with `--release -- --nocapture`): renaming one sender
/// under a live 10,000-message view with the sender included.
#[test]
fn include_target_change_cost() {
    let db = setup(10_000);
    let prepared = db
        .prepare_query(&Query::from("messages").include("senderId"))
        .unwrap();
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    updated(&mut stream);
    let mut times = Vec::new();
    for round in 0..20u32 {
        let started = std::time::Instant::now();
        block_on(db.update(
            "users",
            id(1, round),
            [("name".to_owned(), Value::String(format!("r{round}")))].into(),
            Default::default(),
        ))
        .unwrap();
        block_on(db.tick()).unwrap();
        times.push(started.elapsed());
        assert_eq!(updated(&mut stream), 200);
    }
    times.sort();
    eprintln!(
        "include target change under a 10,000-row view: median {:?}",
        times[times.len() / 2]
    );
}
