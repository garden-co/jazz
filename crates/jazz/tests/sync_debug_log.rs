//! Sync debug logging: at `debug`, target `jazz::sync` names each message
//! crossing a link (its hop, kind, read and size) and each subscription
//! delivery, so a host's log shows why a first callback waited. Nothing is
//! computed when debug logging is off.

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::common::{allow_all_policies, compile_schema};
use duplex_transport::duplex;
use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

/// Keeps every `jazz::sync` event at or above `level` as its fields.
struct Capture {
    level: Level,
    events: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
}

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(
            field.name().to_owned(),
            format!("{value:?}").trim_matches('"').to_owned(),
        );
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
}

impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "jazz::sync" && *metadata.level() <= self.level
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));
        self.events.lock().unwrap().push(fields);
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("label", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

/// Subscribe a fresh client to a one-row server under `level`, and return
/// what was logged.
fn logged_subscription(level: Level) -> Vec<BTreeMap<String, String>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let capture = Capture {
        level,
        events: events.clone(),
    };
    tracing::subscriber::with_default(capture, || {
        let server = block_on(Db::open_history_complete(config(
            0x61,
            AuthorSubject::SYSTEM,
        )))
        .unwrap();
        server
            .seed_settled_mergeable_for_bootstrap(
                "items",
                RowUuid::from_bytes([0x62; 16]),
                AuthorSubject::SYSTEM,
                BTreeMap::from([("label".to_owned(), Value::String("a".to_owned()))]),
            )
            .unwrap();
        let client = block_on(Db::open(config(
            0x63,
            AuthorSubject::for_test_bytes([0x63; 16]),
        )))
        .unwrap();
        let (client_side, server_side) = duplex();
        let _upstream = block_on(client.connect_upstream(client_side));
        server
            .accept_subscriber(server_side, AuthorSubject::SYSTEM)
            .unwrap();
        let prepared = client.prepare_query(&Query::from("items")).unwrap();
        let mut stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
        for _ in 0..5 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        block_on(client.tick()).unwrap();
        assert!(stream.try_next_event().is_some());
    });
    let events = events.lock().unwrap().clone();
    events
}

fn has(events: &[BTreeMap<String, String>], pairs: &[(&str, &str)]) -> bool {
    events.iter().any(|event| {
        pairs
            .iter()
            .all(|(key, value)| event.get(*key).is_some_and(|got| got == value))
    })
}

#[test]
fn debug_logging_names_each_sync_message_and_the_subscriptions_delivery() {
    let events = logged_subscription(Level::DEBUG);
    assert!(
        has(&events, &[("hop", "client→upstream"), ("kind", "Read")]),
        "the client's read goes up: {events:#?}"
    );
    assert!(has(&events, &[("hop", "session→store"), ("kind", "Read")]));
    assert!(has(&events, &[("hop", "store→session"), ("kind", "Rows")]));
    assert!(has(
        &events,
        &[("hop", "upstream→client"), ("kind", "Settled")]
    ));
    assert!(
        has(
            &events,
            &[("message", "subscription delivered"), ("table", "items")]
        ),
        "the opening is logged: {events:#?}"
    );
    let read = events
        .iter()
        .find(|event| event.get("kind").is_some_and(|kind| kind == "Read"))
        .unwrap();
    assert!(
        read.get("bytes")
            .is_some_and(|bytes| bytes.parse::<usize>().unwrap() > 0),
        "a message's size is logged: {read:?}"
    );
}

#[test]
fn sync_logging_is_silent_above_debug() {
    assert!(logged_subscription(Level::INFO).is_empty());
}
