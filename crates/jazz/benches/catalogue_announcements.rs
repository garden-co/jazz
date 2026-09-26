//! Catalogue announcement checks at the public node/transport boundary.
//! This isolates a repeated sync phase; it is not a browser startup benchmark.
use std::{cell::Cell, rc::Rc, time::Instant};

use jazz::{
    block_on,
    db::{Node, Transport},
    groove::storage::MemoryStorage,
    ids::{AuthorSubject, NodeUuid},
    node::{CommitUnitTrust, NodeState},
    protocol::SyncMessage,
    schema::JazzSchema,
    tools::{ColumnType, SchemaBuilder, TableSchemaBuilder},
    wire::TransportError,
};

struct CountAnnouncements(Rc<Cell<usize>>);

impl Transport for CountAnnouncements {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if matches!(message, SyncMessage::CatalogueSnapshot(_)) {
            self.0.set(self.0.get() + 1);
        }
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        None
    }
}

fn run(tables: usize, turns: usize) {
    let start = Instant::now();
    let mut builder = SchemaBuilder::new();
    for table in 0..tables {
        let mut definition = TableSchemaBuilder::new(&format!("items_{table}"));
        for column in 0..16 {
            definition = definition.column(&format!("field_{column}"), ColumnType::Text);
        }
        builder = builder.table(definition);
    }
    let schema = JazzSchema::new(&builder.build()).unwrap();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = MemoryStorage::new(&refs).unwrap();
    let node = Node::new(
        block_on(NodeState::new_with_shared_test_catalogue(
            NodeUuid::from_bytes([0x41; 16]),
            schema,
            storage,
        ))
        .unwrap(),
    );
    let setup_ms = start.elapsed().as_secs_f64() * 1000.;
    let announcements = Rc::new(Cell::new(0));
    let connection = node.accept_subscriber_with_trust(
        Box::new(CountAnnouncements(announcements.clone())),
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    let initial = Instant::now();
    block_on(node.tick()).unwrap();
    let initial_ms = initial.elapsed().as_secs_f64() * 1000.;
    let repeated = Instant::now();
    for _ in 0..turns {
        block_on(node.tick()).unwrap();
    }
    let repeated_ms = repeated.elapsed().as_secs_f64() * 1000.;
    println!(
        "{{\"benchmark\":\"catalogue_announcements\",\"tables\":{tables},\"turns\":{turns},\"setup_ms\":{setup_ms:.3},\"initial_ms\":{initial_ms:.3},\"repeated_ms\":{repeated_ms:.3},\"announcements\":{}}}",
        announcements.get(),
    );
    drop(connection);
}

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let turns = std::env::var("JAZZ_CATALOGUE_TURNS")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(1000);
    for tables in [1, 16, 64] {
        run(tables, turns);
    }
}
