//! Binding write receipts belong to their originating database node.

use crate::common::{allow_all_policies, compile_schema};
use jazz::binding_host::{Binding, HostError, Write};
use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::path::Path;

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn identity(node: u8) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author: AuthorSubject::SYSTEM,
    }
}

fn open(node: u8, path: Option<&Path>) -> Binding {
    let identity = identity(node);
    let config = DbConfig::new(schema(), identity);
    let db = match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap();
    db.set_authority().unwrap();
    Binding::new(db, identity, true)
}

fn title(value: &str) -> Vec<u8> {
    let descriptor = RecordDescriptor::new([("title", ValueType::String)]);
    let record = OwnedRecord::new(
        descriptor
            .create(&[Value::String(value.to_owned())])
            .unwrap(),
        descriptor,
    );
    jazz::binding_codec::encode_named_cells(&record).unwrap()
}

fn insert(binding: &Binding, value: &str) -> Write {
    binding
        .insert("notes", &title(value), &serde_json::Value::Null)
        .unwrap()
}

fn assert_foreign(binding: &Binding, write: &Write) {
    for tier in ["global", "local", "none"] {
        match binding.wait_for_write(write, tier) {
            Err(HostError::Argument(message)) => {
                assert_eq!(message, "write belongs to a different database node");
            }
            Err(error) => panic!("expected an owner refusal, got {error}"),
            Ok(wait) => {
                let result = block_on(wait);
                panic!("foreign receipt admitted at {tier}: {result:?}");
            }
        }
    }
}

/// Alice and Bob each commit their first write. Bob's accepted sequence must
/// not settle Alice's receipt merely because their sequence numbers coincide.
#[test]
fn a_matching_sequence_on_another_node_cannot_settle_a_write() {
    let alice = open(1, None);
    let bob = open(2, None);
    let alice_write = insert(&alice, "alice");
    let bob_write = insert(&bob, "bob");
    assert_eq!(alice_write.seq, bob_write.seq);
    for (binding, write) in [(&alice, &alice_write), (&bob, &bob_write)] {
        block_on(binding.wait_for_write(write, "global").unwrap()).unwrap();
    }
    assert_foreign(&bob, &alice_write);
    assert_foreign(&alice, &bob_write);
}

/// Alice's rejected receipt keeps its original error for Alice. Bob cannot
/// interpret that receipt as one of his own rejected transactions either.
#[test]
fn rejected_receipts_keep_their_owner_and_original_rejection() {
    let alice = open(1, None);
    let bob = open(2, None);
    let live = insert(&alice, "live");
    let rejected = alice
        .restore(
            "notes",
            &live.row_id.to_bytes(),
            Some(&title("again")),
            &serde_json::Value::Null,
        )
        .unwrap();
    let expected = rejected
        .rejected
        .as_ref()
        .expect("restoring a live row is rejected");
    let error = block_on(alice.wait_for_write(&rejected, "local").unwrap()).unwrap_err();
    match error {
        HostError::Engine(error) => {
            assert_eq!(error.code, expected.code);
            assert_eq!(error.message, expected.message);
        }
        other => panic!("expected the original rejection, got {other}"),
    }
    assert_foreign(&bob, &rejected);
}

/// Alice can use a cloned receipt through another schema handle and after
/// reopening the same durable node; no live binding pointer is needed.
#[test]
fn the_original_node_can_wait_through_a_schema_view_and_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.sqlite");
    let receipt = {
        let alice = open(1, Some(&path));
        let receipt = insert(&alice, "retained");
        let view = block_on(alice.db.register_schema_view(schema())).unwrap();
        let sibling = Binding::new(view, identity(1), true);
        let cloned = receipt.clone();
        block_on(sibling.wait_for_write(&cloned, "global").unwrap()).unwrap();
        receipt
    };
    let reopened = open(1, Some(&path));
    block_on(reopened.wait_for_write(&receipt, "global").unwrap()).unwrap();
    let query = reopened.db.prepare_query(&Query::from("notes")).unwrap();
    let rows = reopened.db.read(&query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell("title"),
        Some(Value::String("retained".into()))
    );
}
