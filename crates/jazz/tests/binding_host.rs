//! The shared binding layer, driven the way a JavaScript host drives it:
//! options objects (JSON here), binding-codec cells, postcard queries, and
//! sync frames the host carries between two bindings.

mod common;

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::binding_host::{self, Binding, EventChunk, HostError, Link};
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use serde_json::json;

use common::{allow_all_policies, compile_schema};

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
        author: AuthorSubject::for_test_bytes([node; 16]),
    }
}

fn open(node: u8) -> Binding {
    let identity = identity(node);
    let db = block_on(Db::open(DbConfig::new(schema(), identity))).unwrap();
    Binding::new(db, identity, false)
}

fn authority() -> Binding {
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0xee; 16]),
        author: AuthorSubject::SYSTEM,
    };
    let db = block_on(Db::open_history_complete(DbConfig::new(schema(), identity))).unwrap();
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

fn titles(binding: &Binding) -> Vec<String> {
    let prepared = binding.db.prepare_query(&Query::from("notes")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let mut titles: Vec<String> = block_on(binding.db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| match row.cells().find(|(name, _)| *name == "title") {
            Some((_, Some(Value::String(title)))) => title.clone(),
            other => panic!("unexpected title {other:?}"),
        })
        .collect();
    titles.sort();
    titles
}

fn none() -> serde_json::Value {
    serde_json::Value::Null
}

fn argument(result: Result<impl std::fmt::Debug, HostError>) -> String {
    match result {
        Err(HostError::Argument(message)) => message,
        other => panic!("expected an argument error, got {other:?}"),
    }
}

/// The host carrier: move frames both ways and tick both sides.
fn pump(links: &[(&Link, &Link)]) {
    for _ in 0..8 {
        for (client, server) in links {
            server.receive(client.take_outbound());
            client.receive(server.take_outbound());
            block_on(client.binding().tick()).unwrap();
            block_on(server.binding().tick()).unwrap();
        }
    }
}

fn drive<T>(links: &[(&Link, &Link)], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..50 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        pump(links);
    }
    panic!("did not complete");
}

#[test]
fn options_objects_decode_as_the_typescript_runtime_sends_them() {
    let row = [7u8; 16];
    let insert = binding_host::insert_options(&json!({
        "rowId": row,
        "updatedAtMs": 1_704_067_200_123u64,
        "branch": null,
    }))
    .unwrap();
    assert_eq!(insert.row_id, Some(RowUuid::from_bytes(row)));
    assert_eq!(insert.updated_at_ms, Some(1_704_067_200_123));
    assert!(
        argument(binding_host::insert_options(&json!({
            "author": ([1u8; 16]),
            "attribution": ([2u8; 16]),
        })))
        .contains("both author and attribution")
    );
    assert!(
        argument(binding_host::insert_options(&json!({ "updatedAtMs": 1.5 })))
            .contains("updatedAtMs")
    );
    // A removed field is refused by presence, even as null.
    assert!(
        argument(binding_host::upsert_options(&json!({ "branch": null }))).contains("`branch`")
    );
    assert!(
        argument(binding_host::update_options(&json!({ "base": { "x": 1 } }))).contains("head")
    );

    let read = binding_host::read_opts(&json!({
        "tier": "local-only",
        "include_deleted": true,
        "first_load_remote_wait_ms": 250,
    }))
    .unwrap();
    assert_eq!(read.tier, ReadTier::LocalOnly);
    assert!(read.include_deleted);
    // Only a local-first read waits for its first remote load.
    assert_eq!(read.first_load, jazz::db::FirstLoad::Deliver);
    assert!(
        argument(binding_host::read_opts(
            &json!({ "first_load_remote_wait_ms": -1 })
        ))
        .contains("non-negative")
    );
    assert_eq!(
        binding_host::read_opts(&none()).unwrap(),
        ReadOpts::default()
    );
}

#[test]
fn writes_transactions_and_attribution_go_through_one_binding() {
    let db = open(0xa1);
    let first = db.insert("notes", &title("one"), &json!({})).unwrap();
    db.update("notes", &first.row_id.to_bytes(), &title("uno"), &none())
        .unwrap();
    assert_eq!(titles(&db), ["uno"]);

    // Attributed writes are a backend's alone.
    assert!(
        argument(db.insert("notes", &title("x"), &json!({ "attribution": (AuthorSubject::for_test_bytes([9; 16]).canonical().as_bytes()) })))
            .contains("backend")
    );

    let tx = &OpenTransactionId::new().to_string();
    db.begin_transaction(tx, "serializable", None, None)
        .unwrap();
    db.insert_in_transaction(tx, "notes", &title("two"), &none())
        .unwrap();
    assert_eq!(titles(&db), ["uno"], "staged writes wait for the commit");
    let commit = db.commit_transaction(tx, Some("serializable")).unwrap();
    assert_eq!(titles(&db), ["two", "uno"]);
    assert_eq!(commit.row_id, RowUuid::from_bytes([0; 16]));

    let tx = &OpenTransactionId::new().to_string();
    db.begin_transaction(tx, "mergeable", None, None).unwrap();
    db.delete_in_transaction(tx, "notes", &first.row_id.to_bytes(), &none())
        .unwrap();
    db.rollback_transaction(tx).unwrap();
    assert_eq!(titles(&db), ["two", "uno"]);

    // A staged restore brings a deleted row back at commit.
    db.delete("notes", &first.row_id.to_bytes(), &none())
        .unwrap();
    let tx = &OpenTransactionId::new().to_string();
    db.begin_transaction(tx, "mergeable", None, None).unwrap();
    db.restore_in_transaction(
        tx,
        "notes",
        &first.row_id.to_bytes(),
        &title("back"),
        &none(),
    )
    .unwrap();
    assert_eq!(titles(&db), ["two"]);
    db.commit_transaction(tx, None).unwrap();
    assert_eq!(titles(&db), ["back", "two"]);
    assert!(
        argument(db.begin_transaction(tx, "optimistic", None, None))
            .contains("unknown transaction kind")
    );
}

#[test]
fn two_bindings_sync_over_frames_the_host_carries() {
    let server = authority();
    let client = open(0xa1);
    let upstream = client.connect_upstream();
    let subscriber = server.accept_subscriber(client.author(), Default::default());
    let links = [(&upstream, &subscriber)];

    let write = client.insert("notes", &title("hello"), &none()).unwrap();
    drive(&links, client.wait_for(write.seq, "global").unwrap()).unwrap();
    assert_eq!(titles(&server), ["hello"]);

    // A subscription sees the authority's later write.
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut stream = client.subscribe(&query, &none(), None, None).unwrap();
    let opening = binding_host::event_chunk(drive(&links, client.next_event(&mut stream))).unwrap();
    assert!(matches!(opening, EventChunk::Delta { reset: true, .. }));
    server
        .insert("notes", &title("from the server"), &none())
        .unwrap();
    let delta = binding_host::event_chunk(drive(&links, client.next_event(&mut stream))).unwrap();
    let EventChunk::Delta {
        delta,
        reset: false,
        ..
    } = delta
    else {
        panic!("expected an incremental delta, got {delta:?}");
    };
    let (added, _) = jazz::binding_codec::decode_subscription_delta_rows(&delta).unwrap();
    assert_eq!(
        added[0].rows[0].cells["title"],
        Value::String("from the server".to_owned())
    );

    // Reads, advice and snapshots resolve through the same progress.
    let read = client
        .all(query, &json!({ "tier": "remote" }), None, None, None)
        .unwrap();
    let rows = jazz::binding_codec::decode_rows(&drive(&links, read).unwrap()).unwrap();
    let mut read_titles = rows
        .iter()
        .flat_map(|batch| &batch.rows)
        .map(|row| row.cells["title"].clone())
        .collect::<Vec<_>>();
    read_titles.sort_by_key(|title| format!("{title:?}"));
    assert_eq!(
        read_titles,
        [
            Value::String("from the server".to_owned()),
            Value::String("hello".to_owned())
        ]
    );
    let (_, advice) = client.request_permission_advice(
        binding_host::advice::insert("notes".to_owned(), &title("may I")).unwrap(),
    );
    assert_eq!(drive(&links, advice).unwrap(), "allowed");
    let snapshot = drive(&links, client.capture_snapshot()).unwrap();
    assert!(
        jazz::protocol::SnapshotRef::from_persisted(std::str::from_utf8(&snapshot).unwrap())
            .is_ok()
    );
    drive(&links, client.wait_for_pending_writes_global()).unwrap();

    let mut upstream = upstream;
    assert!(upstream.close());
    assert!(!upstream.close());
}

#[test]
fn a_streamed_value_publishes_as_one_write() {
    let db = open(0xa1);
    let row = [3u8; 16];
    let mut streaming = db
        .begin_streaming_mutation(
            "notes".to_owned(),
            &row,
            &empty_cells(),
            "title".to_owned(),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    let text = "x".repeat(200 * 1024);
    for chunk in text.as_bytes().chunks(64 * 1024) {
        streaming.push(chunk).unwrap();
    }
    let write = streaming.finish().unwrap();
    assert_eq!(write.row_id, RowUuid::from_bytes(row));
    assert_eq!(titles(&db), [text]);
    assert!(argument(streaming.push(b"late")).contains("closed"));
    assert!(!streaming.abort().unwrap());
}

fn empty_cells() -> Vec<u8> {
    let descriptor = RecordDescriptor::new(Vec::<(String, ValueType)>::new());
    jazz::binding_codec::encode_named_cells(&OwnedRecord::new(
        descriptor.create(&[]).unwrap(),
        descriptor,
    ))
    .unwrap()
}
