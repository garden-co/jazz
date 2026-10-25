//! The shared binding layer, driven the way a JavaScript host drives it:
//! options objects (JSON here), binding-codec cells, postcard queries, and
//! sync frames the host carries between two bindings.

use crate::common;

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::binding_host::{self, Binding, EventChunk, HostError, Link};
use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, ReadOpts, ReadTier, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, Query};
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
            .table(
                TableSchemaBuilder::new("comments")
                    .column("note", ColumnType::Uuid)
                    .column("body", ColumnType::Text)
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
    assert!(read.read_view.include_deleted);
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
        Some(&title("back")),
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
fn a_staged_write_refused_for_its_rows_state_rejects_the_commit() {
    let db = open(0xa2);
    let row = RowUuid::from_bytes([0x42; 16]);
    for kind in ["mergeable", "serializable"] {
        let tx = &OpenTransactionId::new().to_string();
        db.begin_transaction(tx, kind, None, None).unwrap();
        // Patching nothing is fine for a stored row but cannot create one:
        // the call stages, and the commit carries the refusal.
        db.upsert_in_transaction(tx, "notes", &row.to_bytes(), &empty_cells(), &none())
            .unwrap();
        db.insert_in_transaction(tx, "notes", &title("kept out"), &none())
            .unwrap();
        let commit = db.commit_transaction(tx, Some(kind)).unwrap();
        let refusal = commit.rejected.expect("the commit is rejected");
        assert!(refusal.is_apply_time_rejection(), "{refusal:?}");
        assert!(refusal.message.contains("missing required field `title`"));
        assert!(titles(&db).is_empty(), "no staged write applies");
        assert!(
            db.commit_transaction(tx, Some(kind)).is_err(),
            "the transaction is closed"
        );
    }

    // A rolled back transaction forgets its refusal.
    let tx = &OpenTransactionId::new().to_string();
    db.begin_transaction(tx, "mergeable", None, None).unwrap();
    db.upsert_in_transaction(tx, "notes", &row.to_bytes(), &empty_cells(), &none())
        .unwrap();
    db.rollback_transaction(tx).unwrap();
    db.begin_transaction(tx, "mergeable", None, None).unwrap();
    db.insert_in_transaction(tx, "notes", &title("after"), &none())
        .unwrap();
    assert!(db.commit_transaction(tx, None).unwrap().rejected.is_none());
    assert_eq!(titles(&db), ["after"]);
}

#[test]
fn restoring_a_live_row_is_rejected_through_its_receipt() {
    let db = open(0xa3);
    let live = db.insert("notes", &title("live"), &none()).unwrap();
    let receipt = db
        .restore(
            "notes",
            &live.row_id.to_bytes(),
            Some(&title("again")),
            &none(),
        )
        .unwrap();
    let refusal = receipt.rejected.expect("the restore is rejected");
    assert!(refusal.is_apply_time_rejection(), "{refusal:?}");
    assert_eq!(
        refusal.message,
        format!("row not deleted: {}", live.row_id.0)
    );
    assert_eq!(titles(&db), ["live"]);
}

#[test]
fn a_row_changed_after_a_serializable_read_rejects_the_commit() {
    let db = open(0xa4);
    let read = db.insert("notes", &title("read"), &none()).unwrap();
    let other = db.insert("notes", &title("other"), &none()).unwrap();
    let tx = &OpenTransactionId::new().to_string();
    db.begin_transaction(tx, "serializable", None, None)
        .unwrap();
    let query = postcard::to_allocvec(&Query::from("notes").filter(jazz::query::eq(
        jazz::query::col("id"),
        jazz::query::lit(Value::Uuid(read.row_id.0)),
    )))
    .unwrap();
    block_on(db.all(query, &none(), Some(tx), None, None).unwrap()).unwrap();
    db.update("notes", &read.row_id.to_bytes(), &title("changed"), &none())
        .unwrap();
    db.update_in_transaction(
        tx,
        "notes",
        &other.row_id.to_bytes(),
        &title("staged"),
        &none(),
    )
    .unwrap();
    let commit = db.commit_transaction(tx, Some("serializable")).unwrap();
    let conflict = commit.rejected.expect("the commit is rejected");
    assert_eq!(
        conflict.code,
        ErrorCode::TransactionConflict,
        "{conflict:?}"
    );
    assert_eq!(
        titles(&db),
        ["changed", "other"],
        "the staged write is not applied"
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
    let (_, advice) = client
        .request_permission_advice(
            binding_host::advice::insert("notes".to_owned(), &title("may I")).unwrap(),
            None,
            None,
        )
        .unwrap();
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
fn a_write_made_before_the_upstream_connects_uploads_once_it_does() {
    let server = authority();
    let client = open(0xa1);
    let write = client.insert("notes", &title("offline"), &none()).unwrap();
    let upstream = client.connect_upstream();
    let subscriber = server.accept_subscriber(client.author(), Default::default());
    let links = [(&upstream, &subscriber)];
    drive(&links, client.wait_for(write.seq, "global").unwrap()).unwrap();
    assert_eq!(titles(&server), ["offline"]);
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

fn comment(note: RowUuid, body: &str) -> Vec<u8> {
    let descriptor =
        RecordDescriptor::new([("note", ValueType::Uuid), ("body", ValueType::String)]);
    let record = OwnedRecord::new(
        descriptor
            .create(&[Value::Uuid(note.0), Value::String(body.to_owned())])
            .unwrap(),
        descriptor,
    );
    jazz::binding_codec::encode_named_cells(&record).unwrap()
}

#[test]
fn a_relation_read_nests_each_relation_in_its_parents_cells() {
    let db = open(0xa1);
    let note = db
        .insert("notes", &title("with comments"), &none())
        .unwrap();
    db.insert("notes", &title("without"), &none()).unwrap();
    for body in ["first", "second"] {
        db.insert("comments", &comment(note.row_id, body), &none())
            .unwrap();
    }

    let query = Query::from("notes")
        .array_subquery(ArraySubquery::new("comments", "comments", "note", "id"));
    let read = db
        .all(
            postcard::to_allocvec(&query).unwrap(),
            &json!({ "tier": "local-only" }),
            None,
            None,
            None,
        )
        .unwrap();
    let (root_count, batches) =
        jazz::binding_codec::decode_relation_snapshot(&block_on(read).unwrap()).unwrap();
    let roots = batches
        .iter()
        .flat_map(|batch| &batch.rows)
        .collect::<Vec<_>>();
    assert_eq!((root_count, roots.len()), (2, 2));

    let mut bodies = Vec::new();
    for root in roots {
        let Value::Array(children) = &root.cells["comments"] else {
            panic!(
                "expected a comments array, got {:?}",
                root.cells["comments"]
            );
        };
        let mut row_bodies = Vec::new();
        for child in children {
            let Value::Record(record) = child else {
                panic!("expected a nested record, got {child:?}");
            };
            let names = record
                .descriptor()
                .fields()
                .iter()
                .map(|field| field.name.clone().unwrap())
                .collect::<Vec<_>>();
            let values = record.to_values().unwrap();
            assert_eq!(names[0], "row_uuid");
            let cell = |name: &str| values[names.iter().position(|n| n == name).unwrap()].clone();
            assert_eq!(cell("note"), Value::Uuid(note.row_id.0));
            row_bodies.push(cell("body"));
        }
        row_bodies.sort_by_key(|body| format!("{body:?}"));
        bodies.push((root.cells["title"].clone(), row_bodies));
    }
    bodies.sort_by_key(|(title, _)| format!("{title:?}"));
    assert_eq!(
        bodies,
        [
            (
                Value::String("with comments".to_owned()),
                vec![
                    Value::String("first".to_owned()),
                    Value::String("second".to_owned())
                ]
            ),
            (Value::String("without".to_owned()), vec![]),
        ]
    );
}

/// A local-first account's proof for app `app`, and the canonical author it
/// proves.
fn local_first_proof(seed: u8, app: &str) -> (String, String) {
    use jazz::identity::{LOCAL_FIRST_ISSUER, derive_user_id, mint_jazz_self_signed_token};
    let seed = [seed; 32];
    let token = mint_jazz_self_signed_token(&seed, LOCAL_FIRST_ISSUER, app, 600).unwrap();
    let user = derive_user_id(&seed).to_string();
    let app = jazz::app_id::AppId::from_name(app);
    let author = AuthorSubject::reserved(LOCAL_FIRST_ISSUER, &user)
        .unwrap()
        .with_account(jazz::account_registry::local_first_account_id(
            *app.uuid(),
            &user,
        ));
    (token, author.canonical().to_owned())
}

#[test]
fn a_backend_writes_as_a_local_first_account_only_once_admitted() {
    let backend = authority();
    let (token, author) = local_first_proof(7, "app");
    let as_account = json!({ "author": author.as_bytes() });

    // Raw author bytes never name a Jazz-reserved identity on their own.
    argument(backend.insert("notes", &title("before"), &as_account));

    backend
        .admit_local_first_session(&token, "app", &author)
        .unwrap();
    let write = backend
        .insert("notes", &title("after"), &as_account)
        .unwrap();
    assert!(write.rejected.is_none());
    assert_eq!(titles(&backend), ["after"]);

    // Only a backend admits, and only the author its proof derives.
    let (other_token, other_author) = local_first_proof(8, "app");
    argument(open(1).admit_local_first_session(&other_token, "app", &other_author));
    argument(backend.admit_local_first_session(&other_token, "app", &author));
    argument(backend.admit_local_first_session(&token, "other-app", &author));
}

#[test]
fn a_first_result_larger_than_one_wire_frame_splits_into_rows_messages() {
    use jazz::db::{Image, SyncCell, SyncMessage, encode_frames};
    let image = |n: u8| Image {
        table: "todos".into(),
        id: [n; 16],
        key: Vec::new(),
        seq: 1,
        deleted: false,
        content: true,
        cells: vec![("title".into(), SyncCell::Text(vec![b'x'; 300 * 1024]))],
    };
    let images: Vec<Image> = (0..20).map(image).collect();
    let frames = encode_frames(SyncMessage::Rows {
        images: images.clone(),
    })
    .unwrap();
    assert!(frames.len() > 1);
    let limit = jazz::protocol_limits::MAX_WIRE_FRAME_BYTES;
    let mut ids = Vec::new();
    for frame in frames {
        assert!(frame.len() < limit);
        match postcard::from_bytes::<SyncMessage>(&frame).unwrap() {
            SyncMessage::Rows { images } => ids.extend(images.into_iter().map(|i| i.id)),
            other => panic!("expected rows, got {other:?}"),
        }
    }
    assert_eq!(ids, images.iter().map(|i| i.id).collect::<Vec<_>>());
    // A message that fits stays one frame.
    let one = encode_frames(SyncMessage::Rows {
        images: vec![image(1)],
    })
    .unwrap();
    assert_eq!(one.len(), 1);
}

/// A browser tab over its worker: the worker is a replica whose own
/// identity the tab connects as.
fn tab_over(worker: &Binding, tab: Binding) -> (Binding, Link, Link) {
    tab.set_non_durable_client();
    tab.set_remote_link_hint("live").unwrap();
    let up = tab.connect_upstream();
    let down = worker.accept_subscriber(worker.author(), Default::default());
    (tab, up, down)
}

/// The opening's rows of a fresh subscription on `binding`.
fn opening_rows(links: &[(&Link, &Link)], binding: &Binding) -> usize {
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut stream = binding.subscribe(&query, &none(), None, None).unwrap();
    let opening = binding_host::event_chunk(drive(links, binding.next_event(&mut stream))).unwrap();
    let EventChunk::Delta {
        delta, reset: true, ..
    } = opening
    else {
        panic!("expected an opening, got {opening:?}");
    };
    let (added, _) = jazz::binding_codec::decode_subscription_delta_rows(&delta).unwrap();
    added.iter().map(|batch| batch.rows.len()).sum()
}

#[test]
fn a_worker_serves_its_own_tabs_as_it_reads_itself() {
    // No permissions: a session would be denied everything, but the tabs
    // are the worker's own foregrounds, not sessions it must police.
    let bare = compile_schema(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("notes").column("title", ColumnType::Text))
            .build(),
    );
    let open = |node: u8| {
        let identity = identity(node);
        let db = block_on(Db::open(DbConfig::new(bare.clone(), identity))).unwrap();
        Binding::new(db, identity, false)
    };
    let worker = open(0xb0);
    let (tab, up, down) = tab_over(&worker, open(0xb1));
    let links = [(&up, &down)];
    tab.insert("notes", &title("local"), &none()).unwrap();
    assert_eq!(opening_rows(&links, &tab), 1);
    assert_eq!(titles(&worker), ["local"]);
}

#[test]
fn a_tab_reads_and_writes_through_its_worker_to_the_server() {
    let server = authority();
    let worker = open(0xc0);
    worker.set_remote_link_hint("live").unwrap();
    let up = worker.connect_upstream();
    let down = server.accept_subscriber(worker.author(), Default::default());
    let (tab, tab_up, tab_down) = tab_over(&worker, open(0xc1));
    let links = [(&up, &down), (&tab_up, &tab_down)];

    // A global wait resolves once the server, not the worker, has the row.
    let write = tab.insert("notes", &title("from tab"), &none()).unwrap();
    drive(&links, tab.wait_for(write.seq, "global").unwrap()).unwrap();
    assert_eq!(titles(&server), ["from tab"]);

    // A Remote read asks the server through the worker, so it sees a row
    // the worker does not hold yet.
    server
        .insert("notes", &title("from server"), &none())
        .unwrap();
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let read = tab
        .all(query, &json!({ "tier": "remote" }), None, None, None)
        .unwrap();
    let rows = jazz::binding_codec::decode_rows(&drive(&links, read).unwrap()).unwrap();
    assert_eq!(rows.iter().map(|batch| batch.rows.len()).sum::<usize>(), 2);
    assert_eq!(opening_rows(&links, &tab), 2);
}

#[test]
fn a_tab_sees_another_workers_tab_write_live() {
    let server = authority();
    let connect = |node: u8| {
        let worker = open(node);
        worker.set_remote_link_hint("live").unwrap();
        let up = worker.connect_upstream();
        let down = server.accept_subscriber(worker.author(), Default::default());
        let (tab, tab_up, tab_down) = tab_over(&worker, open(node + 1));
        (worker, tab, up, down, tab_up, tab_down)
    };
    let (_worker_a, tab_a, a_up, a_down, a_tab_up, a_tab_down) = connect(0xd0);
    let (_worker_b, tab_b, b_up, b_down, b_tab_up, b_tab_down) = connect(0xd2);
    let links = [
        (&a_up, &a_down),
        (&a_tab_up, &a_tab_down),
        (&b_up, &b_down),
        (&b_tab_up, &b_tab_down),
    ];

    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut stream = tab_b.subscribe(&query, &none(), None, None).unwrap();
    let opening = binding_host::event_chunk(drive(&links, tab_b.next_event(&mut stream))).unwrap();
    assert!(matches!(opening, EventChunk::Delta { reset: true, .. }));

    tab_a.insert("notes", &title("from a"), &none()).unwrap();
    let delta = binding_host::event_chunk(drive(&links, tab_b.next_event(&mut stream))).unwrap();
    let EventChunk::Delta { delta, .. } = delta else {
        panic!("expected a delta, got {delta:?}");
    };
    let (added, _) = jazz::binding_codec::decode_subscription_delta_rows(&delta).unwrap();
    assert_eq!(
        added[0].rows[0].cells["title"],
        Value::String("from a".to_owned())
    );
    assert_eq!(titles(&server), ["from a"]);
}

#[test]
fn a_tabs_large_value_reaches_the_server_through_its_worker() {
    let server = authority();
    let worker = open(0xe0);
    worker.set_remote_link_hint("live").unwrap();
    let up = worker.connect_upstream();
    let down = server.accept_subscriber(worker.author(), Default::default());
    let (tab, tab_up, tab_down) = tab_over(&worker, open(0xe1));
    let links = [(&up, &down), (&tab_up, &tab_down)];

    // The worker pushes on the chunks the tab pushed it, ahead of the
    // commit that names them.
    let text = "x".repeat(300 * 1024);
    let write = tab.insert("notes", &title(&text), &none()).unwrap();
    drive(&links, tab.wait_for(write.seq, "global").unwrap()).unwrap();
    assert_eq!(titles(&server), [text]);
}

#[test]
fn a_tab_whose_write_the_server_refuses_reverts_to_the_servers_row() {
    // Notes may be inserted and read, never changed.
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(
                        jazz::tools::TablePolicies::new()
                            .with_select(jazz::tools::PolicyExpr::True)
                            .with_insert(jazz::tools::PolicyExpr::True),
                    ),
            )
            .build(),
    );
    let open = |node: u8| {
        let identity = identity(node);
        let db = block_on(Db::open(DbConfig::new(schema.clone(), identity))).unwrap();
        Binding::new(db, identity, false)
    };
    let server = {
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([0xee; 16]),
            author: AuthorSubject::SYSTEM,
        };
        let db = block_on(Db::open_history_complete(DbConfig::new(
            schema.clone(),
            identity,
        )))
        .unwrap();
        Binding::new(db, identity, true)
    };
    let worker = open(0xf0);
    worker.set_remote_link_hint("live").unwrap();
    let up = worker.connect_upstream();
    let down = server.accept_subscriber(worker.author(), Default::default());
    let (tab, tab_up, tab_down) = tab_over(&worker, open(0xf1));
    let links = [(&up, &down), (&tab_up, &tab_down)];

    let write = tab.insert("notes", &title("kept"), &none()).unwrap();
    drive(&links, tab.wait_for(write.seq, "global").unwrap()).unwrap();
    let row = *write.row_id.0.as_bytes();

    let update = tab
        .update("notes", &row, &title("changed"), &none())
        .unwrap();
    assert!(drive(&links, tab.wait_for(update.seq, "global").unwrap()).is_err());
    pump(&links);
    assert_eq!(titles(&worker), ["kept"]);
    assert_eq!(titles(&tab), ["kept"]);

    let delete = tab.delete("notes", &row, &none()).unwrap();
    assert!(drive(&links, tab.wait_for(delete.seq, "global").unwrap()).is_err());
    pump(&links);
    assert_eq!(titles(&worker), ["kept"]);
    assert_eq!(titles(&tab), ["kept"]);
}

#[test]
fn a_refusal_for_a_closed_tabs_write_names_the_tabs_transaction() {
    // Notes may be inserted and read, never changed.
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(
                        jazz::tools::TablePolicies::new()
                            .with_select(jazz::tools::PolicyExpr::True)
                            .with_insert(jazz::tools::PolicyExpr::True),
                    ),
            )
            .build(),
    );
    let open = |node: u8| {
        let identity = identity(node);
        let db = block_on(Db::open(DbConfig::new(schema.clone(), identity))).unwrap();
        Binding::new(db, identity, false)
    };
    let server = {
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([0xed; 16]),
            author: AuthorSubject::SYSTEM,
        };
        let db = block_on(Db::open_history_complete(DbConfig::new(
            schema.clone(),
            identity,
        )))
        .unwrap();
        Binding::new(db, identity, true)
    };
    let worker = open(0xe0);
    let refused = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let seen = std::rc::Rc::clone(&refused);
    *worker.hooks.rejected.borrow_mut() = Some(Box::new(move |tx_id, _| {
        seen.borrow_mut().push(tx_id.clone());
    }));
    worker.set_remote_link_hint("live").unwrap();
    let mut up = worker.connect_upstream();
    let down = server.accept_subscriber(worker.author(), Default::default());
    let (tab, tab_up, mut tab_down) = tab_over(&worker, open(0xe1));
    let links = [(&up, &down), (&tab_up, &tab_down)];
    let write = tab.insert("notes", &title("kept"), &none()).unwrap();
    drive(&links, tab.wait_for(write.seq, "global").unwrap()).unwrap();
    let row = *write.row_id.0.as_bytes();

    // Changed offline through the worker by a tab that then goes away.
    up.close();
    let update = tab
        .update("notes", &row, &title("changed"), &none())
        .unwrap();
    pump(&[(&tab_up, &tab_down)]);
    assert_eq!(titles(&worker), ["changed"]);
    tab_down.close();

    // The worker uploads it once it is back, and the server refuses it.
    let up = worker.connect_upstream();
    let down = server.accept_subscriber(worker.author(), Default::default());
    pump(&[(&up, &down)]);
    assert_eq!(titles(&worker), ["kept"]);
    assert_eq!(*refused.borrow(), [update.tx_id]);
}

#[test]
fn a_tab_subscribed_while_its_worker_is_offline_hears_the_server_once_it_is_back() {
    let server = authority();
    server.insert("notes", &title("server's"), &none()).unwrap();
    let worker = open(0xac);
    let up = worker.connect_upstream();
    worker.set_remote_link_hint("failed").unwrap();
    let down = server.accept_subscriber(worker.author(), Default::default());
    let (tab, tab_up, tab_down) = tab_over(&worker, open(0xad));
    let tab_links = [(&tab_up, &tab_down)];

    // Served from the worker's store at once: it holds nothing yet.
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut stream = tab.subscribe(&query, &none(), None, None).unwrap();
    let opening =
        binding_host::event_chunk(drive(&tab_links, tab.next_event(&mut stream))).unwrap();
    let EventChunk::Delta { delta, .. } = opening else {
        panic!("expected an opening, got {opening:?}");
    };
    let (added, _) = jazz::binding_codec::decode_subscription_delta_rows(&delta).unwrap();
    assert_eq!(added.iter().map(|batch| batch.rows.len()).sum::<usize>(), 0);

    // The link comes back: the server's row reaches the tab.
    worker.set_remote_link_hint("live").unwrap();
    let links = [(&up, &down), (&tab_up, &tab_down)];
    let change = binding_host::event_chunk(drive(&links, tab.next_event(&mut stream))).unwrap();
    let EventChunk::Delta { delta, .. } = change else {
        panic!("expected a change, got {change:?}");
    };
    let (added, _) = jazz::binding_codec::decode_subscription_delta_rows(&delta).unwrap();
    assert_eq!(added.iter().map(|batch| batch.rows.len()).sum::<usize>(), 1);
}

#[test]
fn tabs_changing_one_row_at_once_settle_on_their_workers_winner() {
    // A worker with no server: it settles its tabs' commits itself.
    let worker = open(0xc0);
    worker.set_remote_link_hint("none").unwrap();
    let (a, a_up, a_down) = tab_over(&worker, open(0xc1));
    let (b, b_up, b_down) = tab_over(&worker, open(0xc2));
    a.set_remote_link_hint("none").unwrap();
    b.set_remote_link_hint("none").unwrap();
    let links = [(&a_up, &a_down), (&b_up, &b_down)];
    let write = a.insert("notes", &title("before"), &none()).unwrap();
    pump(&links);
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut streams = [&a, &b].map(|tab| tab.subscribe(&query, &none(), None, None).unwrap());
    for (tab, stream) in [&a, &b].into_iter().zip(&mut streams) {
        binding_host::event_chunk(drive(&links, tab.next_event(stream))).unwrap();
    }
    let row = *write.row_id.0.as_bytes();

    a.update("notes", &row, &title("from a"), &none()).unwrap();
    b.update("notes", &row, &title("from b"), &none()).unwrap();
    pump(&links);
    let winner = titles(&worker);
    assert_eq!(titles(&a), winner);
    assert_eq!(titles(&b), winner);
}

#[test]
fn a_tabs_local_reads_are_its_workers_store() {
    let server = authority();
    let worker = open(0xa8);
    worker.set_remote_link_hint("live").unwrap();
    let up = worker.connect_upstream();
    let down = server.accept_subscriber(worker.author(), Default::default());
    let (tab, tab_up, tab_down) = tab_over(&worker, open(0xa9));
    let links = [(&up, &down), (&tab_up, &tab_down)];
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let read = |tier: &str| {
        let read = tab
            .all(query.clone(), &json!({ "tier": tier }), None, None, None)
            .unwrap();
        let rows = jazz::binding_codec::decode_rows(&drive(&links, read).unwrap()).unwrap();
        rows.iter().map(|batch| batch.rows.len()).sum::<usize>()
    };

    // The worker's own row: a tab's local-only read is served there.
    worker.insert("notes", &title("worker's"), &none()).unwrap();
    assert_eq!(read("local-only"), 1);

    // A row only the server holds: a local-first read answers from the
    // worker's store at once and refreshes it, so the next one sees it.
    server.insert("notes", &title("server's"), &none()).unwrap();
    assert_eq!(read("local-only"), 1);
    read("local-first");
    pump(&links);
    assert_eq!(read("local-only"), 2);
}

#[test]
fn a_tab_reads_its_workers_store_when_the_worker_has_no_server() {
    let worker = open(0xaa);
    let tab_binding = open(0xab);
    let (tab, up, down) = tab_over(&worker, tab_binding);
    // The tab hears the worker's own path to the server: there is none.
    tab.set_remote_link_hint("none").unwrap();
    let links = [(&up, &down)];
    worker.insert("notes", &title("worker's"), &none()).unwrap();

    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    for tier in ["local-only", "local-first"] {
        let read = tab
            .all(query.clone(), &json!({ "tier": tier }), None, None, None)
            .unwrap();
        let rows = jazz::binding_codec::decode_rows(&drive(&links, read).unwrap()).unwrap();
        assert_eq!(
            rows.iter().map(|batch| batch.rows.len()).sum::<usize>(),
            1,
            "{tier}"
        );

        let mut stream = tab
            .subscribe(&query, &json!({ "tier": tier }), None, None)
            .unwrap();
        let opening =
            binding_host::event_chunk(drive(&links, tab.next_event(&mut stream))).unwrap();
        let EventChunk::Delta { delta, .. } = opening else {
            panic!("expected an opening, got {opening:?}");
        };
        let (added, _) = jazz::binding_codec::decode_subscription_delta_rows(&delta).unwrap();
        assert_eq!(
            added.iter().map(|batch| batch.rows.len()).sum::<usize>(),
            1,
            "{tier}"
        );
    }
}
