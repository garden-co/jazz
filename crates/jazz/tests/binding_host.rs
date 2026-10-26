//! The shared binding layer, driven the way a JavaScript host drives it:
//! options objects (JSON here), binding-codec cells, postcard queries, and
//! sync frames the host carries between two bindings.

use crate::common;

use std::cell::Cell;
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
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

fn public_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
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
        .build()
}

fn schema() -> JazzSchema {
    compile_schema(&public_schema())
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

/// Alice's rejected chunk cannot be published by staging the failed upload
/// into a transaction. Both publication routes stay closed until she aborts.
#[test]
fn a_failed_stream_cannot_be_staged_or_finished() {
    let alice = open(0xa7);
    alice
        .set_large_value_staging_policy(1.0, 60_000.0, None)
        .unwrap();
    let mut upload = alice
        .begin_streaming_mutation(
            "notes".to_owned(),
            &[7; 16],
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
    assert!(
        upload
            .push(&vec![b'x'; jazz::large_values::LEAF_MAX_BYTES + 1])
            .is_err()
    );
    assert!(upload.stage().is_err());
    assert!(upload.finish().is_err());
    assert!(upload.push(b"late").is_err());
    assert!(upload.abort().unwrap());
    assert!(!upload.abort().unwrap());
    assert!(titles(&alice).is_empty());
}

/// Alice stages a large value through the host binding, edits it in the same
/// serializable batch, and publishes both writes together. Invalid attachment
/// consumes its capability; neither rollback nor retry can publish it later.
#[test]
fn staged_upload_and_partial_edit_share_one_binding_transaction() {
    use jazz::db::{LargeValueUpdate, LargeValueUpdatePage, LargeValueUpdateSplice};
    let alice = open(0xa1);
    let upload = |id: u8| {
        let mut streaming = alice
            .begin_streaming_mutation(
                "notes".to_owned(),
                &[id; 16],
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
        streaming.push("x".repeat(200 * 1024).as_bytes()).unwrap();
        let staged = streaming.stage().unwrap();
        assert!(streaming.finish().is_err());
        staged
    };
    let tx = OpenTransactionId::new().to_string();
    alice
        .begin_transaction(&tx, "serializable", None, None)
        .unwrap();
    let mut staged = upload(3);
    staged.attach(&tx).unwrap();
    assert!(!staged.abort().unwrap());
    alice
        .update_large_values_in_transaction(
            &tx,
            "notes",
            &[3; 16],
            &empty_cells(),
            vec![LargeValueUpdate::Splice {
                column: "title".to_owned(),
                within: LargeValueUpdatePage::TextUtf8 { from: 0, to: 1 },
                splices: vec![LargeValueUpdateSplice {
                    at: 0,
                    delete: 1,
                    insert: b"A".to_vec(),
                }],
            }],
            None,
        )
        .unwrap();
    alice
        .insert_in_transaction(&tx, "notes", &title("manifest"), &none())
        .unwrap();
    assert!(titles(&alice).is_empty());
    alice.commit_transaction(&tx, Some("serializable")).unwrap();
    assert_eq!(
        titles(&alice),
        [
            format!("A{}", "x".repeat(200 * 1024 - 1)),
            "manifest".to_owned()
        ]
    );

    let mut invalid = upload(4);
    assert!(invalid.attach("invalid transaction").is_err());
    assert!(invalid.attach(&tx).is_err());
    assert!(!invalid.abort().unwrap());
    let tx = OpenTransactionId::new().to_string();
    alice
        .begin_transaction(&tx, "serializable", None, None)
        .unwrap();
    upload(5).attach(&tx).unwrap();
    alice.rollback_transaction(&tx).unwrap();
    assert_eq!(titles(&alice).len(), 2);
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

/// A receipt belongs to its database node, including shared schema views; a
/// different database with the same numeric sequence cannot reinterpret it.
#[test]
fn write_state_and_wait_preserve_receipt_ownership() {
    use jazz::tx::{DurabilityTier, Fate, RejectionReason};
    let alice = open(1);
    let bob = open(2);
    let write = alice
        .insert("notes", &title("first"), &json!({"rowId": vec![9;16]}))
        .unwrap();
    assert_eq!(alice.write_state(&write).unwrap().fate, Fate::Pending);
    assert!(argument(bob.write_state(&write)).contains("different database node"));
    assert!(matches!(
        bob.wait_for_write(&write, "local"),
        Err(HostError::Argument(_))
    ));
    let view = alice
        .register_schema(&serde_json::to_vec(&public_schema()).unwrap())
        .unwrap();
    assert_eq!(
        view.write_state(&write).unwrap(),
        alice.write_state(&write).unwrap()
    );
    block_on(view.wait_for_write(&write, "local").unwrap()).unwrap();
    let rejected = alice
        .insert("notes", &title("duplicate"), &json!({"rowId": vec![9;16]}))
        .unwrap();
    let state = alice.write_state(&rejected).unwrap();
    assert!(matches!(
        state.fate,
        Fate::Rejected(RejectionReason::MalformedCommit(_))
    ));
    assert_eq!(state.durability, DurabilityTier::None);
    for _ in 0..2 {
        assert!(block_on(alice.wait_for_write(&rejected, "local").unwrap()).is_err());
    }
    assert_eq!(alice.write_state(&rejected).unwrap(), state);
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
    use jazz::db::{FrameJoiner, Image, SyncCell, SyncMessage, encode_frames};
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
    // Every frame but the last carries leading parts; the last is the
    // `Rows` that completes them, and only it yields the whole message.
    let last = frames.len() - 1;
    let mut joiner = FrameJoiner::default();
    for (i, frame) in frames.iter().enumerate() {
        assert!(frame.len() < limit);
        let decoded = jazz::db::decode_message(frame).unwrap();
        match (i == last, decoded) {
            (false, SyncMessage::RowsPart { .. }) | (true, SyncMessage::Rows { .. }) => {}
            (_, other) => panic!("frame {i} of {}: {other:?}", frames.len()),
        }
        match joiner.push(frame).unwrap() {
            None => assert!(i < last),
            Some(SyncMessage::Rows { images: joined }) => {
                assert_eq!(i, last);
                assert_eq!(
                    joined.iter().map(|i| i.id).collect::<Vec<_>>(),
                    images.iter().map(|i| i.id).collect::<Vec<_>>()
                );
            }
            Some(other) => panic!("expected rows, got {other:?}"),
        }
    }
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

#[test]
fn a_frame_is_one_message_and_a_rows_message_ends_its_parts() {
    use jazz::db::{FrameJoiner, SyncMessage, decode_message, encode_frames};
    let [frame] =
        <[Vec<u8>; 1]>::try_from(encode_frames(SyncMessage::Unsubscribe { id: 7 }).unwrap())
            .unwrap();
    assert!(matches!(
        decode_message(&frame),
        Ok(SyncMessage::Unsubscribe { id: 7 })
    ));
    // Postcard alone reads a valid payload prefix and ignores the rest;
    // the semantic codec header and strict decoder fence the whole frame.
    let trailing = [frame.as_slice(), &[0]].concat();
    assert!(
        postcard::from_bytes::<SyncMessage>(
            trailing
                .strip_prefix(jazz::db::SYNC_ENCODING_PREFIX)
                .unwrap()
        )
        .is_ok()
    );
    assert!(decode_message(&trailing).is_err());
    assert!(FrameJoiner::default().push(&trailing).is_err());
    // Parts followed by anything but their `Rows` are the peer's error.
    let [part] = <[Vec<u8>; 1]>::try_from(
        encode_frames(SyncMessage::RowsPart { images: Vec::new() }).unwrap(),
    )
    .unwrap();
    let mut joiner = FrameJoiner::default();
    assert!(joiner.push(&part).unwrap().is_none());
    assert!(joiner.push(&frame).is_err());
}

/// A frame no sync message decodes from.
const MALFORMED: [u8; 2] = [0xc8, 0x01];

#[test]
fn a_server_disconnects_a_client_whose_frame_does_not_decode() {
    let worker = open(0xe0);
    let (tab, up, down) = tab_over(&worker, open(0xe1));
    pump(&[(&up, &down)]);
    assert_eq!(worker.db.open_subscribers(), 1);
    down.receive([MALFORMED.to_vec()]);
    block_on(worker.tick()).unwrap();
    assert_eq!(worker.db.open_subscribers(), 0);
    // Nothing more reaches the tab over the closed session.
    worker.insert("notes", &title("after"), &none()).unwrap();
    block_on(worker.tick()).unwrap();
    assert!(down.take_outbound().is_empty());
    drop(tab);
}

#[test]
fn a_client_detaches_from_an_upstream_whose_frame_does_not_decode() {
    let worker = open(0xe2);
    let (tab, up, down) = tab_over(&worker, open(0xe3));
    pump(&[(&up, &down)]);
    // A frame with bytes after its message is as malformed as any.
    let [frame] = <[Vec<u8>; 1]>::try_from(
        jazz::db::encode_frames(jazz::db::SyncMessage::Unsubscribe { id: 1 }).unwrap(),
    )
    .unwrap();
    up.receive([[frame.as_slice(), &[0]].concat()]);
    block_on(tab.tick()).unwrap();
    // Detached, the tab's writes queue instead of going upstream.
    tab.insert("notes", &title("offline"), &none()).unwrap();
    block_on(tab.tick()).unwrap();
    assert!(up.take_outbound().is_empty());
    assert_eq!(titles(&tab), ["offline"]);
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

/// The host as a browser runs it: a binding ticks only when it asks
/// (receiving frames asks), never on a timer.
fn drive_on_demand<T>(links: &[(&Link, &Link)], future: impl Future<Output = T>) -> T {
    let mut bindings: Vec<(&Binding, Rc<Cell<bool>>)> = Vec::new();
    for link in links.iter().flat_map(|(client, server)| [client, server]) {
        let binding = link.binding();
        if bindings
            .iter()
            .any(|(seen, _)| Rc::ptr_eq(&seen.db, &binding.db))
        {
            continue;
        }
        let asked = Rc::new(Cell::new(false));
        let flag = Rc::clone(&asked);
        *binding.hooks.schedule_tick.borrow_mut() = Some(Box::new(move || flag.set(true)));
        bindings.push((binding, asked));
    }
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..50 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for (client, server) in links {
            // An empty delivery is no delivery.
            let up = client.take_outbound();
            if !up.is_empty() {
                server.receive(up);
            }
            let down = server.take_outbound();
            if !down.is_empty() {
                client.receive(down);
            }
        }
        for (binding, asked) in &bindings {
            if asked.replace(false) {
                block_on(binding.tick()).unwrap();
            }
        }
    }
    panic!("did not complete");
}

#[test]
fn a_tab_read_asks_for_the_tick_that_fetches_its_large_values() {
    // A reloaded tab holds nothing: its read gets the row from the worker
    // and then the value's chunks, which only a tick sends for. Nothing else
    // is in flight to tick it.
    let worker = open(0xd0);
    let large = "q".repeat(300 * 1024);
    worker.insert("notes", &title(&large), &none()).unwrap();
    let (tab, up, down) = tab_over(&worker, open(0xd1));
    let links = [(&up, &down)];
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let read = tab.all(query, &none(), None, None, None).unwrap();
    let rows = jazz::binding_codec::decode_rows(&drive_on_demand(&links, read).unwrap()).unwrap();
    assert_eq!(rows.iter().map(|batch| batch.rows.len()).sum::<usize>(), 1);
    assert_eq!(titles(&tab), [large]);
}

#[test]
fn a_tab_reads_a_large_value_its_worker_relays_from_the_server() {
    // The worker holds the row once the server answers, and fetches its
    // chunks before it serves the tab's read.
    let server = authority();
    let large = "r".repeat(300 * 1024);
    server.insert("notes", &title(&large), &none()).unwrap();
    let worker = open(0xd2);
    worker.set_remote_link_hint("live").unwrap();
    let up = worker.connect_upstream();
    let down = server.accept_subscriber(worker.author(), Default::default());
    let (tab, tab_up, tab_down) = tab_over(&worker, open(0xd3));
    let links = [(&up, &down), (&tab_up, &tab_down)];
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let read = tab
        .all(query, &json!({ "tier": "remote" }), None, None, None)
        .unwrap();
    let rows = jazz::binding_codec::decode_rows(&drive_on_demand(&links, read).unwrap()).unwrap();
    assert_eq!(rows.iter().map(|batch| batch.rows.len()).sum::<usize>(), 1);
    assert_eq!(titles(&tab), [large]);
}

#[test]
fn a_tab_subscription_opens_with_a_large_value_its_worker_relays_from_the_server() {
    // A fresh tab and worker: the opening delta carries the value once the
    // worker has its chunks.
    let server = authority();
    let large = "s".repeat(300 * 1024);
    server.insert("notes", &title(&large), &none()).unwrap();
    let worker = open(0xd4);
    worker.set_remote_link_hint("live").unwrap();
    let up = worker.connect_upstream();
    let down = server.accept_subscriber(worker.author(), Default::default());
    let (tab, tab_up, tab_down) = tab_over(&worker, open(0xd5));
    let links = [(&up, &down), (&tab_up, &tab_down)];
    let query = postcard::to_allocvec(&Query::from("notes").select(["title"])).unwrap();
    let mut stream = tab.subscribe(&query, &none(), None, None).unwrap();
    let mut seen = Vec::new();
    for _ in 0..4 {
        let event = binding_host::event_chunk(drive(&links, tab.next_event(&mut stream))).unwrap();
        if let EventChunk::Delta { delta, .. } = event {
            let (added, _) = jazz::binding_codec::decode_subscription_delta_rows(&delta).unwrap();
            seen.extend(
                added
                    .iter()
                    .flat_map(|batch| &batch.rows)
                    .map(|row| row.cells["title"].clone()),
            );
        }
        if !seen.is_empty() {
            break;
        }
    }
    assert_eq!(seen, [Value::String(large)]);
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
    *worker.hooks.rejected.borrow_mut() = Some(Box::new(move |tx_id, _, _, _| {
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

/// `future` is still waiting after `links` moved every frame they could.
fn still_pending<F: Future + ?Sized>(links: &[(&Link, &Link)], future: std::pin::Pin<&mut F>) {
    let mut future = future;
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..20 {
        assert!(
            future.as_mut().poll(&mut context).is_pending(),
            "resolved without an authority's answer"
        );
        pump(links);
    }
}

fn rows_in(bytes: &[u8]) -> usize {
    let rows = jazz::binding_codec::decode_rows(bytes).unwrap();
    rows.iter().map(|batch| batch.rows.len()).sum()
}

fn delta_rows(chunk: EventChunk) -> usize {
    let EventChunk::Delta { delta, .. } = chunk else {
        panic!("expected a delta, got {chunk:?}");
    };
    let (added, _) = jazz::binding_codec::decode_subscription_delta_rows(&delta).unwrap();
    added.iter().map(|batch| batch.rows.len()).sum()
}

/// Only the authority confirms a remote result or makes a write `Global`
/// (INV-API-32): a worker with no server orders its tabs' writes, but
/// their strict remote reads and Global waits stay pending, while their
/// local-first reads answer from its store.
#[test]
fn a_tabs_remote_reads_and_global_waits_stay_pending_when_its_worker_has_no_server() {
    let worker = open(0xb0);
    worker.set_remote_link_hint("none").unwrap();
    let (tab, up, down) = tab_over(&worker, open(0xb1));
    tab.set_remote_link_hint("none").unwrap();
    let links = [(&up, &down)];
    worker.insert("notes", &title("worker's"), &none()).unwrap();
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let remote = json!({ "tier": "remote" });

    let mut read = tab.all(query.clone(), &remote, None, None, None).unwrap();
    still_pending(&links, read.as_mut());
    let mut stream = tab.subscribe(&query, &remote, None, None).unwrap();
    still_pending(&links, pin!(tab.next_event(&mut stream)));
    let write = tab.insert("notes", &title("tab's"), &none()).unwrap();
    let mut global = tab.wait_for_write(&write, "global").unwrap();
    still_pending(&links, global.as_mut());

    let local = tab
        .all(query, &json!({ "tier": "local-first" }), None, None, None)
        .unwrap();
    assert_eq!(rows_in(&drive(&links, local).unwrap()), 2);
}

/// A strict remote read waits while its path to the server is down, and
/// is answered by the server once it is back: a tab's through its worker,
/// and a client's of its own.
#[test]
fn a_remote_read_waits_for_the_server_while_offline() {
    let server = authority();
    server.insert("notes", &title("server's"), &none()).unwrap();
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let remote = json!({ "tier": "remote" });

    // A tab, its worker's link down.
    let worker = open(0xb2);
    let up = worker.connect_upstream();
    let down = server.accept_subscriber(worker.author(), Default::default());
    worker.set_remote_link_hint("failed").unwrap();
    let (tab, tab_up, tab_down) = tab_over(&worker, open(0xb3));
    let tab_links = [(&tab_up, &tab_down)];
    let mut read = tab.all(query.clone(), &remote, None, None, None).unwrap();
    still_pending(&tab_links, read.as_mut());
    let mut stream = tab.subscribe(&query, &remote, None, None).unwrap();
    {
        let opening = tab.next_event(&mut stream);
        still_pending(&tab_links, pin!(opening));
    }
    worker.set_remote_link_hint("live").unwrap();
    let links = [(&up, &down), (&tab_up, &tab_down)];
    assert_eq!(rows_in(&drive(&links, read).unwrap()), 1);
    let opening = binding_host::event_chunk(drive(&links, tab.next_event(&mut stream))).unwrap();
    assert_eq!(delta_rows(opening), 1);

    // A client of its own, its link down.
    let client = open(0xb4);
    let up = client.connect_upstream();
    let down = server.accept_subscriber(client.author(), Default::default());
    client.set_remote_link_hint("failed").unwrap();
    let mut read = client.all(query, &remote, None, None, None).unwrap();
    still_pending(&[], read.as_mut());
    client.set_remote_link_hint("live").unwrap();
    assert_eq!(rows_in(&drive(&[(&up, &down)], read).unwrap()), 1);
}

/// A tab's `firstLoadRemoteWaitMs` reaches its worker: while the server
/// has not answered, the opening waits that long, then shows the worker's
/// store.
#[test]
fn a_tabs_first_load_wait_reaches_its_worker() {
    let server = authority();
    let worker = open(0xb5);
    worker.set_remote_link_hint("live").unwrap();
    // Connected, but the server never answers: its frames are never carried.
    let _up = worker.connect_upstream();
    let _down = server.accept_subscriber(worker.author(), Default::default());
    worker.insert("notes", &title("worker's"), &none()).unwrap();
    let (tab, tab_up, tab_down) = tab_over(&worker, open(0xb6));
    let links = [(&tab_up, &tab_down)];
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let wait = json!({ "tier": "local-first", "first_load_remote_wait_ms": 300 });

    let mut stream = tab.subscribe(&query, &wait, None, None).unwrap();
    let mut read = tab.all(query, &wait, None, None, None).unwrap();
    {
        let opening = tab.next_event(&mut stream);
        let mut opening = pin!(opening);
        let mut context = Context::from_waker(Waker::noop());
        assert!(read.as_mut().poll(&mut context).is_pending());
        pump(&links);
        assert!(
            opening.as_mut().poll(&mut context).is_pending(),
            "the opening waits for the server first"
        );
        assert!(
            read.as_mut().poll(&mut context).is_pending(),
            "the read waits for the server first"
        );
        std::thread::sleep(std::time::Duration::from_millis(350));
        assert_eq!(
            delta_rows(binding_host::event_chunk(drive(&links, opening)).unwrap()),
            1
        );
    }
    assert_eq!(rows_in(&drive(&links, read).unwrap()), 1);
}

/// Whether a subscription event is ready now, without moving any frames.
fn event_ready(binding: &Binding, stream: &mut jazz::db::SubscriptionStream) -> bool {
    let mut context = Context::from_waker(Waker::noop());
    pin!(binding.next_event(stream))
        .poll(&mut context)
        .is_ready()
}

/// A strict remote subscription lists what the server lists, without
/// pending local versions (INV-API-32): a row written while the server
/// has not accepted it appears only once it has, and a settled row changed
/// meanwhile stays listed as it was settled. A local-first one shows both
/// at once.
#[test]
fn a_remote_subscription_never_lists_pending_local_versions() {
    let server = authority();
    let row = *server
        .insert("notes", &title("settled"), &none())
        .unwrap()
        .row_id
        .0
        .as_bytes();
    let client = open(0xb7);
    let up = client.connect_upstream();
    let down = server.accept_subscriber(client.author(), Default::default());
    let links = [(&up, &down)];
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut remote = client
        .subscribe(&query, &json!({ "tier": "remote" }), None, None)
        .unwrap();
    let opening = binding_host::event_chunk(drive(&links, client.next_event(&mut remote))).unwrap();
    assert_eq!(delta_rows(opening), 1);
    let mut local = client
        .subscribe(&query, &json!({ "tier": "local-first" }), None, None)
        .unwrap();
    drive(&links, client.next_event(&mut local));

    // Written while the server has not seen them: no frames move.
    client.insert("notes", &title("pending"), &none()).unwrap();
    client
        .update("notes", &row, &title("changed"), &none())
        .unwrap();
    block_on(client.db.tick()).unwrap();
    assert!(
        event_ready(&client, &mut local),
        "local-first shows pending writes"
    );
    assert!(
        !event_ready(&client, &mut remote),
        "a remote subscription listed a pending local version"
    );
    let read = client
        .all(
            query.clone(),
            &json!({ "tier": "local-only" }),
            None,
            None,
            None,
        )
        .unwrap();
    assert_eq!(rows_in(&drive(&links, read).unwrap()), 2);

    // The server accepts them: now they are listed.
    let change = binding_host::event_chunk(drive(&links, client.next_event(&mut remote))).unwrap();
    assert!(delta_rows(change) >= 1);
    let read = client
        .all(query, &json!({ "tier": "remote" }), None, None, None)
        .unwrap();
    assert_eq!(rows_in(&drive(&links, read).unwrap()), 2);
    assert_eq!(titles(&server), ["changed", "pending"]);
}

/// Alice's ordinary open cannot smuggle a legacy backend credential past the
/// decoder. New envelopes and legacy absent fields retain ordinary identity.
#[test]
fn open_config_rejects_legacy_credentials_and_unknown_trailing_bytes() {
    let schema = serde_json::to_vec(&public_schema()).unwrap();
    let identity = identity(0xa1);
    let config = binding_host::OpenConfig {
        identity: binding_host::OpenIdentity {
            node: identity.node,
            author: identity.author,
        },
        row_id_seed: None,
        history_complete: false,
        initial_sync_flush_every: None,
    };
    let raw = postcard::to_allocvec(&config).unwrap();
    for tail in [vec![], vec![0]] {
        let mut bytes = raw.clone();
        bytes.extend(tail);
        let (_, mut decoded) = binding_host::decode_open(&schema, &bytes).unwrap();
        assert_eq!(
            binding_host::open_identity(&mut decoded, binding_host::OpenAs::Untrusted).unwrap(),
            (identity, false)
        );
    }
    for credential in ["", "fixture-backend-secret"] {
        let mut bytes = raw.clone();
        bytes.extend(postcard::to_allocvec(&Some(credential)).unwrap());
        assert!(binding_host::decode_open(&schema, &bytes).is_err());
    }
    for tail in [vec![0, 0], vec![1], vec![2]] {
        let mut bytes = raw.clone();
        bytes.extend(tail);
        assert!(binding_host::decode_open(&schema, &bytes).is_err());
    }
}

/// Alice's bounded host carrier preserves upload ordering and resumes after a
/// drain. Large content fills the byte budget before the frame-count budget;
/// all rows and their actual authority receipts must eventually arrive.
#[test]
fn bounded_binding_carriers_resume_without_losing_large_uploads() {
    let client = open(0xb7);
    let server = authority();
    let up = client.connect_upstream();
    let down = server.accept_subscriber(client.author(), Default::default());
    let byte_limit = jazz::protocol_limits::MAX_WIRE_FRAME_BYTES;
    up.set_outbound_limits(128, byte_limit).unwrap();
    down.set_outbound_limits(1, byte_limit).unwrap();
    pump(&[(&up, &down)]);
    let expected: Vec<String> = (0..40)
        .map(|n| format!("{n:02}:{}", "x".repeat(80 * 1024)))
        .collect();
    let writes = expected
        .iter()
        .map(|text| client.insert("notes", &title(text), &none()).unwrap())
        .collect::<Vec<_>>();
    block_on(client.tick()).unwrap();
    let queued = up.outbound_len();
    assert!(
        queued > 1 && queued < 128,
        "byte budget must bind first: {queued}"
    );
    assert!(up.take_outbound_with_budget(128, 0).is_empty());
    assert_eq!(
        up.outbound_len(),
        queued,
        "an undersized carrier budget cannot discard its head"
    );
    for _ in 0..1024 {
        let frames = up.take_outbound();
        assert!(frames.iter().map(Vec::len).sum::<usize>() <= byte_limit);
        down.receive(frames);
        block_on(server.tick()).unwrap();
        assert!(down.outbound_len() <= 1);
        up.receive(down.take_outbound());
        block_on(client.tick()).unwrap();
        if writes.iter().all(|write| {
            client.write_state(write).unwrap().durability == jazz::tx::DurabilityTier::Global
        }) {
            assert_eq!(titles(&server), expected);
            return;
        }
    }
    panic!("bounded carrier did not settle every upload");
}

/// Alice's local retry prefix has a finite budget. Exhausting it closes that
/// carrier; reconnecting resends pending uploads and obtains real receipts
/// without applying any accepted upload twice.
#[test]
fn exhausted_local_output_budget_recovers_on_reconnect() {
    let client = open(0xb8);
    let server = authority();
    let mut up = client.connect_upstream();
    let down = server.accept_subscriber(client.author(), Default::default());
    down.set_outbound_limits(1, jazz::protocol_limits::MAX_WIRE_FRAME_BYTES)
        .unwrap();
    pump(&[(&up, &down)]);
    let expected: Vec<String> = (0..1100).map(|n| format!("row {n:04}")).collect();
    let writes = expected
        .iter()
        .map(|text| client.insert("notes", &title(text), &none()).unwrap())
        .collect::<Vec<_>>();
    block_on(client.tick()).unwrap();
    down.receive(up.take_outbound());
    block_on(server.tick()).unwrap();
    assert_eq!(
        server.db.open_subscribers(),
        0,
        "the retained prefix must not grow without bound"
    );
    assert!(down.outbound_len() <= 1);
    assert_eq!(titles(&server), expected);
    assert!(
        writes
            .iter()
            .all(|write| client.write_state(write).unwrap().durability
                != jazz::tx::DurabilityTier::Global)
    );
    assert!(up.close());
    let up = client.connect_upstream();
    let down = server.accept_subscriber(client.author(), Default::default());
    pump(&[(&up, &down)]);
    assert!(
        writes
            .iter()
            .all(|write| client.write_state(write).unwrap().durability
                == jazz::tx::DurabilityTier::Global)
    );
    assert_eq!(titles(&server), expected);
}

/// Alice seals an initialization on an idle foreground. Its host only ticks
/// when asked; Bob's durable owner must acknowledge the reservation without an
/// unrelated write, timer or second foreground waking either side.
#[test]
fn idle_foreground_initialization_schedules_its_owner_reservation() {
    let owner = open(0xf1);
    owner.db.set_scope_isolated_storage_owner().unwrap();
    let id = DbIdentity {
        node: NodeUuid::from_bytes([0xf2; 16]),
        author: owner.author(),
    };
    let foreground = Binding::new(
        block_on(Db::open(DbConfig::new(schema(), id))).unwrap(),
        id,
        false,
    );
    foreground.set_private_owner_client().unwrap();
    let up = foreground.connect_upstream();
    let down = owner.accept_subscriber(owner.author(), Default::default());
    let links = [(&up, &down)];
    pump(&links);
    let tx = OpenTransactionId::new().to_string();
    foreground
        .begin_transaction(&tx, "serializable", None, None)
        .unwrap();
    foreground
        .record_initialization_insert_absence(&tx, "notes", &[0xf3; 16])
        .unwrap();
    foreground
        .insert_in_transaction(
            &tx,
            "notes",
            &title("unpublished"),
            &json!({"rowId":vec![0xf3;16]}),
        )
        .unwrap();
    // Drain all setup work before the seal begins; only the seal can kick it.
    pump(&links);
    let seal = drive_on_demand(
        &links,
        foreground.seal_initialization_transaction_with_owner(&tx),
    )
    .unwrap();
    assert!(!seal.reserved_tx_id.is_empty());
    assert!(titles(&owner).is_empty(), "reserving is not publication");
    assert!(titles(&foreground).is_empty());
    foreground
        .cancel_initialization_transaction(&seal.token)
        .unwrap();
    assert!(
        foreground
            .publish_initialization_transaction(&seal.token)
            .is_err()
    );
}

/// Alice requests fresh catalogue coverage after her connection to Bob is idle.
/// Receiving cached metadata earlier must not make the new request depend on
/// an unrelated host tick.
#[test]
fn idle_catalogue_coverage_schedules_its_authority_request() {
    let server = authority();
    let alice = open(0xf4);
    let up = alice.connect_upstream();
    let down = server.accept_subscriber(alice.author(), Default::default());
    let links = [(&up, &down)];
    pump(&links);
    let coverage = alice.cover_catalogue("notes".into(), None, None).unwrap();
    assert!(drive_on_demand(&links, coverage).unwrap().is_empty());
    assert!(!alice.table_identity("notes").unwrap().is_empty());
}

/// Alice's idle serializable read asks Bob's authority for accepted rows and
/// their matching origin metadata. The host receives both without a timer or
/// another operation initiating delivery.
#[test]
fn idle_paired_read_schedules_its_authority_request() {
    let server = authority();
    let write = server.insert("notes", &title("accepted"), &none()).unwrap();
    let alice = open(0xf5);
    let up = alice.connect_upstream();
    let down = server.accept_subscriber(alice.author(), Default::default());
    let links = [(&up, &down)];
    pump(&links);
    let tx = OpenTransactionId::new().to_string();
    alice
        .begin_transaction(&tx, "serializable", None, None)
        .unwrap();
    let read = alice
        .all_settlement_metadata(
            postcard::to_allocvec(&Query::from("notes")).unwrap(),
            &json!({"tier":"remote"}),
            &tx,
            None,
            None,
            true,
        )
        .unwrap();
    let bytes = drive_on_demand(&links, read).unwrap();
    let length = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    let metadata: serde_json::Value = serde_json::from_slice(&bytes[4..4 + length]).unwrap();
    assert_eq!(metadata[0]["rowId"], write.row_id.0.to_string());
    assert_eq!(metadata[0]["transactionId"], write.tx_id.to_string());
    let rows = jazz::binding_codec::decode_rows(&bytes[4 + length..]).unwrap();
    assert_eq!(rows.iter().map(|batch| batch.rows.len()).sum::<usize>(), 1);
    assert_eq!(titles(&alice), ["accepted"]);
    alice.rollback_transaction(&tx).unwrap();
}

/// Alice reads through Bob's relay from an empty authority. Accepted coverage
/// is progress even when it contains no rows: it must reach Alice in the same
/// requested drive chain, without a timer or unrelated write ticking Bob.
#[test]
fn a_relay_forwards_empty_remote_settlement_on_demand() {
    let server = authority();
    let worker = open(0xe1);
    worker.set_remote_link_hint("live").unwrap();
    let up = worker.connect_upstream();
    let down = server.accept_subscriber(worker.author(), Default::default());
    let (tab, tab_up, tab_down) = tab_over(&worker, open(0xe2));
    let links = [(&up, &down), (&tab_up, &tab_down)];
    pump(&links);
    let read = tab
        .all(
            postcard::to_allocvec(&Query::from("notes")).unwrap(),
            &json!({"tier":"remote"}),
            None,
            None,
            None,
        )
        .unwrap();
    let bytes = drive_on_demand(&links, read).unwrap();
    let rows = jazz::binding_codec::decode_rows(&bytes).unwrap();
    assert!(rows.iter().all(|batch| batch.rows.is_empty()));
}

/// Bob refuses Alice's upload before Alice calls wait in the same host task.
/// The host's deferred error report must retain that receipt's live claim, so
/// waiting still owns the error and an unrelated refusal remains unclaimed.
#[test]
fn a_deferred_upstream_refusal_retains_its_live_wait_claim() {
    use std::cell::RefCell;
    let schema = schema().without_permissions();
    let server_identity = DbIdentity {
        node: NodeUuid::from_bytes([0xed; 16]),
        author: AuthorSubject::SYSTEM,
    };
    let server = Binding::new(
        block_on(Db::open_history_complete(DbConfig::new(
            schema.clone(),
            server_identity,
        )))
        .unwrap(),
        server_identity,
        true,
    );
    let alice = identity(1);
    let client = Binding::new(
        block_on(Db::open(DbConfig::new(schema, alice))).unwrap(),
        alice,
        false,
    );
    client.set_remote_link_hint("live").unwrap();
    let up = client.connect_upstream();
    let down = server.accept_subscriber(alice.author, Default::default());
    let links = [(&up, &down)];
    let reports = Rc::new(RefCell::new(Vec::new()));
    let received = reports.clone();
    *client.hooks.rejected.borrow_mut() = Some(Box::new(move |tx, _, _, claim| {
        received.borrow_mut().push((tx.clone(), claim));
    }));
    let waited = client.insert("notes", &title("waited"), &none()).unwrap();
    let waited_claim = Rc::new(Cell::new(false));
    client.hooks.track(&waited, &waited_claim);
    let unclaimed = client
        .insert("notes", &title("unclaimed"), &none())
        .unwrap();
    let unclaimed_claim = Rc::new(Cell::new(false));
    client.hooks.track(&unclaimed, &unclaimed_claim);
    pump(&links);
    let reports = reports.borrow();
    assert_eq!(reports.len(), 2);
    let claim = |tx: &_| {
        reports
            .iter()
            .find(|(id, _)| id == tx)
            .unwrap()
            .1
            .as_ref()
            .unwrap()
    };
    assert!(!claim(&waited.tx_id).get());
    assert!(!claim(&unclaimed.tx_id).get());
    // This is the host receipt's public claim cell, set when wait is called.
    waited_claim.set(true);
    assert!(block_on(client.wait_for_write(&waited, "global").unwrap()).is_err());
    assert!(claim(&waited.tx_id).get());
    assert!(!claim(&unclaimed.tx_id).get());
    assert!(titles(&client).is_empty());
}

/// Alice reads an empty result, Bob commits a matching row, and the authority
/// refuses Alice's stale read-only transaction. Every host-facing receipt and
/// unclaimed-error callback must preserve the retryable conflict classification.
#[test]
fn authority_conflicts_remain_conflicts_in_host_receipts_and_events() {
    use jazz::tx::{Fate, RejectionReason};
    use std::cell::RefCell;

    let server = authority();
    let alice = open(0xa6);
    let bob = open(0xb6);
    alice.set_remote_link_hint("live").unwrap();
    bob.set_remote_link_hint("live").unwrap();
    let alice_up = alice.connect_upstream();
    let alice_down = server.accept_subscriber(alice.author(), Default::default());
    let bob_up = bob.connect_upstream();
    let bob_down = server.accept_subscriber(bob.author(), Default::default());
    let alice_links = [(&alice_up, &alice_down)];
    let bob_links = [(&bob_up, &bob_down)];
    pump(&alice_links);
    pump(&bob_links);

    let tx = OpenTransactionId::new().to_string();
    alice
        .begin_transaction(&tx, "serializable", None, None)
        .unwrap();
    let rows = drive(
        &alice_links,
        alice
            .all(
                postcard::to_allocvec(&Query::from("notes")).unwrap(),
                &none(),
                Some(&tx),
                None,
                None,
            )
            .unwrap(),
    )
    .unwrap();
    assert!(jazz::binding_codec::decode_rows(&rows).unwrap().is_empty());
    let concurrent = bob
        .insert("notes", &title("Bob's accepted row"), &none())
        .unwrap();
    drive(
        &bob_links,
        bob.wait_for_write(&concurrent, "global").unwrap(),
    )
    .unwrap();

    let reports = Rc::new(RefCell::new(Vec::new()));
    let received = reports.clone();
    *alice.hooks.rejected.borrow_mut() = Some(Box::new(move |tx, error, serializable, _| {
        received.borrow_mut().push((
            tx.clone(),
            binding_host::mutation_error_parts(error, true),
            serializable,
        ));
    }));
    let stale = alice.commit_transaction(&tx, Some("serializable")).unwrap();
    assert!(
        stale.rejected.is_none(),
        "the authority must decide this race"
    );
    let claim = Rc::new(Cell::new(false));
    alice.hooks.track(&stale, &claim);
    pump(&alice_links);
    assert_eq!(
        alice.write_state(&stale).unwrap().fate,
        Fate::Rejected(RejectionReason::SerializableConflict),
    );
    let reports = reports.borrow();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].0, stale.tx_id);
    assert_eq!(reports[0].1.0, "serialization_conflict");
    assert!(reports[0].2);
    let HostError::Engine(error) =
        block_on(alice.wait_for_write(&stale, "global").unwrap()).unwrap_err()
    else {
        panic!("authority conflict must retain the engine error");
    };
    assert!(error.is_serialization_conflict());
    assert!(error.is_conflict());
    assert_eq!(titles(&server), ["Bob's accepted row"]);
}
