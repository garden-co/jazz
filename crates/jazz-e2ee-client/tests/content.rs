//! One device's content through jazz-core with the runtime's hooks
//! (`E2EE_PROTOCOL.md` §4, §5): every write seals its encrypted cells and
//! tokens under the key the device holds, every read opens them again, an
//! equality filter reaches storage and the wire only as tokens, and a
//! version the device does not hold leaves its rows out and is reported.

mod common;

use std::collections::BTreeMap;
use std::rc::Rc;

use common::*;
use jazz::db::{ErrorCode, block_on};
use jazz::e2ee::{E2eeRefusal, UnreadableCode};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz_e2ee_client::{MemoryStore, Report};

fn cell(row: &jazz::db::CurrentRow, column: &str) -> Value {
    row.cell(column)
        .unwrap_or_else(|| panic!("no `{column}` cell"))
}

struct Unread;
impl std::io::Read for Unread {
    fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
        panic!("the source was read before its metadata was accepted");
    }
}

/// A mistyped plain title alongside an encrypted body leaves the attachment source unread.
#[test]
fn invalid_plain_stream_sibling_leaves_the_attachment_unread() {
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);
    let mut cells = note(space, SECRET);
    cells.insert("title".into(), Value::I32(4));
    let error =
        block_on(db.insert_streaming_value("notes", cells, "attachment", Unread)).unwrap_err();
    assert_eq!(error.code, ErrorCode::WriteRejected);
    assert!(error.message.contains("title"), "{error}");
}

fn refuses_invalid_stream_metadata(kind: jazz::db::StreamingMutationKind) {
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);
    let existing = block_on(db.insert("notes", note(space, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    let (row, mut cells) = if kind == jazz::db::StreamingMutationKind::Update {
        (existing, BTreeMap::new())
    } else {
        (
            jazz::ids::RowUuid(uuid::Uuid::now_v7()),
            note(space, SECRET),
        )
    };
    for streamed in ["attachment", "title"] {
        cells.remove(streamed);
        cells.insert("body".into(), Value::I32(4));
        let write = |cells| {
            block_on(db.write_streaming_value_with_id(
                kind, "notes", row, cells, streamed, Unread, None, None, None, None,
            ))
        };
        let error = write(cells.clone()).unwrap_err();
        assert!(error.message.contains("plaintext type"), "{error}");
        if kind != jazz::db::StreamingMutationKind::Update {
            cells.remove("body");
            let error = write(cells.clone()).unwrap_err();
            assert!(error.message.contains("notes.body"), "{error}");
        }
    }
}

/// Alice's insert rejects a mistyped encrypted caption before reading the attachment.
#[test]
fn invalid_stream_metadata_leaves_an_insert_source_unread() {
    refuses_invalid_stream_metadata(jazz::db::StreamingMutationKind::Insert);
}

/// Alice's update discovers the space from her stored row and rejects a bad caption before reading.
#[test]
fn invalid_stream_metadata_leaves_an_update_source_unread() {
    refuses_invalid_stream_metadata(jazz::db::StreamingMutationKind::Update);
}

/// Alice's upsert of a new row rejects a bad caption before reading the attachment.
#[test]
fn invalid_stream_metadata_leaves_an_upsert_source_unread() {
    refuses_invalid_stream_metadata(jazz::db::StreamingMutationKind::Upsert);
}

/// Alice can stream a plain patch without a row at begin. A patch that
/// supplies encrypted metadata at begin or finish leaves the secret alone.
#[test]
fn a_rowless_stream_only_publishes_plain_patches_of_an_encrypted_table() {
    use jazz::db::StreamingMutationKind::Update;

    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);
    let row = block_on(db.insert("notes", note(space, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    let metadata = BTreeMap::from([("body".to_owned(), text("caption"))]);
    let error = db
        .begin_streaming_value_upload("notes", &metadata, "title")
        .err()
        .unwrap();
    assert_eq!(error.code, ErrorCode::Query);
    assert!(error.message.contains("notes.body"));

    let publish = |cells| {
        let mut upload = db
            .begin_streaming_value_upload("notes", &BTreeMap::new(), "title")
            .unwrap();
        block_on(db.push_streaming_value_upload(&mut upload, b"plain title")).unwrap();
        block_on(db.finish_streaming_value_upload(
            upload,
            Update,
            "notes",
            row,
            cells,
            "title",
            Default::default(),
            None,
            None,
            None,
        ))
    };
    publish(BTreeMap::new()).unwrap();
    assert_eq!(publish(metadata).unwrap_err().code, ErrorCode::Query);
    pump(&server, &db);
    let rows = all(&server, Query::from("notes")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);
    assert_eq!(cell(&rows[0], "title"), text("plain title"));
    assert_eq!(
        cell(&all(&db, Query::from("notes")).unwrap()[0], "body"),
        text(SECRET)
    );
}

/// Alice has the vault key and supplies its plain label. An insert or upsert
/// would also create an encrypted null secret, so a rowless stream must refuse
/// publication when it learns the mutation kind at finish.
#[test]
fn a_rowless_stream_cannot_create_an_encrypted_default() {
    use jazz::db::StreamingMutationKind::{Insert, Upsert};

    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let vault = uuid::Uuid::now_v7();
    let held = new_space_key(&client.device().unwrap(), &address_in(&db, "vaults", vault));
    block_on(client.keep(held)).unwrap();
    for kind in [Insert, Upsert] {
        let cells = BTreeMap::new();
        let mut upload = db
            .begin_streaming_value_upload("vaults", &cells, "label")
            .unwrap();
        block_on(db.push_streaming_value_upload(&mut upload, b"plain label")).unwrap();
        let error = block_on(db.finish_streaming_value_upload(
            upload,
            kind,
            "vaults",
            jazz::ids::RowUuid(vault),
            cells,
            "label",
            Default::default(),
            None,
            None,
            None,
        ))
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::Query);
        assert!(error.message.contains("encrypted table `vaults`"));
    }
    pump(&server, &db);
    assert!(all(&db, Query::from("vaults")).unwrap().is_empty());
    assert!(all(&server, Query::from("vaults")).unwrap().is_empty());
}

/// Contract: alice's encrypted cells of every type (required text with an
/// equality index, optional text, optional bytes, optional integer, null
/// included) are stored and uploaded sealed, with a token for the indexed
/// one; she reads back exactly what she wrote, typed as an unencrypted
/// optional column reads, and her equality query finds the row by token.
///
/// ```text
/// alice ──seal──► her store ──upload──► server (ciphertext and tokens)
///   ▲
///   └──open── her reads and her token query
/// ```
#[test]
fn alice_reads_back_what_she_sealed_and_nothing_leaks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, wire) = online(&client, &server, Some(&path));
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);

    let mut cells = note(space, SECRET);
    cells.insert("note".to_owned(), text("NOTE-SECRET"));
    cells.insert(
        "attachment".to_owned(),
        Value::Bytes(b"BYTES-SECRET".to_vec()),
    );
    cells.insert("rank".to_owned(), Value::I32(424_242));
    cells.insert("plainNote".to_owned(), text("visible"));
    let full = block_on(db.insert("notes", cells, Default::default()))
        .unwrap()
        .row_uuid();
    let mut sparse = note(space, "other");
    sparse.insert("note".to_owned(), Value::Nullable(None));
    let sparse = block_on(db.insert("notes", sparse, Default::default()))
        .unwrap()
        .row_uuid();
    pump(&server, &db);

    let rows = all(&db, Query::from("notes")).unwrap();
    let row = |id| rows.iter().find(|row| row.row_uuid() == id).unwrap();
    assert_eq!(cell(row(full), "body"), text(SECRET));
    // An encrypted optional column reads like a plain one.
    assert_eq!(
        cell(row(full), "plainNote"),
        Value::Nullable(Some(Box::new(text("visible"))))
    );
    assert_eq!(
        cell(row(full), "note"),
        Value::Nullable(Some(Box::new(text("NOTE-SECRET"))))
    );
    assert_eq!(
        cell(row(full), "attachment"),
        Value::Nullable(Some(Box::new(Value::Bytes(b"BYTES-SECRET".to_vec()))))
    );
    assert_eq!(
        cell(row(full), "rank"),
        Value::Nullable(Some(Box::new(Value::I32(424_242))))
    );
    assert_eq!(cell(row(sparse), "note"), cell(row(sparse), "plainNote"));
    assert_eq!(cell(row(sparse), "note"), Value::Nullable(None));
    assert!(
        row(full).cell("__e2ee_eq_body").is_none(),
        "tokens never reach a reader"
    );

    let searched = |body: &str| {
        Query::from("notes")
            .filter(eq(col("projectId"), lit(Value::Uuid(space))))
            .filter(eq(col("body"), lit(text(body))))
    };
    let found = all(&db, searched(SECRET)).unwrap();
    assert_eq!(
        found.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        [full]
    );
    assert!(all(&db, searched("absent")).unwrap().is_empty());
    let prepared = db.prepare_query(&searched(SECRET)).unwrap();
    assert!(!format!("{:?}", prepared.query()).contains(SECRET));
    let _live = block_on(db.subscribe(&prepared, Default::default())).unwrap();
    pump(&server, &db);

    // The server stores sealed cells and the token alice uploaded.
    for row in all(&server, Query::from("notes")).unwrap() {
        let Value::Bytes(body) = cell(&row, "body") else {
            panic!("the server stores a sealed cell");
        };
        assert!(body.starts_with(b"JE2E") && !contains(&body, SECRET.as_bytes()));
        assert!(matches!(
            cell(&row, "__e2ee_eq_body"),
            Value::Nullable(Some(_))
        ));
    }
    let wire = wire.borrow();
    assert!(contains(&wire, b"__e2ee_eq_body"), "the query went up");
    for secret in [SECRET, "NOTE-SECRET", "BYTES-SECRET"] {
        assert!(!contains(&wire, secret.as_bytes()), "{secret} on the wire");
    }
    drop(db);
    let stored = stored_bytes(&path);
    for secret in [SECRET, "NOTE-SECRET", "BYTES-SECRET"] {
        assert!(
            !contains(&stored, secret.as_bytes()),
            "{secret} in alice's store"
        );
    }
}

/// Contract: without a key for the space, alice's write is refused on the
/// call with `key-pending` naming the space and applies nothing; her read
/// of rows sealed under a version she does not hold leaves them out and
/// reports the missing version for the runtime to acquire; a strict read
/// fails instead.
#[test]
fn a_version_alice_does_not_hold_leaves_her_rows_out() {
    let server = authority();
    let writer = open_client(Rc::new(MemoryStore::new()));
    let (writer_db, _w, _) = online(&writer, &server, None);
    let space = project(&writer_db, "plans");
    hold_new_space(&writer, &writer_db, space);
    block_on(writer_db.insert("notes", note(space, SECRET), Default::default())).unwrap();
    pump(&server, &writer_db);

    let alice = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&alice, &server, None);
    let heard = Rc::new(std::cell::RefCell::new(0));
    let sink = heard.clone();
    alice.on_report(move |report| {
        if matches!(report, Report::MissingKey { .. }) {
            *sink.borrow_mut() += 1;
        }
    });
    let query = Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))));
    let _live =
        block_on(db.subscribe(&db.prepare_query(&query).unwrap(), Default::default())).unwrap();
    pump(&server, &db);

    assert!(all(&db, query.clone()).unwrap().is_empty());
    assert!(all(&db, query.clone()).unwrap().is_empty());
    alice.flush_reports();
    assert_eq!(
        *heard.borrow(),
        1,
        "a missing version reaches the listener once"
    );
    let reports = alice.take_reports();
    assert!(
        reports.iter().any(|report| matches!(
            report,
            Report::MissingKey { table, space: missing, .. }
                if table == "notes" && missing.identifier == space.to_string()
        )),
        "{reports:?}"
    );

    let error = block_on(db.insert("notes", note(space, "mine"), Default::default())).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    let Some(E2eeRefusal::NeedsKey { space: needed }) = E2eeRefusal::of(&error) else {
        panic!("{error:?}");
    };
    assert_eq!(needed.identifier, space.to_string());
    assert_eq!(all(&db, query.clone()).unwrap().len(), 0);

    let strict = block_on(db.all_strict(&db.prepare_query(&query).unwrap(), local()));
    assert!(strict.is_err());
}

/// Contract: a cell some writer stored without sealing it, or sealed for
/// another row, is never returned: the row is left out and reported
/// `invalid-ciphertext`, and the other rows still read.
///
/// ```text
/// alice ──sealed row──────────► server ──► alice reads it
/// mallory ──raw bytes / moved cell──► server ──► alice: left out, reported
/// ```
#[test]
fn cells_that_do_not_authenticate_are_reported_not_returned() {
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);
    let good = block_on(db.insert("notes", note(space, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    pump(&server, &db);
    let sealed = cell(&all(&server, Query::from("notes")).unwrap()[0], "body");

    // Mallory writes straight to the authority: raw bytes, and alice's
    // sealed cell copied into another row.
    let raw = unsealed(space, Value::Bytes(b"not a sealed cell at all".to_vec()));
    let bad = block_on(server.insert("notes", raw, Default::default()))
        .unwrap()
        .row_uuid();
    let copied = block_on(server.insert("notes", unsealed(space, sealed), Default::default()))
        .unwrap()
        .row_uuid();
    let query = Query::from("notes");
    let _live =
        block_on(db.subscribe(&db.prepare_query(&query).unwrap(), Default::default())).unwrap();
    pump(&server, &db);
    client.take_reports();

    let rows = all(&db, query).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        [good]
    );
    let invalid: Vec<_> = client
        .take_reports()
        .into_iter()
        .filter_map(|report| match report {
            Report::Unreadable(unreadable) => Some(unreadable),
            _ => None,
        })
        .collect();
    assert!(
        invalid
            .iter()
            .all(|unreadable| unreadable.code == UnreadableCode::InvalidCiphertext),
        "{invalid:?}"
    );
    let mut body_rows: Vec<_> = invalid
        .iter()
        .filter(|unreadable| unreadable.column.as_deref() == Some("body"))
        .map(|unreadable| unreadable.id.unwrap())
        .collect();
    body_rows.sort();
    let mut expected = vec![bad, copied];
    expected.sort();
    assert_eq!(body_rows, expected);
}

/// Unreadable reports alice's runtime left since its last call, as
/// `(row, column, code, space)`, sorted and without repeats.
fn unreadable(
    client: &jazz_e2ee_client::E2eeClient,
) -> Vec<(jazz::ids::RowUuid, Option<String>, UnreadableCode, String)> {
    let mut out: Vec<_> = client
        .take_reports()
        .into_iter()
        .filter_map(|report| match report {
            Report::Unreadable(unreadable) => Some((
                unreadable.id.unwrap(),
                unreadable.column,
                unreadable.code,
                unreadable.space.unwrap().identifier,
            )),
            _ => None,
        })
        .collect();
    out.sort_by_key(|(id, column, ..)| (*id, column.clone()));
    out.dedup();
    out
}

/// Contract: a sealed cell's context binds its column (protocol §4.1).
/// Mallory, writing straight to the authority, copies a row's sealed
/// `body` into the same row's `note`: the copy does not open, so the row
/// is left out and its `note` reported `invalid-ciphertext`, while alice's
/// untouched row still reads.
///
/// ```text
/// alice ──seal──► server ◄── mallory: row.note = row.body
///   ▲               │
///   └── reads ◄─────┘   untouched row only; the copy is reported
/// ```
#[test]
fn a_cell_copied_into_another_column_does_not_open() {
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let plans = project(&db, "plans");
    hold_new_space(&client, &db, plans);
    let insert = |body: &str| {
        block_on(db.insert("notes", note(plans, body), Default::default()))
            .unwrap()
            .row_uuid()
    };
    let (untouched, copied) = (insert("kept"), insert(SECRET));
    pump(&server, &db);
    let stored = all(&server, Query::from("notes")).unwrap();
    let body = cell(
        stored.iter().find(|row| row.row_uuid() == copied).unwrap(),
        "body",
    );
    let cells = BTreeMap::from([("note".to_owned(), body)]);
    block_on(server.update("notes", copied, cells, Default::default())).unwrap();
    let query = Query::from("notes");
    let _live =
        block_on(db.subscribe(&db.prepare_query(&query).unwrap(), Default::default())).unwrap();
    pump(&server, &db);
    client.take_reports();

    let rows = all(&db, query).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        [untouched]
    );
    assert_eq!(
        unreadable(&client),
        [(
            copied,
            Some("note".to_owned()),
            UnreadableCode::InvalidCiphertext,
            plans.to_string()
        )]
    );
}

/// Contract: a sealed cell's context binds its space (protocol §4.1).
/// Mallory moves alice's row to another space she also holds without
/// resealing it: no cell opens under the row's new space, each opens only
/// under the space it was sealed for, so the row is returned and every
/// encrypted cell is reported `stale-space` naming that space (§4.3: such
/// a row needs repair and is never reported `invalid-ciphertext`).
///
/// ```text
/// alice ──seal (plans)──► server ◄── mallory: projectId = other
///   ▲                       │
///   └── reads, opened under plans ◄── reported stale-space (plans)
/// ```
#[test]
fn a_row_moved_to_another_space_opens_only_under_the_space_it_was_sealed_for() {
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let (plans, other) = (project(&db, "plans"), project(&db, "other"));
    hold_new_space(&client, &db, plans);
    hold_new_space(&client, &db, other);
    let moved = block_on(db.insert("notes", note(plans, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    pump(&server, &db);
    let cells = BTreeMap::from([("projectId".to_owned(), Value::Uuid(other))]);
    block_on(server.update("notes", moved, cells, Default::default())).unwrap();
    let query = Query::from("notes");
    let _live =
        block_on(db.subscribe(&db.prepare_query(&query).unwrap(), Default::default())).unwrap();
    pump(&server, &db);
    client.take_reports();

    let rows = all(&db, query).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        [moved]
    );
    assert_eq!(cell(&rows[0], "body"), text(SECRET));
    let reports = unreadable(&client);
    let columns: Vec<_> = reports
        .iter()
        .map(|(_, column, ..)| column.clone())
        .collect();
    assert!(columns.contains(&Some("body".to_owned())), "{reports:?}");
    assert!(
        reports.iter().all(|(id, _, code, space)| *id == moved
            && *code == UnreadableCode::StaleSpace
            && *space == plans.to_string()),
        "{reports:?}"
    );
}

/// A note as a writer without the hooks stores it: every encrypted column
/// holds `value` as it is.
fn unsealed(space: uuid::Uuid, value: Value) -> BTreeMap<String, Value> {
    let mut cells = note(space, "ignored");
    for column in ["body", "note", "attachment", "rank"] {
        cells.insert(column.to_owned(), value.clone());
    }
    cells
}

/// Contract: queries the encrypted columns cannot answer without a scan
/// are refused, naming what is not supported: ordering by an encrypted
/// column, a filter other than `eq` on one, `eq` on a column without an
/// equality index, `eq` without the space, an aggregate, and encrypted
/// columns selected without the space column; and a value of another type
/// never matches by accident.
#[test]
fn unanswerable_encrypted_queries_are_refused() {
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);
    let in_space = || Query::from("notes").filter(eq(col("projectId"), lit(Value::Uuid(space))));
    for refused in [
        in_space().order_by("body", OrderDirection::Asc),
        in_space().filter(jazz::query::Predicate::Ne(
            jazz::query::Operand::Column("body".to_owned()),
            jazz::query::Operand::Literal(text("x")),
        )),
        in_space().filter(eq(col("note"), lit(text("x")))),
        Query::from("notes").filter(eq(col("body"), lit(text("x")))),
        in_space().filter(eq(col("body"), lit(Value::I32(1)))),
        in_space().count(),
        Query::from("notes").select(["title", "body"]),
    ] {
        let error = db.prepare_query(&refused).unwrap_err();
        assert_eq!(error.code, ErrorCode::Query, "{error:?}");
    }
    assert!(
        db.prepare_query(&in_space().order_by("title", OrderDirection::Asc))
            .is_ok()
    );
    assert!(
        db.prepare_query(&Query::from("notes").select(["projectId", "body"]))
            .is_ok()
    );
}

/// Contract: offline, an equality query computes no tokens: it reads the
/// space's rows and compares the opened values, so it finds the same rows
/// as online.
#[test]
fn offline_equality_queries_compare_opened_values() {
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);
    for body in ["x", "y", "x"] {
        block_on(db.insert("notes", note(space, body), Default::default())).unwrap();
    }
    let searched = Query::from("notes")
        .filter(eq(col("projectId"), lit(Value::Uuid(space))))
        .filter(eq(col("body"), lit(text("x"))));
    let online_rows = all(&db, searched.clone()).unwrap().len();
    client.set_online(false);
    let offline = db.prepare_query(&searched).unwrap();
    assert!(!format!("{:?}", offline.query()).contains("__e2ee_eq_body"));
    assert_eq!(
        block_on(db.all(&offline, local())).unwrap().len(),
        online_rows
    );
    assert_eq!(online_rows, 2);
}

/// Contract: a large value alice streams into an encrypted bytes column is
/// sealed as it streams, under the space's held key, and reads back whole;
/// without the key the stream is refused before any source byte is read.
#[test]
fn streamed_values_are_sealed_under_the_held_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, wire) = online(&client, &server, Some(&path));
    let space = project(&db, "plans");
    let value = SECRET.repeat(20_000).into_bytes();
    let row = jazz::ids::RowUuid(uuid::Uuid::now_v7());
    let mut source = &value[..];
    let refused = block_on(db.insert_streaming_value_with_id(
        "notes",
        row,
        note(space, "streamed"),
        "attachment",
        &mut source,
    ));
    assert!(matches!(
        refused.map_err(|error| E2eeRefusal::of(&error)),
        Err(Some(E2eeRefusal::NeedsKey { .. }))
    ));
    assert_eq!(
        source.len(),
        value.len(),
        "no source byte is read before the key"
    );

    hold_new_space(&client, &db, space);
    block_on(db.insert_streaming_value_with_id(
        "notes",
        row,
        note(space, "streamed"),
        "attachment",
        &value[..],
    ))
    .unwrap();
    pump(&server, &db);
    let rows = all(&db, Query::from("notes")).unwrap();
    assert_eq!(
        cell(&rows[0], "attachment"),
        Value::Nullable(Some(Box::new(Value::Bytes(value))))
    );
    assert_eq!(cell(&rows[0], "body"), text("streamed"));
    assert!(
        !contains(&wire.borrow(), SECRET.as_bytes()),
        "plaintext on the wire"
    );
    drop(db);
    assert!(
        !contains(&stored_bytes(&path), SECRET.as_bytes()),
        "plaintext in the store"
    );
}

/// Contract: an ordinary write cannot move an encrypted row to another
/// space; a move re-encrypts every encrypted column under the target
/// space's key in one serializable transaction, and must supply them all.
#[test]
fn only_a_move_reseals_a_row_under_another_space() {
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let (from, to) = (project(&db, "from"), project(&db, "to"));
    hold_new_space(&client, &db, from);
    hold_new_space(&client, &db, to);
    let row = block_on(db.insert("notes", note(from, SECRET), Default::default()))
        .unwrap()
        .row_uuid();
    let moved = block_on(db.update("notes", row, note(to, "moved"), Default::default()));
    assert_eq!(moved.unwrap_err().code, ErrorCode::WriteRejected);

    let move_with = |cells: BTreeMap<String, Value>| {
        let id = jazz::tools::OpenTransactionId::new();
        block_on(db.begin_serializable(id)).unwrap();
        let staged = block_on(db.tx_ref(id).move_to_space(
            "notes",
            row,
            cells,
            Value::Uuid(to),
            Default::default(),
        ));
        match staged {
            Ok(_) => block_on(db.commit_serializable_handle(id)).map(|_| ()),
            Err(error) => {
                db.abandon_transaction_handle(id).unwrap();
                Err(error)
            }
        }
    };
    let partial = move_with(BTreeMap::from([("body".to_owned(), text("moved"))]));
    assert_eq!(partial.unwrap_err().code, ErrorCode::WriteRejected);
    move_with(BTreeMap::from([
        ("body".to_owned(), text("moved")),
        ("note".to_owned(), Value::Nullable(None)),
        ("attachment".to_owned(), Value::Nullable(None)),
        ("rank".to_owned(), Value::Nullable(None)),
    ]))
    .unwrap();
    let rows = all(&db, Query::from("notes")).unwrap();
    assert_eq!(cell(&rows[0], "projectId"), Value::Uuid(to));
    assert_eq!(cell(&rows[0], "body"), text("moved"));
}

/// Contract: a read that selects no encrypted column opens nothing: every
/// row comes back, those of spaces alice holds no key for included, with
/// no report.
#[test]
fn a_plaintext_only_select_returns_every_row() {
    let server = authority();
    let writer = open_client(Rc::new(MemoryStore::new()));
    let (writer_db, _w, _) = online(&writer, &server, None);
    let space = project(&writer_db, "plans");
    hold_new_space(&writer, &writer_db, space);
    for body in ["one", "two"] {
        block_on(writer_db.insert("notes", note(space, body), Default::default())).unwrap();
    }
    pump(&server, &writer_db);

    let alice = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&alice, &server, None);
    let titles = Query::from("notes").select(["title"]);
    let _live =
        block_on(db.subscribe(&db.prepare_query(&titles).unwrap(), Default::default())).unwrap();
    pump(&server, &db);
    alice.take_reports();
    let rows = all(&db, titles).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| cell(row, "title") == text("title")));
    assert_eq!(alice.take_reports(), Vec::new());
}

/// Alice's store with a row written before a migration added `notes`'
/// encrypted columns, reopened on the migrated schema with the runtime's
/// hooks (opened with `declarations`) and a key for the row's project.
struct Migrated {
    _dir: tempfile::TempDir,
    _server: jazz::db::Db,
    client: jazz_e2ee_client::E2eeClient,
    db: jazz::db::Db,
    _link: jazz::db::Upstream,
    space: uuid::Uuid,
}

fn migrated_with_an_old_row(declarations: jazz::e2ee::E2eeDeclarations) -> Migrated {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let server = block_on(jazz::db::Db::open(jazz::db::DbConfig::new(
        schema_before_contents(),
        identity(0),
    )))
    .unwrap();
    server.set_authority().unwrap();
    let space = {
        let before = block_on(jazz::db::Db::open_at(
            jazz::db::DbConfig::new(schema_before_contents(), identity(1)),
            &path,
            jazz::db::FileOptions::default(),
        ))
        .unwrap();
        let (_link, _) = link(&before, &server);
        let space = project(&before, "plans");
        block_on(before.insert(
            "notes",
            BTreeMap::from([
                ("projectId".to_owned(), Value::Uuid(space)),
                ("title".to_owned(), text("old")),
            ]),
            Default::default(),
        ))
        .unwrap();
        pump(&server, &before);
        space
    };
    block_on(server.admit_schema_with_lenses(schema(), &[add_contents()])).unwrap();
    let client = open_client_with(Rc::new(MemoryStore::new()), declarations);
    let db = block_on(jazz::db::Db::open_at(
        migrated(jazz::db::DbConfig::new(schema(), identity(1))).with_e2ee(client.hooks()),
        &path,
        jazz::db::FileOptions::default(),
    ))
    .unwrap();
    let (_link, _) = link(&db, &server);
    pump(&server, &db);
    assert!(client.prepare(&db).unwrap());
    hold_new_space(&client, &db, space);
    Migrated {
        _dir: dir,
        _server: server,
        client,
        db,
        _link,
        space,
    }
}

/// Contract: rows stored before a migration added optional encrypted
/// columns read them as null, with no report; a row written since reads
/// its values.
///
/// ```text
/// alice (old schema) ──insert──► her store     (migration adds note, rank)
/// alice (new schema) ──read──► old row: note = null, rank = null, no report
/// ```
#[test]
fn optional_columns_a_migration_added_read_as_null() {
    let Migrated {
        client, db, space, ..
    } = migrated_with_an_old_row(declarations());
    let mut cells = note(space, "new");
    cells.insert("rank".to_owned(), Value::I32(3));
    block_on(db.insert("notes", cells, Default::default())).unwrap();
    client.take_reports();

    let read = Query::from("notes").select(["projectId", "title", "note", "rank"]);
    let mut rows = all(&db, read).unwrap();
    rows.sort_by_key(|row| format!("{:?}", cell(row, "title")));
    assert_eq!(rows.len(), 2);
    assert_eq!(cell(&rows[0], "title"), text("old"));
    assert_eq!(cell(&rows[0], "note"), Value::Nullable(None));
    assert_eq!(cell(&rows[0], "rank"), Value::Nullable(None));
    assert_eq!(
        cell(&rows[1], "rank"),
        Value::Nullable(Some(Box::new(Value::I32(3))))
    );
    assert_eq!(client.take_reports(), Vec::new());
}

fn with_body_default() -> jazz::e2ee::E2eeDeclarations {
    let mut declared = declarations();
    declared
        .tables
        .get_mut("notes")
        .unwrap()
        .columns
        .get_mut("body")
        .unwrap()
        .plain
        .default = Some(jazz::tools::Value::Text("untitled".to_owned()));
    declared
}

/// Contract: rows stored before a migration added a required encrypted
/// column read its declared default, with no report, and are never left
/// out.
#[test]
fn rows_older_than_a_required_column_read_its_default() {
    let Migrated { client, db, .. } = migrated_with_an_old_row(with_body_default());
    client.take_reports();
    let rows = all(&db, Query::from("notes")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(cell(&rows[0], "title"), text("old"));
    assert_eq!(cell(&rows[0], "body"), text("untitled"));
    assert_eq!(client.take_reports(), Vec::new());
}

/// Contract: an empty value a writer stored in a required encrypted column
/// reads as the column's declared default, and the row is returned and
/// reported `empty-cell`, never treated as malformed.
///
/// ```text
/// mallory ──body = empty bytes──► server ──► alice: "untitled", reported empty-cell
/// ```
#[test]
fn a_stored_empty_value_reads_the_default_and_is_reported() {
    let server = authority();
    let client = open_client_with(Rc::new(MemoryStore::new()), with_body_default());
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);
    let stored_empty = unsealed(space, Value::Bytes(Vec::new()));
    let row = block_on(server.insert("notes", stored_empty, Default::default()))
        .unwrap()
        .row_uuid();
    let everything = Query::from("notes");
    let _live = block_on(db.subscribe(&db.prepare_query(&everything).unwrap(), Default::default()))
        .unwrap();
    pump(&server, &db);
    client.take_reports();

    let rows = all(&db, everything).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(cell(&rows[0], "body"), text("untitled"));
    assert_eq!(cell(&rows[0], "note"), Value::Nullable(None));
    let reports: Vec<_> = client
        .take_reports()
        .into_iter()
        .filter_map(|report| match report {
            Report::Unreadable(unreadable) => Some(unreadable),
            _ => None,
        })
        .collect();
    assert_eq!(reports.len(), 4, "every stored empty cell: {reports:?}");
    assert!(
        reports
            .iter()
            .all(|report| report.code == UnreadableCode::EmptyCell && report.id == Some(row))
    );
    assert!(
        reports
            .iter()
            .any(|report| report.column.as_deref() == Some("body"))
    );
}

/// Contract: a write that creates its row seals every encrypted column it
/// leaves out, so the required stored bytes are always written: an insert
/// or a creating upsert reads such an optional column back as null with no
/// report, and a required column with a declared default as that default.
/// An update that leaves a column out keeps what the row holds.
#[test]
fn omitted_encrypted_columns_are_sealed_when_a_row_is_created() {
    let server = authority();
    let client = open_client_with(Rc::new(MemoryStore::new()), with_body_default());
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);
    let titled = |title: &str| {
        BTreeMap::from([
            ("projectId".to_owned(), Value::Uuid(space)),
            ("title".to_owned(), text(title)),
        ])
    };
    let inserted = block_on(db.insert("notes", titled("inserted"), Default::default()))
        .unwrap()
        .row_uuid();
    let upserted = jazz::ids::RowUuid(uuid::Uuid::now_v7());
    block_on(db.upsert("notes", upserted, titled("upserted"), Default::default())).unwrap();
    let mut with_note = note(space, "kept");
    with_note.insert("note".to_owned(), text("keep me"));
    let updated = block_on(db.insert("notes", with_note, Default::default()))
        .unwrap()
        .row_uuid();
    block_on(db.update(
        "notes",
        updated,
        BTreeMap::from([("body".to_owned(), text("edited"))]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &db);
    client.take_reports();

    let rows = all(&db, Query::from("notes")).unwrap();
    let row = |id| rows.iter().find(|row| row.row_uuid() == id).unwrap();
    for id in [inserted, upserted] {
        assert_eq!(cell(row(id), "note"), Value::Nullable(None));
        assert_eq!(cell(row(id), "rank"), Value::Nullable(None));
        assert_eq!(cell(row(id), "body"), text("untitled"));
    }
    assert_eq!(
        cell(row(updated), "note"),
        Value::Nullable(Some(Box::new(text("keep me"))))
    );
    assert_eq!(cell(row(updated), "body"), text("edited"));
    assert_eq!(client.take_reports(), Vec::new());
    // The cells are sealed, not empty: the server stores no empty value.
    for stored in all(&server, Query::from("notes")).unwrap() {
        assert_ne!(cell(&stored, "note"), Value::Bytes(Vec::new()));
    }
}

/// Contract: on a column a migration added, an equality search for the
/// value its older rows read (the declared default) is refused rather than
/// answered from the index, which holds no token for those rows; a search
/// for any other value is answered.
#[test]
fn searching_for_a_column_s_default_is_refused() {
    let Migrated {
        client, db, space, ..
    } = migrated_with_an_old_row(with_body_default());
    let searched = |body: &str| {
        Query::from("notes")
            .filter(eq(col("projectId"), lit(Value::Uuid(space))))
            .filter(eq(col("body"), lit(text(body))))
    };
    let error = db.prepare_query(&searched("untitled")).unwrap_err();
    assert_eq!(error.code, ErrorCode::Query, "{error:?}");
    block_on(db.insert("notes", note(space, "written"), Default::default())).unwrap();
    let found = all(&db, searched("written")).unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(cell(&found[0], "body"), text("written"));
    client.take_reports();
}

/// Contract: on columns the table had from its first version, every row
/// was sealed with a token, the omitted ones included, so a search for a
/// column's default, or for null on an optional column, finds the matching
/// rows.
#[test]
fn searching_for_a_default_finds_rows_where_every_row_has_a_token() {
    let server = authority();
    let mut declared = with_body_default();
    declared
        .tables
        .get_mut("notes")
        .unwrap()
        .equality
        .push("note".to_owned());
    let client = open_client_with(Rc::new(MemoryStore::new()), declared);
    let (db, _link, _) = online(&client, &server, None);
    let space = project(&db, "plans");
    hold_new_space(&client, &db, space);
    let omitted = block_on(db.insert(
        "notes",
        BTreeMap::from([
            ("projectId".to_owned(), Value::Uuid(space)),
            ("title".to_owned(), text("omitted")),
        ]),
        Default::default(),
    ))
    .unwrap()
    .row_uuid();
    let mut written = note(space, "written");
    written.insert("note".to_owned(), text("noted"));
    block_on(db.insert("notes", written, Default::default())).unwrap();
    let ids = |filter: jazz::query::Predicate| {
        let query = Query::from("notes")
            .filter(eq(col("projectId"), lit(Value::Uuid(space))))
            .filter(filter);
        all(&db, query)
            .unwrap()
            .iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(eq(col("body"), lit(text("untitled")))), [omitted]);
    assert_eq!(ids(eq(col("note"), lit(Value::Nullable(None)))), [omitted]);
    assert_eq!(client.take_reports(), Vec::new());
}

/// Contract: an upsert of a row that is its own space seals the encrypted
/// columns it leaves out when it creates the row, so they read back null
/// with no report; when the row exists, an upsert keeps what it holds.
#[test]
fn a_self_space_upsert_that_creates_its_row_seals_what_it_omits() {
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let (db, _link, _) = online(&client, &server, None);
    let vault = uuid::Uuid::now_v7();
    let held = new_space_key(&client.device().unwrap(), &address_in(&db, "vaults", vault));
    client.record_fixture_rotation_check(held.address());
    block_on(client.keep(held)).unwrap();
    let label = |text_value: &str| BTreeMap::from([("label".to_owned(), text(text_value))]);
    let row = jazz::ids::RowUuid(vault);
    block_on(db.upsert("vaults", row, label("created"), Default::default())).unwrap();
    let read = || all(&db, Query::from("vaults")).unwrap();
    assert_eq!(cell(&read()[0], "secret"), Value::Nullable(None));

    let mut secret = label("filled");
    secret.insert("secret".to_owned(), text(SECRET));
    block_on(db.upsert("vaults", row, secret, Default::default())).unwrap();
    block_on(db.upsert("vaults", row, label("relabelled"), Default::default())).unwrap();
    let rows = read();
    assert_eq!(cell(&rows[0], "label"), text("relabelled"));
    assert_eq!(
        cell(&rows[0], "secret"),
        Value::Nullable(Some(Box::new(text(SECRET))))
    );
    assert_eq!(client.take_reports(), Vec::new());
}
