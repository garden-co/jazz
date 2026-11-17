//! Thin-client transactions served from a client linked to the core as the
//! thin session (`jazz::thin_transactions`), as a thin session's backend
//! serves them: the core only accepts finished uploads.

use crate::common;

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::binding_host::{Binding, Link};
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::schema::JazzSchema;
use jazz::thin::{ThinBody, ThinOp};
use jazz::thin_transactions::{
    MAX_OPEN_TRANSACTION_UPLOADS, Step, ThinRouter, ThinTransactions, answer_frames,
};
use jazz::tools::{
    CmpOp, ColumnType, OpenTransactionId, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies,
    TableSchemaBuilder,
};

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            // Each session writes only rows it owns.
            .table(
                TableSchemaBuilder::new("owned")
                    .column("owner", ColumnType::Text)
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(own_rows())
                            .with_update(Some(own_rows()), own_rows()),
                    ),
            )
            // Readable by everyone, writable by no session.
            .table(
                TableSchemaBuilder::new("locked")
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::False),
                    ),
            )
            .build(),
    )
}

/// The row's `owner` is the writing session's subject.
fn own_rows() -> PolicyExpr {
    PolicyExpr::Cmp {
        column: "owner".to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec!["identity".to_owned(), "subject".to_owned()]),
    }
}

fn authority() -> Binding {
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0xee; 16]),
        author: AuthorSubject::SYSTEM,
    };
    let db = block_on(Db::open_history_complete(DbConfig::new(schema(), identity))).unwrap();
    Binding::new(db, identity, true)
}

/// The session's client in its backend: in memory, linked to the core as
/// the session.
struct Session {
    core: Binding,
    client: Binding,
    upstream: Link,
    subscriber: Link,
    transactions: ThinTransactions,
}

fn session_user() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0xa1; 16])
}

impl Session {
    fn open() -> Self {
        Self::open_with(session_user())
    }

    /// The session's client with an identity of its own, `own`, linked to
    /// the core as the session all the same.
    fn open_with(own: AuthorSubject) -> Self {
        let core = authority();
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([0xa1; 16]),
            author: own,
        };
        let db = block_on(Db::open(DbConfig::new(schema(), identity))).unwrap();
        let client = Binding::new(db, identity, false);
        let upstream = client.connect_upstream();
        let subscriber = core
            .accept_subscriber(session_user(), Default::default())
            .unwrap();
        let transactions = ThinTransactions::new(session_user()).unwrap();
        Self {
            core,
            client,
            upstream,
            subscriber,
            transactions,
        }
    }

    /// The host carrier: move frames both ways and tick both sides.
    fn pump(&self) {
        for _ in 0..8 {
            self.subscriber.receive(self.upstream.take_outbound());
            self.upstream.receive(self.subscriber.take_outbound());
            block_on(self.client.tick()).unwrap();
            block_on(self.core.tick()).unwrap();
        }
    }

    fn serve(&mut self, id: u64, op: ThinOp) -> ThinBody {
        assert!(self.transactions.serves(&op), "{op:?} is a transaction's");
        let Self {
            core,
            client,
            upstream,
            subscriber,
            transactions,
        } = self;
        let mut future = pin!(transactions.serve(&client.db, id, &op));
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..50 {
            if let Poll::Ready(body) = future.as_mut().poll(&mut context) {
                return body;
            }
            for _ in 0..8 {
                subscriber.receive(upstream.take_outbound());
                upstream.receive(subscriber.take_outbound());
                block_on(client.tick()).unwrap();
                block_on(core.tick()).unwrap();
            }
        }
        panic!("{op:?} did not complete");
    }

    fn begin(&mut self, kind: &str) -> String {
        let transaction = OpenTransactionId::new().to_string();
        let body = self.serve(
            1,
            ThinOp::Begin {
                transaction: transaction.clone(),
                kind: kind.to_owned(),
            },
        );
        assert_eq!(body, ThinBody::Ok);
        transaction
    }
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

/// What `binding` holds of `table`, sorted.
fn titles_in(binding: &Binding, table: &str) -> Vec<String> {
    let prepared = binding.db.prepare_query(&Query::from(table)).unwrap();
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

fn titles(binding: &Binding) -> Vec<String> {
    titles_in(binding, "notes")
}

fn by_id(row: RowUuid) -> Vec<u8> {
    postcard::to_allocvec(&Query::from("notes").filter(jazz::query::eq(
        jazz::query::col("id"),
        jazz::query::lit(Value::Uuid(row.0)),
    )))
    .unwrap()
}

fn read_titles(body: ThinBody) -> Vec<String> {
    let ThinBody::Rows { rows, .. } = body else {
        panic!("expected rows, got {body:?}");
    };
    jazz::binding_codec::decode_rows(&rows)
        .unwrap()
        .into_iter()
        .flat_map(|batch| batch.rows)
        .map(|row| match &row.cells["title"] {
            Value::String(title) => title.clone(),
            other => panic!("unexpected title {other:?}"),
        })
        .collect()
}

fn insert(transaction: &str, value: &str) -> ThinOp {
    ThinOp::Insert {
        table: "notes".to_owned(),
        cells: title(value),
        options: Vec::new(),
        transaction: Some(transaction.to_owned()),
    }
}

fn update(transaction: &str, row: RowUuid, value: &str) -> ThinOp {
    ThinOp::Update {
        table: "notes".to_owned(),
        row: *row.as_bytes(),
        patch: title(value),
        options: Vec::new(),
        transaction: Some(transaction.to_owned()),
    }
}

fn read(transaction: &str, row: RowUuid) -> ThinOp {
    ThinOp::Query {
        query: by_id(row),
        options: Vec::new(),
        transaction: Some(transaction.to_owned()),
    }
}

fn commit(transaction: &str) -> ThinOp {
    ThinOp::Commit {
        transaction: transaction.to_owned(),
    }
}

/// The core's row, written there before the session's client saw it.
fn core_row(session: &Session, value: &str) -> RowUuid {
    session
        .core
        .insert("notes", &title(value), &serde_json::Value::Null)
        .unwrap()
        .row_id
}

#[test]
fn only_transaction_requests_are_served_from_the_sessions_client() {
    let transactions = ThinTransactions::new(AuthorSubject::for_test_bytes([1; 16])).unwrap();
    let tx = Some("0190f3e4000070008000000000000007".to_owned());
    for op in [
        ThinOp::Begin {
            transaction: tx.clone().unwrap(),
            kind: "mergeable".to_owned(),
        },
        commit(tx.as_deref().unwrap()),
        ThinOp::Rollback {
            transaction: tx.clone().unwrap(),
        },
        insert(tx.as_deref().unwrap(), "x"),
        read(tx.as_deref().unwrap(), RowUuid::from_bytes([1; 16])),
    ] {
        assert!(transactions.serves(&op), "{op:?}");
    }
    for op in [
        ThinOp::Insert {
            table: "notes".to_owned(),
            cells: title("x"),
            options: Vec::new(),
            transaction: None,
        },
        ThinOp::Query {
            query: by_id(RowUuid::from_bytes([1; 16])),
            options: Vec::new(),
            transaction: None,
        },
        ThinOp::Subscribe {
            query: by_id(RowUuid::from_bytes([1; 16])),
            options: Vec::new(),
        },
        ThinOp::CaptureSnapshot,
        // An upload this client never began is the core's.
        ThinOp::StreamChunk {
            stream: 9,
            chunk: Vec::new(),
        },
    ] {
        assert!(!transactions.serves(&op), "{op:?}");
    }
}

#[test]
fn a_mergeable_transaction_reads_the_cores_rows_and_commits_once_the_core_accepted_it() {
    let mut session = Session::open();
    let existing = core_row(&session, "on the core");
    assert!(
        titles(&session.client).is_empty(),
        "the client starts empty"
    );

    let tx = session.begin("mergeable");
    // A row only the core holds is read from the core.
    assert_eq!(
        read_titles(session.serve(2, read(&tx, existing))),
        ["on the core"]
    );
    assert!(matches!(
        session.serve(3, update(&tx, existing, "changed")),
        ThinBody::Staged { .. }
    ));
    assert!(matches!(
        session.serve(4, insert(&tx, "new")),
        ThinBody::Staged { .. }
    ));
    // Its own staged write reads back from the client.
    assert_eq!(
        read_titles(session.serve(5, read(&tx, existing))),
        ["changed"]
    );
    assert_eq!(
        titles(&session.core),
        ["on the core"],
        "nothing reaches the core before the commit"
    );

    // The commit answers only once the core accepted the upload: without
    // the link carrying it, it waits.
    {
        let op = commit(&tx);
        let Session {
            core,
            client,
            upstream,
            subscriber,
            transactions,
        } = &mut session;
        let mut waiting = pin!(transactions.serve(&client.db, 6, &op));
        let mut context = Context::from_waker(Waker::noop());
        assert!(
            waiting.as_mut().poll(&mut context).is_pending(),
            "the commit does not answer at the local commit"
        );
        let body = loop {
            if let Poll::Ready(body) = waiting.as_mut().poll(&mut context) {
                break body;
            }
            subscriber.receive(upstream.take_outbound());
            upstream.receive(subscriber.take_outbound());
            block_on(client.tick()).unwrap();
            block_on(core.tick()).unwrap();
        };
        assert!(matches!(body, ThinBody::Written { .. }), "{body:?}");
    }
    assert_eq!(titles(&session.core), ["changed", "new"]);
    assert!(session.transactions.is_idle());
}

#[test]
fn a_mergeable_read_sees_the_cores_current_row_not_a_stale_copy() {
    let mut session = Session::open();
    let row = core_row(&session, "first");
    let tx = session.begin("mergeable");
    assert_eq!(read_titles(session.serve(2, read(&tx, row))), ["first"]);
    session
        .core
        .update(
            "notes",
            &row.to_bytes(),
            &title("second"),
            &serde_json::Value::Null,
        )
        .unwrap();
    // The client holds the row it read, unsubscribed: the next read asks
    // the core again, as the core's own read would have seen it.
    assert_eq!(read_titles(session.serve(3, read(&tx, row))), ["second"]);
}

#[test]
fn a_serializable_transaction_is_rejected_by_the_core_when_a_read_row_changed() {
    let mut session = Session::open();
    let read_row = core_row(&session, "read");
    let other = core_row(&session, "other");
    let tx = session.begin("serializable");
    assert_eq!(read_titles(session.serve(2, read(&tx, read_row))), ["read"]);
    session
        .core
        .update(
            "notes",
            &read_row.to_bytes(),
            &title("changed on the core"),
            &serde_json::Value::Null,
        )
        .unwrap();
    let staged = session.serve(3, update(&tx, other, "staged"));
    assert!(matches!(staged, ThinBody::Staged { .. }), "{staged:?}");
    let body = session.serve(4, commit(&tx));
    let ThinBody::Error { code, .. } = &body else {
        panic!("expected the core's refusal, got {body:?}");
    };
    assert_eq!(code, "transaction_conflict", "{body:?}");
    assert_eq!(
        titles(&session.core),
        ["changed on the core", "other"],
        "nothing of it applies"
    );
    assert!(session.transactions.is_idle());
}

#[test]
fn a_serializable_transaction_over_unchanged_rows_commits() {
    let mut session = Session::open();
    let row = core_row(&session, "read");
    let tx = session.begin("serializable");
    assert_eq!(read_titles(session.serve(2, read(&tx, row))), ["read"]);
    assert!(matches!(
        session.serve(3, update(&tx, row, "updated")),
        ThinBody::Staged { .. }
    ));
    let body = session.serve(4, commit(&tx));
    assert!(matches!(body, ThinBody::Written { .. }), "{body:?}");
    assert_eq!(titles(&session.core), ["updated"]);
}

#[test]
fn a_write_the_sessions_policies_refuse_fails_the_commit_and_applies_nothing() {
    let mut session = Session::open();
    let tx = session.begin("mergeable");
    session.serve(2, insert(&tx, "allowed"));
    session.serve(
        3,
        ThinOp::Insert {
            table: "locked".to_owned(),
            cells: title("refused"),
            options: Vec::new(),
            transaction: Some(tx.clone()),
        },
    );
    let body = session.serve(4, commit(&tx));
    assert!(matches!(body, ThinBody::Error { .. }), "{body:?}");
    assert!(titles(&session.core).is_empty(), "nothing of it applies");
    assert!(titles_in(&session.core, "locked").is_empty());
}

#[test]
fn a_rolled_back_or_released_transaction_never_reaches_the_core() {
    let mut session = Session::open();
    let tx = session.begin("mergeable");
    session.serve(2, insert(&tx, "rolled back"));
    assert_eq!(
        session.serve(
            3,
            ThinOp::Rollback {
                transaction: tx.clone()
            }
        ),
        ThinBody::Ok
    );
    assert!(matches!(
        session.serve(4, commit(&tx)),
        ThinBody::Error { .. }
    ));

    let open = session.begin("serializable");
    session.serve(5, insert(&open, "left open"));
    let Session {
        core,
        client,
        upstream,
        subscriber,
        transactions,
    } = session;
    block_on(transactions.release(&client.db));
    let session = Session {
        core,
        client,
        upstream,
        subscriber,
        transactions: ThinTransactions::new(AuthorSubject::for_test_bytes([0xa1; 16])).unwrap(),
    };
    session.pump();
    assert!(titles(&session.core).is_empty());
    assert!(titles(&session.client).is_empty());
}

#[test]
fn staged_writes_run_as_the_session() {
    let mut session = Session::open();
    let tx = session.begin("mergeable");
    for options in [
        serde_json::json!({ "author": "someone else" }),
        serde_json::json!({ "writeKey": { "node": "00".repeat(16), "seq": 1 } }),
    ] {
        let body = session.serve(
            2,
            ThinOp::Insert {
                table: "notes".to_owned(),
                cells: title("x"),
                options: serde_json::to_vec(&options).unwrap(),
                transaction: Some(tx.clone()),
            },
        );
        assert!(
            matches!(&body, ThinBody::Error { code, .. } if code == "Argument"),
            "{options}: {body:?}"
        );
    }
    assert!(matches!(
        session.serve(3, insert("0190f3e4000070008000000000000007", "x")),
        ThinBody::Error { .. }
    ));
}

#[test]
fn a_value_streamed_into_a_transaction_commits_with_it() {
    let mut session = Session::open();
    let tx = session.begin("mergeable");
    let row = RowUuid::from_bytes([3; 16]);
    let begin = ThinOp::BeginStream {
        table: "notes".to_owned(),
        row: *row.as_bytes(),
        cells: {
            let descriptor = RecordDescriptor::new(Vec::<(String, ValueType)>::new());
            jazz::binding_codec::encode_named_cells(&OwnedRecord::new(
                descriptor.create(&[]).unwrap(),
                descriptor,
            ))
            .unwrap()
        },
        column: "title".to_owned(),
        mutation: "insert".to_owned(),
        options: Vec::new(),
        transaction: Some(tx.clone()),
    };
    assert_eq!(session.serve(7, begin), ThinBody::Ok);
    let text = "x".repeat(200 * 1024);
    for chunk in text.as_bytes().chunks(64 * 1024) {
        assert_eq!(
            session.serve(
                8,
                ThinOp::StreamChunk {
                    stream: 7,
                    chunk: chunk.to_vec()
                }
            ),
            ThinBody::Ok
        );
    }
    assert!(matches!(
        session.serve(9, ThinOp::FinishStream { stream: 7 }),
        ThinBody::Staged { .. }
    ));
    assert!(titles(&session.core).is_empty());
    assert!(matches!(
        session.serve(10, commit(&tx)),
        ThinBody::Written { .. }
    ));
    assert_eq!(titles(&session.core), [text]);
}

#[test]
fn a_transaction_writes_rows_it_never_read() {
    for kind in ["mergeable", "serializable"] {
        let mut session = Session::open();
        let updated = core_row(&session, "to update");
        let deleted = core_row(&session, "to delete");
        let tx = session.begin(kind);
        // Rows the session's client never held, as the core's own
        // transactions wrote to any row it held.
        let staged = session.serve(2, update(&tx, updated, "updated"));
        assert!(
            matches!(staged, ThinBody::Staged { .. }),
            "{kind}: {staged:?}"
        );
        let staged = session.serve(
            3,
            ThinOp::Delete {
                table: "notes".to_owned(),
                row: *deleted.as_bytes(),
                options: Vec::new(),
                transaction: Some(tx.clone()),
            },
        );
        assert!(
            matches!(staged, ThinBody::Staged { .. }),
            "{kind}: {staged:?}"
        );
        let body = session.serve(4, commit(&tx));
        assert!(matches!(body, ThinBody::Written { .. }), "{kind}: {body:?}");
        assert_eq!(titles(&session.core), ["updated"], "{kind}");
    }
}

fn owned(owner: AuthorSubject, value: &str) -> Vec<u8> {
    let descriptor =
        RecordDescriptor::new([("owner", ValueType::String), ("title", ValueType::String)]);
    let record = OwnedRecord::new(
        descriptor
            .create(&[
                Value::String(owner.principal_parts().1),
                Value::String(value.to_owned()),
            ])
            .unwrap(),
        descriptor,
    );
    jazz::binding_codec::encode_named_cells(&record).unwrap()
}

fn insert_owned(transaction: &str, owner: AuthorSubject, value: &str) -> ThinOp {
    ThinOp::Insert {
        table: "owned".to_owned(),
        cells: owned(owner, value),
        options: Vec::new(),
        transaction: Some(transaction.to_owned()),
    }
}

#[test]
fn staged_writes_are_the_sessions_whatever_the_clients_own_identity() {
    // The backend's client has an identity of its own; the session's
    // writes are the session's all the same, and its policies decide.
    let backend = AuthorSubject::for_test_bytes([0xb0; 16]);
    let mut session = Session::open_with(backend);
    let tx = session.begin("mergeable");
    session.serve(2, insert_owned(&tx, session_user(), "mine"));
    let body = session.serve(3, commit(&tx));
    assert!(matches!(body, ThinBody::Written { .. }), "{body:?}");
    assert_eq!(titles_in(&session.core, "owned"), ["mine"]);

    // A row owned by anyone else, the backend's own identity included, is
    // refused, and the transaction with it.
    for other in [backend, AuthorSubject::for_test_bytes([0xc0; 16])] {
        let tx = session.begin("mergeable");
        session.serve(4, insert(&tx, "beside it"));
        session.serve(5, insert_owned(&tx, other, "not mine"));
        let body = session.serve(6, commit(&tx));
        assert!(matches!(body, ThinBody::Error { .. }), "{body:?}");
    }
    assert_eq!(titles_in(&session.core, "owned"), ["mine"]);
    assert!(titles(&session.core).is_empty());
}

#[test]
fn a_system_session_is_refused() {
    assert!(ThinTransactions::new(AuthorSubject::SYSTEM).is_err());
}

#[test]
fn a_rolled_back_or_released_transaction_leaves_no_handle() {
    let mut session = Session::open();
    let rolled_back = session.begin("mergeable");
    session.serve(2, insert(&rolled_back, "x"));
    session.serve(
        3,
        ThinOp::Rollback {
            transaction: rolled_back.clone(),
        },
    );
    let released = session.begin("serializable");
    session.serve(4, insert(&released, "y"));
    let Session {
        client,
        transactions,
        ..
    } = session;
    block_on(transactions.release(&client.db));
    for (tx, serializable) in [(rolled_back, false), (released, true)] {
        let id = jazz::binding_host::open_transaction(&tx).unwrap();
        let commit = if serializable {
            block_on(client.db.commit_serializable_handle(id))
        } else {
            block_on(client.db.commit_mergeable_handle(id))
        };
        assert!(commit.is_err(), "{tx} has no handle left");
    }
}

#[test]
fn a_write_refused_for_its_rows_state_fails_the_commit() {
    let mut session = Session::open();
    let tx = session.begin("mergeable");
    // Patching nothing cannot create a row: the call stages, and the
    // commit carries the refusal, before anything is uploaded.
    let staged = session.serve(
        2,
        ThinOp::Upsert {
            table: "notes".to_owned(),
            row: [0x42; 16],
            cells: {
                let descriptor = RecordDescriptor::new(Vec::<(String, ValueType)>::new());
                jazz::binding_codec::encode_named_cells(&OwnedRecord::new(
                    descriptor.create(&[]).unwrap(),
                    descriptor,
                ))
                .unwrap()
            },
            options: Vec::new(),
            transaction: Some(tx.clone()),
        },
    );
    assert!(matches!(staged, ThinBody::Staged { .. }), "{staged:?}");
    session.serve(3, insert(&tx, "kept out"));
    let body = session.serve(4, commit(&tx));
    let ThinBody::Error { message, .. } = &body else {
        panic!("expected the refusal, got {body:?}");
    };
    assert!(
        message.contains("missing required field `title`"),
        "{body:?}"
    );
    assert!(titles(&session.core).is_empty());
    assert!(session.transactions.is_idle());
}

#[test]
fn a_write_builds_on_the_cores_row_not_a_stale_copy() {
    let mut session = Session::open();
    let row = core_row(&session, "first");
    // The client holds the row after reading it...
    let tx = session.begin("mergeable");
    assert_eq!(read_titles(session.serve(2, read(&tx, row))), ["first"]);
    session.serve(3, ThinOp::Rollback { transaction: tx });
    // ...and the core deletes it since.
    session
        .core
        .delete("notes", &row.to_bytes(), &serde_json::Value::Null)
        .unwrap();
    let tx = session.begin("mergeable");
    // Read from the core again, the row is gone: the write is refused as
    // it is staged. (The core refused it at the commit; either way the
    // thin client's transaction fails, since it rolls back instead of
    // committing once a staged write failed.)
    let staged = session.serve(4, update(&tx, row, "on a deleted row"));
    assert!(
        matches!(&staged, ThinBody::Error { code, .. } if code == "write_rejected"),
        "{staged:?}"
    );
    session.serve(5, ThinOp::Rollback { transaction: tx });
    assert!(titles(&session.core).is_empty(), "the row stays deleted");
}

const TX: &str = "0190f3e4000070008000000000000007";

fn frame(id: u64, op: ThinOp) -> Vec<u8> {
    jazz::wire::encode_frame(&jazz::wire::WireFrame::ThinRequest(
        jazz::thin::ThinRequest { id, op },
    ))
    .unwrap()
}

fn plain_insert(value: &str) -> ThinOp {
    ThinOp::Insert {
        table: "notes".to_owned(),
        cells: title(value),
        options: Vec::new(),
        transaction: None,
    }
}

fn plain_read(row: RowUuid) -> ThinOp {
    ThinOp::Query {
        query: by_id(row),
        options: Vec::new(),
        transaction: None,
    }
}

/// An answer of `Ok` to request `id`.
fn ok(id: u64) -> jazz::thin::ThinResponse {
    jazz::thin::ThinResponse {
        id,
        body: ThinBody::Ok,
    }
}

/// The core's answer to request `id`, as the client receives it.
fn core_answer(id: u64) -> Vec<u8> {
    jazz::wire::encode_frame(&jazz::wire::WireFrame::ThinResponse(
        jazz::thin::ThinResponse {
            id,
            body: ThinBody::Ok,
        },
    ))
    .unwrap()
}

/// What `steps` release: core frames, and backend requests by id.
fn released(steps: Vec<Step>) -> Vec<String> {
    steps
        .into_iter()
        .map(|step| match step {
            Step::ToCore(frames) => format!("core:{}", frames.len()),
            Step::ToBackend(request) => format!("backend:{}", request.id),
        })
        .collect()
}

#[test]
fn a_thin_connection_routes_transaction_requests_to_the_backend_and_the_rest_to_the_core() {
    let mut router = ThinRouter::default();
    let core_frames = vec![
        frame(
            1,
            ThinOp::Subscribe {
                query: by_id(RowUuid::from_bytes([1; 16])),
                options: Vec::new(),
            },
        ),
        // A frame that is not a thin request passes through too.
        jazz::wire::encode_frame(&jazz::wire::WireFrame::Heartbeat).unwrap(),
    ];
    router
        .from_client(vec![
            core_frames[0].clone(),
            core_frames[1].clone(),
            frame(
                2,
                ThinOp::Begin {
                    transaction: TX.to_owned(),
                    kind: "mergeable".to_owned(),
                },
            ),
            frame(3, insert(TX, "staged")),
        ])
        .unwrap();
    // A live query is not waited on: the transaction goes on at once.
    let steps = router.ready();
    assert_eq!(steps[0], Step::ToCore(core_frames));
    assert_eq!(released(steps), ["core:2", "backend:2", "backend:3"]);
    assert!(router.from_client(vec![vec![0xff, 0xff, 0xff]]).is_err());
}

#[test]
fn a_transaction_request_waits_for_the_plain_write_sent_before_it() {
    let mut router = ThinRouter::default();
    let row = RowUuid::from_bytes([7; 16]);
    // insert(x), not awaited, then the transaction reads x.
    router
        .from_client(vec![
            frame(1, plain_insert("x")),
            frame(
                2,
                ThinOp::Begin {
                    transaction: TX.to_owned(),
                    kind: "mergeable".to_owned(),
                },
            ),
            frame(3, read(TX, row)),
        ])
        .unwrap();
    assert_eq!(released(router.ready()), ["core:1"]);
    assert!(
        router.ready().is_empty(),
        "the core has not answered the write"
    );
    router.from_core(&[core_answer(9)]);
    assert!(router.ready().is_empty(), "another request's answer");
    router.from_core(&[core_answer(1)]);
    assert_eq!(released(router.ready()), ["backend:2", "backend:3"]);
}

#[test]
fn a_plain_read_sent_after_a_commit_waits_for_the_commit() {
    let mut router = ThinRouter::default();
    let row = RowUuid::from_bytes([7; 16]);
    router
        .from_client(vec![frame(5, commit(TX)), frame(6, plain_read(row))])
        .unwrap();
    assert_eq!(released(router.ready()), ["backend:5"]);
    assert!(router.ready().is_empty(), "the commit is not answered yet");
    router.answered(&ok(5));
    assert_eq!(released(router.ready()), ["core:1"]);
    // Later backend requests wait for the read in turn.
    router.from_client(vec![frame(7, read(TX, row))]).unwrap();
    assert!(router.ready().is_empty());
    router.from_core(&[core_answer(6)]);
    assert_eq!(released(router.ready()), ["backend:7"]);
}

#[test]
fn a_split_answer_from_the_core_frees_what_waits_on_it() {
    let mut router = ThinRouter::default();
    router
        .from_client(vec![
            frame(1, plain_read(RowUuid::from_bytes([1; 16]))),
            frame(2, commit(TX)),
        ])
        .unwrap();
    assert_eq!(released(router.ready()), ["core:1"]);
    let parts = answer_frames(jazz::thin::ThinResponse {
        id: 1,
        body: ThinBody::Rows {
            rows: vec![7; 3 * 1024 * 1024],
            relation: false,
        },
    });
    assert!(parts.len() > 1);
    router.from_core(&parts[..1]);
    assert!(router.ready().is_empty(), "not whole yet");
    router.from_core(&parts[1..]);
    assert_eq!(released(router.ready()), ["backend:2"]);
}

#[test]
fn an_upload_begun_in_a_transaction_is_the_backends_until_it_ends() {
    let begin = |id: u64, transaction: Option<&str>| {
        frame(
            id,
            ThinOp::BeginStream {
                table: "notes".to_owned(),
                row: [3; 16],
                cells: Vec::new(),
                column: "title".to_owned(),
                mutation: "insert".to_owned(),
                options: Vec::new(),
                transaction: transaction.map(str::to_owned),
            },
        )
    };
    let chunk = |id: u64, stream: u64| {
        frame(
            id,
            ThinOp::StreamChunk {
                stream,
                chunk: vec![1, 2, 3],
            },
        )
    };
    let mut router = ThinRouter::default();
    router
        .from_client(vec![
            begin(7, Some(TX)),
            chunk(8, 7),
            frame(9, ThinOp::FinishStream { stream: 7 }),
        ])
        .unwrap();
    assert_eq!(
        released(router.ready()),
        ["backend:7", "backend:8", "backend:9"]
    );
    for id in [7, 8, 9] {
        router.answered(&ok(id));
    }
    // Finished: a chunk naming it is the core's now, as is an upload
    // outside a transaction.
    router
        .from_client(vec![chunk(10, 7), begin(11, None), chunk(12, 11)])
        .unwrap();
    assert_eq!(released(router.ready()), ["core:3"]);
    for id in [10, 11, 12] {
        router.from_core(&[core_answer(id)]);
    }

    // A transaction's end ends its uploads, and only so many stay open.
    let mut ids = 100..;
    let mut frames = Vec::new();
    for _ in 0..=MAX_OPEN_TRANSACTION_UPLOADS {
        frames.push(begin(ids.next().unwrap(), Some(TX)));
    }
    let last = 100 + MAX_OPEN_TRANSACTION_UPLOADS as u64;
    frames.push(chunk(ids.next().unwrap(), last));
    frames.push(chunk(ids.next().unwrap(), 100));
    router.from_client(frames).unwrap();
    let steps = released(router.ready());
    assert_eq!(steps.len(), MAX_OPEN_TRANSACTION_UPLOADS + 1, "{steps:?}");
    assert!(steps.iter().all(|step| step.starts_with("backend:")));
    for id in 100..=last {
        router.answered(&ok(id));
    }
    // The chunk of the upload past the bound went to the core; the other,
    // the backend's, waits for its answer.
    assert_eq!(released(router.ready()), ["core:1"]);
    router.from_core(&[core_answer(117)]);
    assert_eq!(released(router.ready()), ["backend:118"]);
    router.answered(&ok(118));
    router
        .from_client(vec![frame(200, commit(TX)), chunk(201, 100)])
        .unwrap();
    assert_eq!(released(router.ready()), ["backend:200"]);
    router.answered(&ok(200));
    assert_eq!(
        released(router.ready()),
        ["core:1"],
        "ended with its transaction"
    );
}

#[test]
fn a_split_request_is_joined_to_route_it_and_reaches_the_core_as_its_parts() {
    let big = "x".repeat(3 * 1024 * 1024);
    let mut router = ThinRouter::default();

    let staged = jazz::thin::thin_wire_frames(frame(1, insert(TX, &big))).unwrap();
    assert!(staged.len() > 1, "split");
    for part in staged {
        router.from_client(vec![part]).unwrap();
    }
    let steps = router.ready();
    let [Step::ToBackend(request)] = steps.as_slice() else {
        panic!("expected the joined request, got {steps:?}");
    };
    assert_eq!(request.op, insert(TX, &big));
    router.answered(&ok(1));

    let plain = jazz::thin::thin_wire_frames(frame(2, plain_insert(&big))).unwrap();
    router.from_client(plain.clone()).unwrap();
    assert_eq!(router.ready(), [Step::ToCore(plain)], "the same parts");
}

#[test]
fn a_large_answer_travels_in_parts_the_client_joins() {
    let answer = jazz::thin::ThinResponse {
        id: 9,
        body: ThinBody::Rows {
            rows: vec![7u8; 3 * 1024 * 1024],
            relation: false,
        },
    };
    let frames = answer_frames(answer.clone());
    assert!(frames.len() > 1);
    let mut assembler = jazz::thin::ThinAssembler::default();
    let mut joined = None;
    for frame in frames {
        let jazz::wire::WireFrame::ThinPart(part) = jazz::wire::decode_frame(&frame).unwrap()
        else {
            panic!("expected parts");
        };
        joined = assembler.push(part).unwrap();
    }
    let jazz::wire::WireFrame::ThinResponse(joined) =
        jazz::wire::decode_postcard_frame(&joined.unwrap()).unwrap()
    else {
        panic!("expected the answer");
    };
    assert_eq!(joined, answer);
    let small = jazz::thin::ThinResponse {
        id: 1,
        body: ThinBody::Ok,
    };
    assert_eq!(answer_frames(small).len(), 1);
}

#[test]
fn the_router_keeps_the_schema_the_client_bound_once_the_core_accepted_it() {
    let mut router = ThinRouter::default();
    assert!(router.schema().is_none());
    let bind = |id: u64, schema: &[u8]| {
        frame(
            id,
            ThinOp::Schema {
                schema: schema.to_vec(),
            },
        )
    };
    router.from_client(vec![bind(1, b"{}")]).unwrap();
    assert_eq!(released(router.ready()), ["core:1"]);
    assert!(router.schema().is_none(), "not before the core accepted it");
    router.from_core(&[core_answer(1)]);
    assert_eq!(router.schema(), Some(&b"{}"[..]));

    // A version the core refuses binds nothing: not the earlier one either.
    router
        .from_client(vec![bind(2, b"{\"forged\":1}")])
        .unwrap();
    assert_eq!(released(router.ready()), ["core:1"]);
    assert!(router.schema().is_none());
    router.from_core(&[
        jazz::wire::encode_frame(&jazz::wire::WireFrame::ThinResponse(
            jazz::thin::ThinResponse {
                id: 2,
                body: ThinBody::Error {
                    code: "SchemaNotDeployed".to_owned(),
                    message: "not admitted".to_owned(),
                },
            },
        ))
        .unwrap(),
    ]);
    assert!(router.schema().is_none());
}

#[test]
fn routed_requests_are_served_by_the_sessions_transactions() {
    let mut session = Session::open();
    let existing = core_row(&session, "on the core");
    let tx = OpenTransactionId::new().to_string();
    let mut router = ThinRouter::default();
    router
        .from_client(vec![
            frame(
                1,
                ThinOp::Begin {
                    transaction: tx.clone(),
                    kind: "serializable".to_owned(),
                },
            ),
            frame(2, update(&tx, existing, "changed")),
            frame(3, commit(&tx)),
        ])
        .unwrap();
    let mut answers = Vec::new();
    for step in router.ready() {
        let Step::ToBackend(request) = step else {
            panic!("all the backend's");
        };
        answers.push(session.serve(request.id, request.op));
    }
    assert_eq!(answers[0], ThinBody::Ok);
    assert!(matches!(answers[1], ThinBody::Staged { .. }));
    assert!(matches!(answers[2], ThinBody::Written { .. }));
    assert_eq!(titles(&session.core), ["changed"]);
}

#[test]
fn an_upload_the_backend_refused_to_begin_is_not_one() {
    let mut router = ThinRouter::default();
    router
        .from_client(vec![frame(
            7,
            ThinOp::BeginStream {
                table: "notes".to_owned(),
                row: [3; 16],
                cells: Vec::new(),
                column: "title".to_owned(),
                mutation: "insert".to_owned(),
                options: Vec::new(),
                transaction: Some(TX.to_owned()),
            },
        )])
        .unwrap();
    assert_eq!(released(router.ready()), ["backend:7"]);
    assert!(!router.is_waiting(), "nothing routed waits");
    router.answered(&jazz::thin::ThinResponse {
        id: 7,
        body: ThinBody::Error {
            code: "Argument".to_owned(),
            message: "refused".to_owned(),
        },
    });
    router
        .from_client(vec![frame(
            8,
            ThinOp::StreamChunk {
                stream: 7,
                chunk: vec![1],
            },
        )])
        .unwrap();
    assert_eq!(released(router.ready()), ["core:1"]);
}

#[test]
fn the_router_says_when_it_waits_on_an_answer() {
    let mut router = ThinRouter::default();
    router
        .from_client(vec![frame(1, plain_insert("x")), frame(2, commit(TX))])
        .unwrap();
    assert!(!router.ready().is_empty());
    assert!(router.is_waiting(), "the commit waits on the core");
    router.from_core(&[core_answer(1)]);
    assert!(!router.is_waiting());
}

#[test]
fn a_staged_write_refused_outright_fails_the_transaction() {
    let mut session = Session::open();
    let tx = session.begin("mergeable");
    session.serve(2, insert(&tx, "kept out"));
    // No such row: refused as it is staged.
    let refused = session.serve(3, update(&tx, RowUuid::from_bytes([0x99; 16]), "nowhere"));
    assert!(matches!(refused, ThinBody::Error { .. }), "{refused:?}");
    // A client that commits all the same commits nothing.
    let body = session.serve(4, commit(&tx));
    assert!(matches!(body, ThinBody::Error { .. }), "{body:?}");
    assert!(titles(&session.core).is_empty());
}

#[test]
fn a_trusted_sessions_writes_are_the_backends_own() {
    // A trusted backend client's session: its client is the backend's,
    // linked as it, and its writes are the database's own.
    let core = authority();
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0xb1; 16]),
        author: AuthorSubject::SYSTEM,
    };
    let db = block_on(Db::open(DbConfig::new(schema(), identity))).unwrap();
    let client = Binding::new(db, identity, true);
    let upstream = client.connect_upstream();
    let subscriber = core
        .accept_subscriber(AuthorSubject::SYSTEM, Default::default())
        .unwrap();
    let mut session = Session {
        core,
        client,
        upstream,
        subscriber,
        transactions: ThinTransactions::trusted(),
    };
    let tx = session.begin("mergeable");
    session.serve(2, insert(&tx, "the backend's"));
    let body = session.serve(3, commit(&tx));
    assert!(matches!(body, ThinBody::Written { .. }), "{body:?}");
    assert_eq!(titles(&session.core), ["the backend's"]);
}
