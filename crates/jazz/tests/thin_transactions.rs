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
use jazz::thin_transactions::ThinTransactions;
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
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

impl Session {
    fn open() -> Self {
        let core = authority();
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([0xa1; 16]),
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
        };
        let db = block_on(Db::open(DbConfig::new(schema(), identity))).unwrap();
        let client = Binding::new(db, identity, false);
        let upstream = client.connect_upstream();
        let subscriber = core
            .accept_subscriber(client.author(), Default::default())
            .unwrap();
        let transactions = ThinTransactions::new(client.author());
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
    let transactions = ThinTransactions::new(AuthorSubject::for_test_bytes([1; 16]));
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
        transactions: ThinTransactions::new(AuthorSubject::SYSTEM),
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
