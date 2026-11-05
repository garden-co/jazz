//! Binding one-shot reads wait for their authorized large values to arrive.
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, FirstLoad, InsertOptions, ReadOpts, ReadTier, RemoteLinkHint,
    SerializedReadResult, SyncMessage, Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::{Pin, pin},
    rc::Rc,
    task::{Context, Poll, Waker},
};
const SIZE: usize = 300 * 1024;
fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([1; 16])
}
fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("owner", ColumnType::Uuid)
                    .column("body", ColumnType::Bytea)
                    .index_only(["owner"])
                    .policies(TablePolicies::new().with_select(PolicyExpr::eq_session(
                        "owner",
                        vec!["user".into(), "account".into()],
                    ))),
            )
            .build(),
    )
    .unwrap()
}
fn open(node: u8, who: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: who,
        },
    )))
    .unwrap()
}
#[derive(Default)]
struct Gate {
    open: Cell<bool>,
    requests: Cell<usize>,
    held: RefCell<VecDeque<SyncMessage>>,
}
struct Gated {
    wire: Box<dyn Transport>,
    gate: Rc<Gate>,
}
impl Transport for Gated {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if matches!(message, SyncMessage::ChunkRequests { .. }) {
            self.gate.requests.set(self.gate.requests.get() + 1);
        }
        self.wire.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if self.gate.open.get() {
            return self
                .gate
                .held
                .borrow_mut()
                .pop_front()
                .or_else(|| self.wire.try_recv());
        }
        while let Some(message) = self.wire.try_recv() {
            if matches!(message, SyncMessage::Chunks { .. }) {
                self.gate.held.borrow_mut().push_back(message);
            } else {
                return Some(message);
            }
        }
        None
    }
}
fn fixture(scoped: bool) -> (Db, Db, Rc<Gate>) {
    let server = open(90, AuthorSubject::SYSTEM);
    for (id, who, byte) in [
        (1, alice(), 7),
        (2, AuthorSubject::for_test_bytes([2; 16]), 9),
    ] {
        block_on(server.insert(
            "docs",
            BTreeMap::from([
                ("owner".into(), Value::Uuid(who.account_id().unwrap().0)),
                ("body".into(), Value::Bytes(vec![byte; SIZE])),
            ]),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([id; 16])),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let who = if scoped {
        AuthorSubject::SYSTEM
    } else {
        alice()
    };
    let replica = open(1, who);
    let gate = Rc::new(Gate::default());
    let (up, down) = duplex();
    block_on(replica.connect_upstream(Box::new(Gated {
        wire: up,
        gate: gate.clone(),
    })));
    server.accept_subscriber(down, who);
    (server, replica, gate)
}
fn turn(server: &Db, replica: &Db) {
    block_on(replica.tick()).unwrap();
    block_on(server.tick()).unwrap();
}
fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
async fn read(db: &Db, options: ReadOpts, scoped: bool) -> Result<SerializedReadResult, Error> {
    db.all_serialized_query_as(
        &postcard::to_allocvec(&Query::from("docs")).unwrap(),
        options,
        None,
        scoped.then(|| (alice(), BTreeMap::new())),
        scoped.then_some(alice()),
        false,
        || false,
        |_| {},
    )
    .await
}
fn complete(
    server: &Db,
    replica: &Db,
    future: impl Future<Output = Result<SerializedReadResult, Error>>,
) -> SerializedReadResult {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(result) = poll(future.as_mut()) {
            return result.unwrap();
        }
        turn(server, replica);
    }
    panic!("read did not hydrate")
}
fn assert_alice(result: SerializedReadResult) {
    let SerializedReadResult::Rows(rows) = result else {
        panic!("flat rows expected")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), RowUuid::from_bytes([1; 16]));
    assert_eq!(
        rows[0].cells().find(|(name, _)| *name == "body").unwrap().1,
        Some(&Value::Bytes(vec![7; SIZE]))
    );
}

/// Alice's remote and bounded local-first reads remain pending while Bob's
/// authority has sent the row but not its chunks. Ordinary session and trusted
/// backend scoped reads both return Alice's complete value and exclude Mallory's.
#[test]
fn one_shot_reads_wait_for_authorized_chunks() {
    for scoped in [false, true] {
        for tier in [ReadTier::Remote, ReadTier::LocalFirst] {
            let (server, replica, gate) = fixture(scoped);
            let mut opts = ReadOpts::for_read_tier(tier);
            opts.first_load = FirstLoad::WaitForRemote { timeout_ms: 1_000 };
            let mut pending = pin!(read(&replica, opts, scoped));
            for _ in 0..12 {
                let state = poll(pending.as_mut());
                assert!(
                    state.is_pending(),
                    "scoped={scoped}, tier={tier:?}: {state:?}"
                );
                turn(&server, &replica);
            }
            assert!(gate.requests.get() > 0, "normal sync requested chunks");
            assert!(
                !gate.held.borrow().is_empty(),
                "the authority supplied chunks"
            );
            gate.open.set(true);
            assert_alice(complete(&server, &replica, pending));
        }
    }
}

/// Alice can cancel a read while chunks are delayed without changing the next
/// read's result. A local-only read reports unavailable content immediately;
/// disconnection also terminates the remote hydration wait instead of hanging.
#[test]
fn hydration_cancellation_local_only_and_disconnect_are_bounded() {
    let (server, replica, gate) = fixture(false);
    {
        let mut cancelled = pin!(read(
            &replica,
            ReadOpts::for_read_tier(ReadTier::Remote),
            false
        ));
        for _ in 0..12 {
            assert!(poll(cancelled.as_mut()).is_pending());
            turn(&server, &replica);
        }
        let mut frozen = ReadOpts::for_read_tier(ReadTier::Remote);
        frozen.read_view.source = jazz::protocol::ReadViewSourceSpec::Snapshot {
            snapshot: block_on(replica.capture_snapshot()).unwrap(),
        };
        for opts in [
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            ReadOpts::default(),
            frozen,
        ] {
            let mut local = pin!(read(&replica, opts, false));
            let Poll::Ready(Err(error)) = poll(local.as_mut()) else {
                panic!("immediate and frozen reads must not wait")
            };
            assert!(error.is_missing_chunks());
        }
    }
    let mut pending = pin!(read(
        &replica,
        ReadOpts::for_read_tier(ReadTier::Remote),
        false
    ));
    for _ in 0..12 {
        assert!(poll(pending.as_mut()).is_pending());
        turn(&server, &replica);
    }
    replica.set_remote_link_hint(RemoteLinkHint::Failed);
    let Poll::Ready(Err(error)) = poll(pending.as_mut()) else {
        panic!("disconnected read must stop")
    };
    assert_eq!(error.code, jazz::db::ErrorCode::NotObserved);
    replica.set_remote_link_hint(RemoteLinkHint::Live);
    gate.open.set(true);
    assert_alice(complete(
        &server,
        &replica,
        read(&replica, ReadOpts::for_read_tier(ReadTier::Remote), false),
    ));
}

/// Alice's local-first deadline includes hydration, so a responding authority
/// that withholds chunks cannot prolong the caller's requested wait budget.
#[test]
fn local_first_hydration_respects_the_original_deadline() {
    let (server, replica, _gate) = fixture(false);
    let mut opts = ReadOpts::for_read_tier(ReadTier::LocalFirst);
    opts.first_load = FirstLoad::WaitForRemote { timeout_ms: 200 };
    let mut pending = pin!(read(&replica, opts, false));
    for _ in 0..12 {
        assert!(poll(pending.as_mut()).is_pending());
        turn(&server, &replica);
    }
    std::thread::sleep(std::time::Duration::from_millis(250));
    let Poll::Ready(Err(error)) = poll(pending.as_mut()) else {
        panic!("hydration exceeded read deadline")
    };
    assert_eq!(error.code, jazz::db::ErrorCode::NotObserved);
}

/// Alice reconnects while her original authority response awaits chunks. The
/// old read must not complete using a new connection's evidence, and a fresh
/// read must hydrate normally on that connection.
#[test]
fn reconnect_interrupts_hydration_without_poisoning_the_next_read() {
    let (server, replica, _gate) = fixture(false);
    let mut pending = pin!(read(
        &replica,
        ReadOpts::for_read_tier(ReadTier::Remote),
        false
    ));
    for _ in 0..12 {
        assert!(poll(pending.as_mut()).is_pending());
        turn(&server, &replica);
    }
    let (up, down) = duplex();
    block_on(replica.connect_upstream(up));
    server.accept_subscriber(down, alice());
    let Poll::Ready(Err(error)) = poll(pending.as_mut()) else {
        panic!("old read survived reconnect")
    };
    assert_eq!(error.code, jazz::db::ErrorCode::NotObserved);
    assert!(error.message.contains("reconnect"));
    assert_alice(complete(
        &server,
        &replica,
        read(&replica, ReadOpts::for_read_tier(ReadTier::Remote), false),
    ));
}
