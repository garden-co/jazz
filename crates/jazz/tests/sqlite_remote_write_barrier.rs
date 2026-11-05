//! Remote one-shot ordering through real transport frames and authority fates.
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ReadOpts, ReadTier, SerializedReadResult, SyncMessage,
    Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid};
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

#[derive(Default)]
struct Gate {
    releases: Cell<usize>,
    queries: Cell<usize>,
    held: RefCell<VecDeque<SyncMessage>>,
}
struct Gated {
    wire: Box<dyn Transport>,
    gate: Rc<Gate>,
}
impl Transport for Gated {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if matches!(message, SyncMessage::Commit { .. }) {
            self.gate.held.borrow_mut().push_back(message);
            return Ok(());
        }
        if matches!(
            message,
            SyncMessage::Fetch { .. } | SyncMessage::ScopedFetchV1 { .. }
        ) {
            self.gate.queries.set(self.gate.queries.get() + 1);
        }
        self.wire.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        while self.gate.releases.get() > 0 {
            let Some(message) = self.gate.held.borrow_mut().pop_front() else {
                break;
            };
            self.wire.send(message).unwrap();
            self.gate.releases.set(self.gate.releases.get() - 1);
        }
        self.wire.try_recv()
    }
}
fn fixture(allow: bool) -> (Db, Db, Rc<Gate>) {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(if allow {
                                PolicyExpr::True
                            } else {
                                PolicyExpr::False
                            }),
                    ),
            )
            .build(),
    )
    .unwrap();
    let alice = AuthorSubject::for_test_bytes([1; 16]);
    let open = |node, author| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )))
        .unwrap()
    };
    let bob = open(2, AuthorSubject::SYSTEM);
    bob.set_authority();
    bob.initialize_authority_catalogue().unwrap();
    let alice_db = open(1, alice);
    let gate = Rc::new(Gate::default());
    let (up, down) = duplex();
    block_on(alice_db.connect_upstream(Box::new(Gated {
        wire: up,
        gate: gate.clone(),
    })));
    bob.accept_subscriber(down, alice);
    for _ in 0..8 {
        turn(&alice_db, &bob);
    }
    (alice_db, bob, gate)
}
fn turn(alice: &Db, bob: &Db) {
    block_on(alice.tick()).unwrap();
    block_on(bob.tick()).unwrap();
}
fn write(alice: &Db, text: &str) {
    block_on(alice.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String(text.into()))]),
        Default::default(),
    ))
    .unwrap();
}
fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
async fn read(db: &Db, tier: ReadTier, checks: &Cell<usize>) -> Result<usize, Error> {
    let result = db
        .all_serialized_query_as(
            &postcard::to_allocvec(&Query::from("notes")).unwrap(),
            ReadOpts::for_read_tier(tier),
            None,
            None,
            None,
            true,
            || {
                checks.set(checks.get() + 1);
                false
            },
            |_| {},
        )
        .await?;
    let SerializedReadResult::Rows(rows) = result else {
        panic!("ordinary row query")
    };
    Ok(rows.len())
}

/// Alice's remote read waits for her preceding write's real fate at Bob, but
/// neither spends its coverage budget nor waits for a write begun afterwards.
/// Alice -- write A (held) -- read -- write B (held) --> Bob
///       <-- fate A -------- query / answer ---------- Bob
#[test]
fn remote_read_orders_preceding_writes_and_releases_on_either_fate() {
    for allow in [true, false] {
        let (alice, bob, gate) = fixture(allow);
        write(&alice, "before read");
        let checks = Cell::new(0);
        let mut pending = pin!(read(&alice, ReadTier::Remote, &checks));
        assert!(poll(pending.as_mut()).is_pending());
        write(&alice, "after read");
        for _ in 0..4 {
            turn(&alice, &bob);
            assert!(poll(pending.as_mut()).is_pending());
        }
        assert_eq!(
            gate.queries.get(),
            0,
            "no remote query may overtake write A"
        );
        assert_eq!(
            checks.get(),
            0,
            "upload ordering does not spend query coverage"
        );
        assert_eq!(
            block_on(read(&alice, ReadTier::LocalFirst, &Cell::new(0))).unwrap(),
            2
        );
        gate.releases.set(1);
        let mut result = None;
        for _ in 0..16 {
            turn(&alice, &bob);
            if let Poll::Ready(rows) = poll(pending.as_mut()) {
                result = Some(rows.unwrap());
                break;
            }
        }
        assert_eq!(result, Some(usize::from(allow)));
        assert_eq!(gate.held.borrow().len(), 1, "later write B remains held");
        assert_eq!(gate.queries.get(), 1);
    }
}

/// Alice cancels while her preceding upload is held. Bob can still decide the
/// write, and cancellation leaves no query to issue after its fate arrives.
#[test]
fn cancelling_an_upload_ordered_read_does_not_cancel_the_write_or_issue_a_query() {
    let (alice, bob, gate) = fixture(true);
    write(&alice, "survives cancellation");
    let checks = Cell::new(0);
    {
        let mut pending = pin!(read(&alice, ReadTier::Remote, &checks));
        assert!(poll(pending.as_mut()).is_pending());
        turn(&alice, &bob);
        assert!(poll(pending.as_mut()).is_pending());
    }
    gate.releases.set(1);
    for _ in 0..8 {
        turn(&alice, &bob);
    }
    assert_eq!(gate.queries.get(), 0);
    let query = bob.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(bob.read(&query).unwrap().len(), 1);
}
