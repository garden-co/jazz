//! Reads inside an exclusive transaction on a partial client hydrate through
//! the authority at the transaction's frozen snapshot (INV-TX-13), whatever
//! tier the caller asked for (garden-co/jazz#3694).
//!
//! A partial client's snapshot cut advances with any authority receipt, so
//! without that hydration an exclusive read evaluates whatever the replica
//! happens to hold: a row it never received reads as absent, and a row the
//! authority already deleted still reads as present. Each test drives a Core
//! shell and real clients over queued wire transports, with the reads issued
//! through the same serialized entry point the language bindings use.
#![cfg(feature = "runtime")]

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

mod common;

use jazz::db::{
    Db, DbConfig, DbIdentity, ExclusiveTxOps, ReadOpts, RemoteLinkHint, SerializedReadResult,
    WireTransportAdapter, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, ArraySubqueryRequirement, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::serving::{InMemoryServerShell, InMemoryServerShellConfig, NodeRole, ServerSession};
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, TxId};
use jazz::wire::{TransportError, WireTransport};

use common::compile_schema;

const MAX_TURNS: usize = 200;

fn schema() -> JazzSchema {
    use jazz::tools::test_support::AllowAll;
    compile_schema(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("invites").column("code", ColumnType::Text))
            .table(TableSchemaBuilder::new("members").column("code", ColumnType::Text))
            .table(TableSchemaBuilder::new("audit").column("note", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("grants")
                    .column("code", ColumnType::Text)
                    .fk_column("invite", "invites"),
            )
            .allow_all()
            .build(),
    )
}

fn identity(byte: u8, author: AuthorSubject) -> DbIdentity {
    DbIdentity {
        node: NodeUuid::from_bytes([byte; 16]),
        author,
    }
}

#[derive(Clone, Default)]
struct QueuedWireTransport {
    queues: Rc<RefCell<(VecDeque<Vec<u8>>, VecDeque<Vec<u8>>)>>,
}

impl WireTransport for QueuedWireTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.queues.borrow_mut().1.push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.borrow_mut().0.pop_front()
    }
}

struct Client {
    db: Db,
    wire: QueuedWireTransport,
    session: ServerSession,
}

/// One Core and its connected clients, pumped in lockstep.
struct Net {
    core: RefCell<InMemoryServerShell>,
    clients: Vec<Client>,
}

impl Net {
    fn new(clients: &[u8]) -> Self {
        let schema = schema();
        let mut core = InMemoryServerShell::start(
            InMemoryServerShellConfig::new(schema.clone(), identity(0xc0, AuthorSubject::SYSTEM))
                .with_role(NodeRole::Core),
        )
        .unwrap();
        let clients = clients
            .iter()
            .map(|&byte| {
                let author = AuthorSubject::for_test_bytes([byte; 16]);
                let cfs = schema.column_families();
                let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
                let db = block_on(Db::open(DbConfig::new(
                    schema.clone(),
                    TestStorage::new(&refs),
                    identity(byte, author),
                )))
                .unwrap();
                let wire = QueuedWireTransport::default();
                block_on(
                    db.connect_upstream(Box::new(WireTransportAdapter::current(wire.clone()))),
                );
                let session = core.accept_subscriber_session(author).unwrap();
                Client { db, wire, session }
            })
            .collect();
        Self {
            core: RefCell::new(core),
            clients,
        }
    }

    fn db(&self, index: usize) -> &Db {
        &self.clients[index].db
    }

    fn pump(&self) {
        let mut core = self.core.borrow_mut();
        for client in &self.clients {
            block_on(client.db.tick()).unwrap();
            let outbound = client
                .wire
                .queues
                .borrow_mut()
                .1
                .drain(..)
                .collect::<Vec<_>>();
            core.receive_frames(client.session, outbound).unwrap();
        }
        core.tick().unwrap();
        for client in &self.clients {
            for frame in core.take_frames(client.session).unwrap() {
                client.wire.queues.borrow_mut().0.push_back(frame.into());
            }
            block_on(client.db.tick()).unwrap();
        }
    }

    /// Poll `future` the way bindings re-poll a pending native operation,
    /// pumping every endpoint in between.
    fn drive<T>(&self, future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..MAX_TURNS {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            self.pump();
        }
        panic!("operation did not complete within {MAX_TURNS} turns");
    }

    /// One-shot read through the binding entry point, with the binding's own
    /// coverage choice: only an explicit Global read asks for coverage.
    fn read(
        &self,
        client: usize,
        query: &Query,
        tier: DurabilityTier,
        open_tx: Option<OpenTransactionId>,
    ) -> Vec<RowUuid> {
        let db = self.db(client);
        let bytes = postcard::to_allocvec(query).unwrap();
        let read = db.all_serialized_query(
            &bytes,
            ReadOpts {
                tier,
                ..ReadOpts::default()
            },
            open_tx,
            None,
            None,
            tier >= DurabilityTier::Global,
            || false,
            |attachment| db.detach_query(attachment),
        );
        root_rows(query, self.drive(read).expect("read"))
    }

    /// Poll a one-shot local exclusive read without pumping anything,
    /// reporting `hint` once the read has had its first poll.
    fn read_while(
        &self,
        client: usize,
        query: &Query,
        open_tx: OpenTransactionId,
        hint: RemoteLinkHint,
    ) -> Option<Vec<RowUuid>> {
        let db = self.db(client);
        let bytes = postcard::to_allocvec(query).unwrap();
        let mut read = pin!(db.all_serialized_query(
            &bytes,
            ReadOpts::default(),
            Some(open_tx),
            None,
            None,
            false,
            || false,
            |attachment| db.detach_query(attachment),
        ));
        let mut context = Context::from_waker(Waker::noop());
        let mut polled = read.as_mut().poll(&mut context);
        if polled.is_pending() {
            db.set_remote_link_hint(hint);
            polled = read.as_mut().poll(&mut context);
        }
        match polled {
            Poll::Ready(Ok(result)) => Some(root_rows(query, result)),
            Poll::Ready(Err(error)) => panic!("read failed: {error:?}"),
            Poll::Pending => None,
        }
    }

    fn settle(&self, client: usize, tx_id: TxId) -> Result<TxId, jazz::db::Error> {
        self.drive(
            self.db(client)
                .wait_for_transaction(tx_id, DurabilityTier::Global),
        )
    }

    fn create_invite(&self, client: usize, code: &str) -> RowUuid {
        let write = block_on(
            self.db(client)
                .insert("invites", cells(code), Default::default()),
        )
        .unwrap();
        self.settle(client, write.mergeable_tx_id())
            .expect("invite settles");
        write.row_uuid()
    }

    fn create_grant(&self, client: usize, code: &str, invite: RowUuid) -> RowUuid {
        let mut grant = cells(code);
        grant.insert("invite".to_owned(), Value::Uuid(invite.0));
        let write = block_on(self.db(client).insert("grants", grant, Default::default())).unwrap();
        self.settle(client, write.mergeable_tx_id())
            .expect("grant settles");
        write.row_uuid()
    }

    fn revoke(&self, client: usize, invite: RowUuid) {
        let write = block_on(
            self.db(client)
                .delete("invites", invite, Default::default()),
        )
        .unwrap();
        self.settle(client, write.mergeable_tx_id())
            .expect("revocation settles");
    }

    /// Advance `client`'s known authority cut with a receipt for a table no
    /// test reads or writes, delivering nothing about invites or members.
    fn unrelated_receipt(&self, client: usize) {
        self.read(client, &Query::from("audit"), DurabilityTier::Global, None);
    }

    fn members(&self) -> usize {
        self.read(OWNER, &Query::from("members"), DurabilityTier::Global, None)
            .len()
    }

    /// The invite-link recipe at the default (local) read tier: check the
    /// invite inside an exclusive transaction, add a membership only if it
    /// exists, and report whether the server accepted the commit.
    fn redeem(&self, client: usize, code: &str) -> Redeem {
        self.redeem_via(client, code, InviteCheck::Binding)
    }

    /// Whether `code` names a live invite, read inside `open` the way
    /// `check` reads it.
    fn invite_is_live(
        &self,
        client: usize,
        code: &str,
        open: OpenTransactionId,
        check: InviteCheck,
    ) -> bool {
        let db = self.db(client);
        match check {
            InviteCheck::Binding => !self
                .read(
                    client,
                    &invite_query(code),
                    DurabilityTier::Local,
                    Some(open),
                )
                .is_empty(),
            InviteCheck::BindingCount => {
                let bytes = postcard::to_allocvec(&invite_query(code).count()).unwrap();
                let read = db.all_serialized_query(
                    &bytes,
                    ReadOpts::default(),
                    Some(open),
                    None,
                    None,
                    false,
                    || false,
                    |attachment| db.detach_query(attachment),
                );
                let SerializedReadResult::Rows(rows) = self.drive(read).expect("count") else {
                    panic!("expected plain rows");
                };
                rows[0].cell_at(0) != Some(Value::U64(0))
            }
            InviteCheck::RustApi => {
                let prepared = db.prepare_query(&invite_query(code)).unwrap();
                !self
                    .drive(db.exclusive_tx_ref(open).all_prepared(&prepared))
                    .expect("read")
                    .is_empty()
            }
            InviteCheck::RustWholeTable => self
                .drive(db.exclusive_tx_ref(open).all("invites"))
                .expect("read")
                .iter()
                .any(|row| {
                    !row.is_deleted() && row.cell_at(0) == Some(Value::String(code.to_owned()))
                }),
        }
    }

    /// [`Self::redeem`] with the invite read the way `check` reads it.
    fn redeem_via(&self, client: usize, code: &str, check: InviteCheck) -> Redeem {
        let db = self.db(client);
        self.unrelated_receipt(client);
        let open = OpenTransactionId::new();
        block_on(db.begin_exclusive(open)).unwrap();
        if !self.invite_is_live(client, code, open, check) {
            db.abandon_transaction_handle(open).ok();
            return Redeem::Invalid;
        }
        self.drive(
            db.exclusive_tx_ref(open)
                .insert("members", cells(code), Default::default()),
        )
        .unwrap();
        let tx_id = self.drive(db.commit_exclusive_handle(open)).unwrap();
        match self.settle(client, tx_id) {
            Ok(_) => Redeem::Joined,
            Err(_) => Redeem::Conflict,
        }
    }
}

/// How the invite-link recipe reads the invite inside its transaction.
#[derive(Clone, Copy, Debug)]
enum InviteCheck {
    /// A filtered query through the binding entry point.
    Binding,
    /// A filtered count through the binding entry point.
    BindingCount,
    /// A filtered query through the Rust transaction API.
    RustApi,
    /// A whole-table read through the Rust transaction API.
    RustWholeTable,
}

#[derive(Debug, PartialEq, Eq)]
enum Redeem {
    Invalid,
    Joined,
    Conflict,
}

/// The rows a read returned from its root table.
fn root_rows(query: &Query, result: SerializedReadResult) -> Vec<RowUuid> {
    match result {
        SerializedReadResult::Rows(rows) => rows.iter().map(|row| row.row_uuid()).collect(),
        SerializedReadResult::Relation(snapshot) => snapshot
            .rows
            .iter()
            .filter(|row| row.table() == query.table)
            .map(|row| row.row_uuid())
            .collect(),
    }
}

fn cells(code: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("code".to_owned(), Value::String(code.to_owned()))])
}

fn invite_query(code: &str) -> Query {
    Query::from("invites").filter(eq(col("code"), lit(code)))
}

const OWNER: usize = 0;
const BACKEND: usize = 1;

/// A backend that never received an invite still sees it: the exclusive read
/// hydrates at its snapshot even though the cut already covers the invite.
#[test]
fn exclusive_read_sees_an_invite_the_backend_never_received() {
    let net = Net::new(&[0x0a, 0x0b]);
    let invite = net.create_invite(OWNER, "abc");
    net.unrelated_receipt(BACKEND);

    let open = OpenTransactionId::new();
    block_on(net.db(BACKEND).begin_exclusive(open)).unwrap();
    assert_eq!(
        net.read(
            BACKEND,
            &invite_query("abc"),
            DurabilityTier::Local,
            Some(open)
        ),
        vec![invite]
    );
}

/// garden-co/jazz#3694: an invite revoked before the backend's cut must never
/// add a member, on any attempt.
///
/// The backend still holds the invite: a snapshot receipt does not yet
/// revalidate extra local inputs, so the read sees the stale row and the
/// authority rejects it by its row proof. Reading it as absent needs that
/// revalidation (#3696).
#[test]
fn revoked_invite_never_adds_a_member() {
    let net = Net::new(&[0x0a, 0x0b]);
    let invite = net.create_invite(OWNER, "abc");
    assert_eq!(net.redeem(BACKEND, "abc"), Redeem::Joined);

    net.revoke(OWNER, invite);
    for _ in 0..3 {
        assert_ne!(net.redeem(BACKEND, "abc"), Redeem::Joined);
    }
    assert_eq!(
        net.members(),
        1,
        "only the redemption before the revocation added a member"
    );
}

/// The revocation lands between the exclusive read and the commit: the read
/// saw a live invite, so the authority must reject the membership.
#[test]
fn revocation_after_the_read_rejects_the_redemption() {
    let net = Net::new(&[0x0a, 0x0b]);
    let invite = net.create_invite(OWNER, "abc");
    net.unrelated_receipt(BACKEND);
    let db = net.db(BACKEND);
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    assert_eq!(
        net.read(
            BACKEND,
            &invite_query("abc"),
            DurabilityTier::Local,
            Some(open)
        ),
        vec![invite]
    );

    net.revoke(OWNER, invite);

    net.drive(
        db.exclusive_tx_ref(open)
            .insert("members", cells("abc"), Default::default()),
    )
    .unwrap();
    let tx_id = net.drive(db.commit_exclusive_handle(open)).unwrap();
    assert!(net.settle(BACKEND, tx_id).is_err());
    assert!(
        net.read(OWNER, &Query::from("members"), DurabilityTier::Global, None)
            .is_empty()
    );
}

/// A single-use invite redeemed on one backend cannot be redeemed again on a
/// second backend whose cut already covers the first redemption.
#[test]
fn a_redemption_on_another_backend_is_seen_as_taken() {
    let net = Net::new(&[0x0a, 0x0b, 0x0c]);
    net.create_invite(OWNER, "abc");
    // The first backend already holds the invite, so its redemption does not
    // depend on hydration.
    net.read(BACKEND, &invite_query("abc"), DurabilityTier::Global, None);
    assert_eq!(net.redeem(BACKEND, "abc"), Redeem::Joined);
    net.unrelated_receipt(2);

    // "Only if nobody has redeemed it": the second backend must see the
    // first redemption, which it never received.
    let open = OpenTransactionId::new();
    block_on(net.db(2).begin_exclusive(open)).unwrap();
    let taken = Query::from("members").filter(eq(col("code"), lit("abc")));
    assert_eq!(
        net.read(2, &taken, DurabilityTier::Local, Some(open)).len(),
        1
    );
}

/// Offline, an exclusive read answers from the replica instead of waiting for
/// an authority that cannot answer. The transaction commits locally and the
/// authority accepts it once it syncs, because every row it read still holds.
#[test]
fn offline_prepared_exclusive_commit_succeeds_while_its_reads_hold() {
    let net = Net::new(&[0x0a, 0x0b]);
    let invite = net.create_invite(OWNER, "abc");
    assert_eq!(
        net.read(BACKEND, &invite_query("abc"), DurabilityTier::Global, None),
        vec![invite]
    );
    let db = net.db(BACKEND);
    db.set_remote_link_hint(RemoteLinkHint::NoServer);
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    assert_eq!(
        net.read_while(
            BACKEND,
            &invite_query("abc"),
            open,
            RemoteLinkHint::NoServer
        ),
        Some(vec![invite])
    );
    net.drive(
        db.exclusive_tx_ref(open)
            .insert("members", cells("abc"), Default::default()),
    )
    .unwrap();
    let tx_id = net.drive(db.commit_exclusive_handle(open)).unwrap();
    db.set_remote_link_hint(RemoteLinkHint::Live);
    net.settle(BACKEND, tx_id)
        .expect("the offline read still holds, so the authority accepts it");
    assert_eq!(net.members(), 1);
}

/// An invite revoked while the backend was offline: its offline-prepared
/// redemption read the stale invite, so the authority rejects it.
#[test]
fn offline_prepared_redemption_of_a_revoked_invite_conflicts() {
    let net = Net::new(&[0x0a, 0x0b]);
    let invite = net.create_invite(OWNER, "abc");
    net.read(BACKEND, &invite_query("abc"), DurabilityTier::Global, None);
    let db = net.db(BACKEND);
    db.set_remote_link_hint(RemoteLinkHint::NoServer);
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    assert_eq!(
        net.read_while(
            BACKEND,
            &invite_query("abc"),
            open,
            RemoteLinkHint::NoServer
        ),
        Some(vec![invite])
    );
    net.drive(
        db.exclusive_tx_ref(open)
            .insert("members", cells("abc"), Default::default()),
    )
    .unwrap();
    let tx_id = net.drive(db.commit_exclusive_handle(open)).unwrap();

    net.revoke(OWNER, invite);
    db.set_remote_link_hint(RemoteLinkHint::Live);
    assert!(net.settle(BACKEND, tx_id).is_err());
    assert_eq!(net.members(), 0);
}

/// The double-redeem half of #3694 offline: a backend whose cut covers
/// another backend's redemption, but which never received it, reads "nobody
/// redeemed it" from its replica. That absence guard proves no row, so the
/// redemption the authority returns for it now is a phantom and the commit
/// is rejected.
#[test]
fn offline_absence_guard_conflicts_when_the_invite_was_taken() {
    let net = Net::new(&[0x0a, 0x0b, 0x0c]);
    net.create_invite(OWNER, "abc");
    net.read(BACKEND, &invite_query("abc"), DurabilityTier::Global, None);
    assert_eq!(net.redeem(BACKEND, "abc"), Redeem::Joined);
    // The cut now covers the redemption; the members row never arrived.
    net.read(2, &invite_query("abc"), DurabilityTier::Global, None);

    let db = net.db(2);
    db.set_remote_link_hint(RemoteLinkHint::NoServer);
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    let taken = Query::from("members").filter(eq(col("code"), lit("abc")));
    assert_eq!(
        net.read_while(2, &taken, open, RemoteLinkHint::NoServer),
        Some(vec![])
    );
    net.drive(
        db.exclusive_tx_ref(open)
            .insert("members", cells("abc"), Default::default()),
    )
    .unwrap();
    let tx_id = net.drive(db.commit_exclusive_handle(open)).unwrap();
    db.set_remote_link_hint(RemoteLinkHint::Live);
    assert!(net.settle(2, tx_id).is_err());
    assert_eq!(net.members(), 1, "the invite was redeemed only once");
}

/// An exclusive read waiting on its snapshot hydration stops waiting when
/// the authority becomes unreachable and answers from the replica; the
/// transaction still commits because its read holds.
#[test]
fn exclusive_read_falls_back_to_the_replica_when_the_link_is_lost() {
    let net = Net::new(&[0x0a, 0x0b]);
    let invite = net.create_invite(OWNER, "abc");
    assert_eq!(
        net.read(BACKEND, &invite_query("abc"), DurabilityTier::Global, None),
        vec![invite]
    );
    let db = net.db(BACKEND);
    db.set_remote_link_hint(RemoteLinkHint::Live);
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    // While the link stays live, the read waits for the authority.
    assert_eq!(
        net.read_while(BACKEND, &invite_query("abc"), open, RemoteLinkHint::Live),
        None
    );
    assert_eq!(
        net.read_while(BACKEND, &invite_query("abc"), open, RemoteLinkHint::Failed),
        Some(vec![invite])
    );
    net.drive(
        db.exclusive_tx_ref(open)
            .insert("members", cells("abc"), Default::default()),
    )
    .unwrap();
    db.set_remote_link_hint(RemoteLinkHint::Live);
    let tx_id = net.drive(db.commit_exclusive_handle(open)).unwrap();
    net.settle(BACKEND, tx_id).expect("the read still holds");
    assert_eq!(net.members(), 1);
}

/// A transaction whose snapshot claims no authority state reads its replica
/// offline and still commits.
#[test]
fn offline_exclusive_commit_from_a_genesis_snapshot_is_allowed() {
    let net = Net::new(&[0x0a]);
    let db = net.db(OWNER);
    db.set_remote_link_hint(RemoteLinkHint::NoServer);
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    assert_eq!(
        net.read_while(OWNER, &invite_query("abc"), open, RemoteLinkHint::NoServer),
        Some(vec![])
    );
    net.drive(
        db.exclusive_tx_ref(open)
            .insert("invites", cells("abc"), Default::default()),
    )
    .unwrap();
    net.drive(db.commit_exclusive_handle(open))
        .expect("an offline commit is prepared locally");
}

/// garden-co/jazz#3696: every way of reading the invite inside an exclusive
/// transaction must refuse a revoked invite the backend still holds, not only
/// a filtered row query through the binding entry point.
fn assert_revoked_invite_never_adds_a_member(check: InviteCheck) {
    let net = Net::new(&[0x0a, 0x0b]);
    let invite = net.create_invite(OWNER, "abc");
    // The backend holds the invite before any redemption, whichever way it
    // reads it.
    net.read(BACKEND, &invite_query("abc"), DurabilityTier::Global, None);
    assert_eq!(net.redeem_via(BACKEND, "abc", check), Redeem::Joined);

    net.revoke(OWNER, invite);
    for _ in 0..3 {
        assert_ne!(net.redeem_via(BACKEND, "abc", check), Redeem::Joined);
    }
    assert_eq!(
        net.members(),
        1,
        "only the redemption before the revocation added a member"
    );
}

#[test]
fn revoked_invite_never_adds_a_member_when_counted() {
    assert_revoked_invite_never_adds_a_member(InviteCheck::BindingCount);
}

#[test]
fn revoked_invite_never_adds_a_member_through_the_rust_api() {
    assert_revoked_invite_never_adds_a_member(InviteCheck::RustApi);
}

#[test]
fn revoked_invite_never_adds_a_member_through_a_whole_table_read() {
    assert_revoked_invite_never_adds_a_member(InviteCheck::RustWholeTable);
}

/// The backend still holds the invite, but the owner marked it used (it no
/// longer matches the filter) before the backend's cut.
#[test]
fn invite_used_up_before_the_cut_never_adds_a_member() {
    let net = Net::new(&[0x0a, 0x0b]);
    let invite = net.create_invite(OWNER, "abc");
    assert_eq!(net.redeem(BACKEND, "abc"), Redeem::Joined);

    let write =
        block_on(
            net.db(OWNER)
                .update("invites", invite, cells("used"), Default::default()),
        )
        .unwrap();
    net.settle(OWNER, write.mergeable_tx_id())
        .expect("update settles");
    for _ in 0..3 {
        assert_ne!(net.redeem(BACKEND, "abc"), Redeem::Joined);
    }
    assert_eq!(net.members(), 1);
}

/// The absence guard of a single-use invite read through the Rust API: a
/// second backend that never received the first redemption must not redeem
/// it again, whether it reads the filtered query or the whole table.
fn assert_second_backend_cannot_redeem_through_the_rust_api(whole_table: bool) {
    let net = Net::new(&[0x0a, 0x0b, 0x0c]);
    net.create_invite(OWNER, "abc");
    net.read(BACKEND, &invite_query("abc"), DurabilityTier::Global, None);
    assert_eq!(net.redeem(BACKEND, "abc"), Redeem::Joined);
    net.unrelated_receipt(2);

    let db = net.db(2);
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    let taken = if whole_table {
        net.drive(db.exclusive_tx_ref(open).all("members"))
            .unwrap()
            .into_iter()
            .any(|row| !row.is_deleted() && row.cell_at(0) == Some(Value::String("abc".to_owned())))
    } else {
        let prepared = db
            .prepare_query(&Query::from("members").filter(eq(col("code"), lit("abc"))))
            .unwrap();
        !net.drive(db.exclusive_tx_ref(open).all_prepared(&prepared))
            .unwrap()
            .is_empty()
    };
    if !taken {
        net.drive(
            db.exclusive_tx_ref(open)
                .insert("members", cells("abc"), Default::default()),
        )
        .unwrap();
        if let Ok(tx_id) = net.drive(db.commit_exclusive_handle(open)) {
            let _ = net.settle(2, tx_id);
        }
    }
    assert_eq!(net.members(), 1, "the invite was redeemed only once");
}

#[test]
fn a_second_backend_cannot_redeem_through_the_rust_api() {
    assert_second_backend_cannot_redeem_through_the_rust_api(false);
}

#[test]
fn a_second_backend_cannot_redeem_through_a_whole_table_read() {
    assert_second_backend_cannot_redeem_through_the_rust_api(true);
}

/// How a redemption checks that nobody else redeemed the invite, in the same
/// query that reads the invite.
#[derive(Clone, Copy, Debug)]
enum RedemptionCheck {
    /// Invites joined to their redemptions.
    Join,
    /// Invites with at least one redemption, as a correlated relation.
    Relation,
}

fn redeemed_invite_query(code: &str, check: RedemptionCheck) -> Query {
    match check {
        RedemptionCheck::Join => invite_query(code).join_via_column("members", "code", "code", []),
        RedemptionCheck::Relation => invite_query(code).array_subquery(ArraySubquery {
            requirement: ArraySubqueryRequirement::AtLeastOne,
            ..ArraySubquery::new("redemptions", "members", "code", "code")
        }),
    }
}

/// Redeem `code` on `client` inside one exclusive transaction: read the
/// invite on its own, then read it through its redemptions, and add a member
/// only if the invite is live and has none. `between` runs after the reads
/// and before the commit.
fn redeem_unless_redeemed(
    net: &Net,
    client: usize,
    code: &str,
    check: RedemptionCheck,
    offline: bool,
    between: impl FnOnce(),
) -> Redeem {
    let db = net.db(client);
    let hint = if offline {
        RemoteLinkHint::NoServer
    } else {
        RemoteLinkHint::Live
    };
    db.set_remote_link_hint(hint);
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    let read = |query: &Query| {
        if offline {
            net.read_while(client, query, open, hint)
                .expect("offline read")
        } else {
            net.read(client, query, DurabilityTier::Local, Some(open))
        }
    };
    let live = !read(&invite_query(code)).is_empty();
    let redeemed = !read(&redeemed_invite_query(code, check)).is_empty();
    if !live || redeemed {
        db.abandon_transaction_handle(open).ok();
        return Redeem::Invalid;
    }
    between();
    net.drive(
        db.exclusive_tx_ref(open)
            .insert("members", cells(code), Default::default()),
    )
    .unwrap();
    let tx_id = net.drive(db.commit_exclusive_handle(open)).unwrap();
    db.set_remote_link_hint(RemoteLinkHint::Live);
    match net.settle(client, tx_id) {
        Ok(_) => Redeem::Joined,
        Err(_) => Redeem::Conflict,
    }
}

/// A plain read proves the invite row, and a second read of the same invite
/// through its redemptions finds none on a backend that never received the
/// first redemption. The row proof from the plain read must not stand in for
/// the redemptions the second read depended on.
fn assert_offline_redemption_check_sees_the_first_redemption(check: RedemptionCheck) {
    let net = Net::new(&[0x0a, 0x0b, 0x0c]);
    net.create_invite(OWNER, "abc");
    net.read(BACKEND, &invite_query("abc"), DurabilityTier::Global, None);
    assert_eq!(
        redeem_unless_redeemed(&net, BACKEND, "abc", check, false, || {}),
        Redeem::Joined
    );
    // The second backend holds the invite, and its cut covers the first
    // redemption, which it never received.
    net.read(2, &invite_query("abc"), DurabilityTier::Global, None);

    assert_eq!(
        redeem_unless_redeemed(&net, 2, "abc", check, true, || {}),
        Redeem::Conflict
    );
    assert_eq!(net.members(), 1, "the invite was redeemed only once");
}

#[test]
fn offline_join_redemption_check_sees_the_first_redemption() {
    assert_offline_redemption_check_sees_the_first_redemption(RedemptionCheck::Join);
}

#[test]
fn offline_relation_redemption_check_sees_the_first_redemption() {
    assert_offline_redemption_check_sees_the_first_redemption(RedemptionCheck::Relation);
}

/// Online, a redemption that lands after the join read and before the commit
/// conflicts the second redemption.
#[test]
fn join_redemption_check_conflicts_with_a_concurrent_redemption() {
    let net = Net::new(&[0x0a, 0x0b, 0x0c]);
    net.create_invite(OWNER, "abc");
    net.read(BACKEND, &invite_query("abc"), DurabilityTier::Global, None);
    net.read(2, &invite_query("abc"), DurabilityTier::Global, None);

    let outcome = redeem_unless_redeemed(&net, 2, "abc", RedemptionCheck::Join, false, || {
        assert_eq!(
            redeem_unless_redeemed(&net, BACKEND, "abc", RedemptionCheck::Join, false, || {}),
            Redeem::Joined
        );
    });
    assert_eq!(outcome, Redeem::Conflict);
    assert_eq!(net.members(), 1, "the invite was redeemed only once");
}

/// A redemption check through the invite's redemptions must not conflict on
/// redemptions of other invites the backend never received: nobody redeemed
/// this invite, so the redemption commits.
fn assert_redemption_check_commits_despite_unrelated_redemptions(check: RedemptionCheck) {
    let net = Net::new(&[0x0a, 0x0b, 0x0c]);
    net.create_invite(OWNER, "abc");
    net.create_invite(OWNER, "xyz");
    net.read(BACKEND, &invite_query("xyz"), DurabilityTier::Global, None);
    assert_eq!(
        redeem_unless_redeemed(&net, BACKEND, "xyz", check, false, || {}),
        Redeem::Joined
    );
    net.read(2, &invite_query("abc"), DurabilityTier::Global, None);

    assert_eq!(
        redeem_unless_redeemed(&net, 2, "abc", check, false, || {}),
        Redeem::Joined
    );
    assert_eq!(net.members(), 2);
}

#[test]
fn join_commits_despite_unrelated_joined_rows() {
    assert_redemption_check_commits_despite_unrelated_redemptions(RedemptionCheck::Join);
}

#[test]
fn relation_commits_despite_unrelated_related_rows() {
    assert_redemption_check_commits_despite_unrelated_redemptions(RedemptionCheck::Relation);
}

/// The same check with every read inside the transaction asking for the
/// Global tier.
#[test]
fn global_tier_join_commits_despite_unrelated_joined_rows() {
    let net = Net::new(&[0x0a, 0x0b, 0x0c]);
    net.create_invite(OWNER, "abc");
    net.create_invite(OWNER, "xyz");
    net.read(BACKEND, &invite_query("xyz"), DurabilityTier::Global, None);
    assert_eq!(net.redeem(BACKEND, "xyz"), Redeem::Joined);
    net.read(2, &invite_query("abc"), DurabilityTier::Global, None);

    let db = net.db(2);
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    let joined = redeemed_invite_query("abc", RedemptionCheck::Join);
    assert_eq!(
        net.read(2, &invite_query("abc"), DurabilityTier::Global, Some(open))
            .len(),
        1
    );
    assert!(
        net.read(2, &joined, DurabilityTier::Global, Some(open))
            .is_empty()
    );
    net.drive(
        db.exclusive_tx_ref(open)
            .insert("members", cells("abc"), Default::default()),
    )
    .unwrap();
    let tx_id = net.drive(db.commit_exclusive_handle(open)).unwrap();
    net.settle(2, tx_id)
        .expect("nobody redeemed this invite, so the redemption commits");
    assert_eq!(net.members(), 2);
}

/// garden-co/jazz#3694: a read through a join or a correlated relation records
/// only the joined rows it could have consulted, so a redemption of another
/// invite that lands between the read and the commit does not conflict it.
fn assert_redemption_check_commits_despite_a_concurrent_unrelated_redemption(
    check: RedemptionCheck,
) {
    let net = Net::new(&[0x0a, 0x0b, 0x0c]);
    net.create_invite(OWNER, "abc");
    net.create_invite(OWNER, "xyz");
    net.read(BACKEND, &invite_query("xyz"), DurabilityTier::Global, None);
    net.read(2, &invite_query("abc"), DurabilityTier::Global, None);

    let outcome = redeem_unless_redeemed(&net, 2, "abc", check, false, || {
        assert_eq!(
            redeem_unless_redeemed(&net, BACKEND, "xyz", check, false, || {}),
            Redeem::Joined
        );
    });
    assert_eq!(outcome, Redeem::Joined);
    assert_eq!(net.members(), 2);
}

#[test]
fn join_commits_despite_a_concurrent_unrelated_redemption() {
    assert_redemption_check_commits_despite_a_concurrent_unrelated_redemption(
        RedemptionCheck::Join,
    );
}

#[test]
fn relation_commits_despite_a_concurrent_unrelated_redemption() {
    assert_redemption_check_commits_despite_a_concurrent_unrelated_redemption(
        RedemptionCheck::Relation,
    );
}

/// The narrowed read still covers a correlated row that was absent when the
/// relation read ran: a concurrent redemption of the same invite conflicts.
#[test]
fn relation_redemption_check_conflicts_with_a_concurrent_redemption() {
    let net = Net::new(&[0x0a, 0x0b, 0x0c]);
    net.create_invite(OWNER, "abc");
    net.read(BACKEND, &invite_query("abc"), DurabilityTier::Global, None);
    net.read(2, &invite_query("abc"), DurabilityTier::Global, None);

    let outcome = redeem_unless_redeemed(&net, 2, "abc", RedemptionCheck::Relation, false, || {
        assert_eq!(
            redeem_unless_redeemed(
                &net,
                BACKEND,
                "abc",
                RedemptionCheck::Relation,
                false,
                || {}
            ),
            Redeem::Joined
        );
    });
    assert_eq!(outcome, Redeem::Conflict);
    assert_eq!(net.members(), 1, "the invite was redeemed only once");
}

/// The redeeming client receives the unrelated redemption before it commits.
/// Its local serializability check compares the narrowed read's output rather
/// than rejecting on any newer row in the joined table.
#[test]
fn join_commits_after_receiving_an_unrelated_redemption() {
    let net = Net::new(&[0x0a, 0x0b, 0x0c]);
    net.create_invite(OWNER, "abc");
    net.create_invite(OWNER, "xyz");
    net.read(BACKEND, &invite_query("xyz"), DurabilityTier::Global, None);
    net.read(2, &invite_query("abc"), DurabilityTier::Global, None);

    let outcome = redeem_unless_redeemed(&net, 2, "abc", RedemptionCheck::Join, false, || {
        assert_eq!(
            redeem_unless_redeemed(&net, BACKEND, "xyz", RedemptionCheck::Join, false, || {}),
            Redeem::Joined
        );
        assert_eq!(
            net.read(2, &Query::from("members"), DurabilityTier::Global, None)
                .len(),
            1,
            "the redeeming client holds the unrelated redemption"
        );
    });
    assert_eq!(outcome, Redeem::Joined);
    assert_eq!(net.members(), 2);
}

fn grant_query(code: &str) -> Query {
    Query::from("grants")
        .filter(eq(col("code"), lit(code)))
        .include("invite")
}

/// Redeem the grant `g` after reading it with the invite it includes, while
/// either that invite or another one is revoked between the read and the
/// commit.
fn redeem_grant_while_revoking(revoke_included: bool) -> Redeem {
    let net = Net::new(&[0x0a, 0x0b]);
    let abc = net.create_invite(OWNER, "abc");
    let xyz = net.create_invite(OWNER, "xyz");
    net.create_grant(OWNER, "g", abc);
    net.unrelated_receipt(BACKEND);

    let db = net.db(BACKEND);
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    assert_eq!(
        net.read(
            BACKEND,
            &grant_query("g"),
            DurabilityTier::Local,
            Some(open)
        )
        .len(),
        1
    );
    net.revoke(OWNER, if revoke_included { abc } else { xyz });
    net.drive(
        db.exclusive_tx_ref(open)
            .insert("members", cells("g"), Default::default()),
    )
    .unwrap();
    let tx_id = net.drive(db.commit_exclusive_handle(open)).unwrap();
    match net.settle(BACKEND, tx_id) {
        Ok(_) => Redeem::Joined,
        Err(_) => Redeem::Conflict,
    }
}

/// An include records only the rows its reference reaches: revoking another
/// invite does not conflict the redemption.
#[test]
fn include_commits_despite_an_unrelated_revocation() {
    assert_eq!(redeem_grant_while_revoking(false), Redeem::Joined);
}

/// Revoking the included invite still conflicts.
#[test]
fn include_conflicts_when_the_included_row_is_revoked() {
    assert_eq!(redeem_grant_while_revoking(true), Redeem::Conflict);
}
