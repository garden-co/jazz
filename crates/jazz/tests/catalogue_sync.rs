//! Accepted UUIDs come from the published authority catalogue, never from
//! local schema names, query registration or an optimistic replica proposal.
mod common;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, RemoteLinkHint, SyncMessage, Transport, TransportError,
    block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol::{
    CatalogueSnapshot, CurrentWriteSchema, LensOp, MigrationLens, PhysicalIdentityManifest,
    SchemaLineagePublication, SchemaVersion, TableLens,
};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    future::Future,
    path::Path,
    pin::pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};
fn schema(table: &str, column: &str) -> JazzSchema {
    common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(table)
                    .column(column, ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    )
}
fn genesis(schema: JazzSchema) -> CatalogueSnapshot {
    CatalogueSnapshot {
        genesis_physical_identities: PhysicalIdentityManifest::allocate(&schema),
        schemas: vec![SchemaVersion::new(schema.clone())],
        lineages: vec![],
        current_write_schema: CurrentWriteSchema {
            revision: 1,
            schema: schema.version_id(),
        },
    }
}
fn open(node: u8, schema: JazzSchema, path: Option<&Path>) -> Db {
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap()
}
fn authority(node: u8, snapshot: &CatalogueSnapshot, path: Option<&Path>) -> Db {
    let db = open(node, snapshot.schemas[0].schema.clone(), path);
    db.set_authority();
    db.install_authority_catalogue(snapshot).unwrap();
    db
}
struct Capture {
    inner: Box<dyn Transport>,
    sent: Rc<RefCell<Vec<SyncMessage>>>,
}
impl Transport for Capture {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.sent.borrow_mut().push(message.clone());
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
}
fn connect(
    client: &Db,
    server: &Db,
    who: AuthorSubject,
    claims: Option<BTreeMap<String, Value>>,
) -> Rc<RefCell<Vec<SyncMessage>>> {
    let (a, b) = duplex();
    let sent = Rc::new(RefCell::new(vec![]));
    block_on(client.connect_upstream(a));
    server.accept_subscriber_with_claims(
        Box::new(Capture {
            inner: b,
            sent: sent.clone(),
        }),
        who,
        claims,
    );
    sent
}
fn turn(dbs: &[&Db]) {
    for db in dbs {
        block_on(db.tick()).unwrap();
    }
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(v) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return v;
        }
        turn(dbs);
    }
    panic!("read did not finish")
}
fn user(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", name).unwrap()
}
fn claims(value: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        jazz::query::provider_claim_key("access"),
        Value::String(value.into()),
    )])
}
fn rename(snapshot: &mut CatalogueSnapshot, table: &str, column: &str) -> MigrationLens {
    let source = snapshot.schemas.last().unwrap().schema.clone();
    let source_table = &source.tables()[0];
    let from = &source_table.name;
    let from_col = &source_table.columns[0].name;
    let target = schema(table, column);
    let lens = MigrationLens::new(
        source.version_id(),
        target.version_id(),
        vec![TableLens {
            source_table: from.clone(),
            target_table: table.into(),
            ops: if from_col != column {
                vec![LensOp::RenameColumn {
                    from: from_col.clone(),
                    to: column.into(),
                }]
            } else {
                vec![]
            },
        }],
    )
    .unwrap();
    let identities = snapshot
        .lineages
        .last()
        .map_or(&snapshot.genesis_physical_identities, |(_, p)| {
            &p.physical_identities
        });
    let history = std::iter::once(snapshot.genesis_physical_identities.clone()).chain(
        snapshot
            .lineages
            .iter()
            .map(|(_, p)| p.physical_identities.clone()),
    );
    let publication = SchemaLineagePublication::author_from_prior_with_history(
        &source,
        identities,
        history,
        SchemaVersion::new(target.clone()),
        lens.clone(),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    snapshot
        .lineages
        .push((snapshot.lineages.len() as u64 + 1, publication));
    snapshot.schemas.push(SchemaVersion::new(target.clone()));
    snapshot.current_write_schema = CurrentWriteSchema {
        revision: snapshot.current_write_schema.revision + 1,
        schema: target.version_id(),
    };
    lens
}

/// Alice learns IDs via ordinary sync metadata without downloading row bodies.
/// Offline lookup remains available; each fresh coverage request needs authority.
#[test]
fn cold_coverage_and_offline_lookup_have_distinct_contracts() {
    let snapshot = genesis(schema("notes", "text"));
    let server = authority(3, &snapshot, None);
    block_on(server.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String("private body".into()))]),
        Default::default(),
    ))
    .unwrap();
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    let sent = connect(&alice, &server, user("alice"), Some(claims("alice")));
    drive(
        &[&alice, &server],
        alice.cover_catalogue("notes", Some((user("alice"), claims("alice")))),
    )
    .unwrap();
    assert_eq!(
        alice.table_identity("notes").unwrap(),
        server.table_identity("notes").unwrap()
    );
    assert!(
        !sent
            .borrow()
            .iter()
            .any(|m| matches!(m, SyncMessage::Rows { .. }))
    );
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(
        alice.column_identity("notes", "text").unwrap(),
        server.column_identity("notes", "text").unwrap()
    );
    assert_eq!(
        block_on(alice.cover_catalogue("notes", None))
            .unwrap_err()
            .code,
        ErrorCode::NotObserved
    );
}

/// Bob's relay forwards Alice's fresh coverage to the authority, retaining exact
/// UUIDs. Its cached catalogue cannot satisfy fresh coverage after upstream loss.
#[test]
fn cold_relay_coverage_reaches_authority() {
    let snapshot = genesis(schema("notes", "text"));
    let server = authority(3, &snapshot, None);
    let relay = open(2, snapshot.schemas[0].schema.clone(), None);
    connect(&relay, &server, AuthorSubject::SYSTEM, None);
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    connect(&alice, &relay, user("alice"), Some(claims("alice")));
    drive(
        &[&alice, &relay, &server],
        alice.cover_catalogue("notes", Some((user("alice"), claims("alice")))),
    )
    .unwrap();
    assert_eq!(
        alice.table_identity("notes").unwrap(),
        server.table_identity("notes").unwrap()
    );
    relay.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(
        drive(&[&alice, &relay], alice.cover_catalogue("notes", None))
            .unwrap_err()
            .code,
        ErrorCode::NotObserved
    );
}

/// Mallory cannot replace her authenticated author/claims in a catalogue read;
/// an explicitly trusted backend can delegate Alice's exact request scope.
#[test]
fn catalogue_coverage_preserves_the_connection_scope() {
    for (connection, requested, allow) in [
        (user("alice"), claims("alice"), true),
        (user("alice"), claims("bob"), false),
        (user("mallory"), claims("alice"), false),
        (AuthorSubject::SYSTEM, claims("bob"), true),
    ] {
        let snapshot = genesis(schema("notes", "text"));
        let server = authority(3, &snapshot, None);
        let alice = open(1, snapshot.schemas[0].schema.clone(), None);
        connect(&alice, &server, connection, Some(claims("alice")));
        let result = drive(
            &[&alice, &server],
            alice.cover_catalogue("notes", Some((user("alice"), requested))),
        );
        if allow {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().code, ErrorCode::WriteRejected);
        }
    }
}

/// Alice's owner follows the current accepted descriptor across table/column
/// renames; her fixed old view keeps its original names and the same UUIDs.
#[test]
fn renames_and_fixed_views_preserve_published_identities() {
    let mut snapshot = genesis(schema("notes", "text"));
    let server = authority(3, &snapshot, None);
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    let old = block_on(alice.register_schema_view(snapshot.schemas[0].schema.clone())).unwrap();
    connect(&alice, &server, AuthorSubject::SYSTEM, None);
    drive(&[&alice, &server], alice.cover_catalogue("notes", None)).unwrap();
    let table = alice.table_identity("notes").unwrap();
    let column = alice.column_identity("notes", "text").unwrap();
    let lens = rename(&mut snapshot, "tasks", "body");
    block_on(
        server
            .activate_schema_with_lenses(snapshot.schemas.last().unwrap().schema.clone(), &[lens]),
    )
    .unwrap();
    server.install_authority_catalogue(&snapshot).unwrap();
    drive(&[&alice, &server], alice.cover_catalogue("notes", None)).unwrap();
    assert_eq!(alice.table_identity("tasks").unwrap(), table);
    assert_eq!(alice.column_identity("tasks", "body").unwrap(), column);
    assert!(alice.table_identity("notes").unwrap().is_none());
    assert_eq!(old.table_identity("notes").unwrap(), table);
    assert_eq!(old.column_identity("notes", "text").unwrap(), column);
    assert!(old.table_identity("tasks").unwrap().is_none());
}

/// After an offline restart Alice can resolve the same accepted UUIDs without
/// contacting Bob. Reinitialising the authority also keeps its durable genesis.
#[test]
fn accepted_catalogue_survives_replica_and_authority_restart() {
    let snapshot = genesis(schema("notes", "text"));
    let dir = tempfile::tempdir().unwrap();
    let server_path = dir.path().join("server.sqlite");
    let path = dir.path().join("alice.sqlite");
    let expected = snapshot.genesis_physical_identities.tables["notes"].id;
    {
        let server = authority(3, &snapshot, Some(&server_path));
        let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
        connect(&alice, &server, AuthorSubject::SYSTEM, None);
        drive(&[&alice, &server], alice.cover_catalogue("notes", None)).unwrap();
    }
    let server = open(3, snapshot.schemas[0].schema.clone(), Some(&server_path));
    server.set_authority();
    server.initialize_authority_catalogue().unwrap();
    assert_eq!(server.table_identity("notes").unwrap(), Some(expected));
    let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(alice.table_identity("notes").unwrap(), Some(expected));
}

/// Alice refuses malformed durable metadata rather than accepting plausible identities.
/// Direct SQL corruption is needed to exercise this storage codec boundary.
#[test]
fn corrupt_cached_catalogue_fails_closed() {
    for target in ["head", "table", "schema"] {
        let snapshot = genesis(schema("notes", "text"));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.sqlite");
        {
            let server = authority(3, &snapshot, None);
            let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
            connect(&alice, &server, AuthorSubject::SYSTEM, None);
            drive(&[&alice, &server], alice.cover_catalogue("notes", None)).unwrap();
        }
        let sql = rusqlite::Connection::open(&path).unwrap();
        let name = format!("_jazz_catalogue_{target}_v1");
        sql.execute(&format!("UPDATE {name} SET value=x'ff'"), [])
            .unwrap();
        drop(sql);
        let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
        if target == "schema" {
            let server = authority(3, &snapshot, None);
            connect(&alice, &server, AuthorSubject::SYSTEM, None);
            block_on(alice.tick()).unwrap();
            block_on(server.tick()).unwrap();
            assert_eq!(block_on(alice.tick()).unwrap_err().code, ErrorCode::Storage);
        } else {
            assert_eq!(
                alice.table_identity("notes").unwrap_err().code,
                ErrorCode::Storage
            );
        }
    }
}

/// Alice must not complete coverage from an answer whose metadata rolled back.
/// The synthetic upstream is needed to place a corrupt frame in the same tick.
#[test]
fn rolled_back_metadata_cannot_complete_a_coverage_request() {
    let snapshot = genesis(schema("notes", "text"));
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    let (a, mut peer) = duplex();
    block_on(alice.connect_upstream(a));
    let mut read = pin!(alice.cover_catalogue("notes", None));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    block_on(alice.tick()).unwrap();
    let mut id = None;
    while let Some(message) = peer.try_recv() {
        if let SyncMessage::CatalogueFetchV1 { id: request, .. } = message {
            id = Some(request);
        }
    }
    let head = jazz::db::CatalogueHeadV1 {
        root: [7; 32],
        through: 0,
        revision: 1,
        current: snapshot.schemas[0].id,
    };
    peer.send(SyncMessage::CatalogueMetadataV1 {
        head: head.clone(),
        schemas: vec![jazz::db::CatalogueSchemaV1 {
            schema: snapshot.schemas[0].clone(),
            identities: snapshot.genesis_physical_identities.clone(),
        }],
    })
    .unwrap();
    peer.send(SyncMessage::CatalogueCoveredV1 {
        id: id.unwrap(),
        head: head.clone(),
        opened: head.current,
    })
    .unwrap();
    peer.send(SyncMessage::CatalogueMetadataV1 {
        head: jazz::db::CatalogueHeadV1 {
            root: [0; 32],
            ..head
        },
        schemas: vec![],
    })
    .unwrap();
    assert_eq!(
        block_on(alice.tick()).unwrap_err().code,
        ErrorCode::Protocol
    );
    assert_eq!(
        alice.table_identity("notes").unwrap_err().code,
        ErrorCode::NotObserved
    );
    match read.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Err(error)) => assert_eq!(error.code, ErrorCode::NotObserved),
        _ => panic!("rolled-back catalogue answer remained usable"),
    }
}

/// Cancelling before send removes Alice's request; reconnecting cannot transfer
/// an in-flight request to a different authenticated upstream session.
#[test]
fn cancelled_and_reconnected_reads_do_not_inherit_coverage() {
    let alice = open(1, schema("notes", "text"), None);
    let (a, mut peer) = duplex();
    block_on(alice.connect_upstream(a));
    {
        let mut read = pin!(alice.cover_catalogue("notes", None));
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    block_on(alice.tick()).unwrap();
    while let Some(message) = peer.try_recv() {
        assert!(!matches!(message, SyncMessage::CatalogueFetchV1 { .. }));
    }
    let mut read = pin!(alice.cover_catalogue("notes", None));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let (a, _) = duplex();
    block_on(alice.connect_upstream(a));
    match read.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Err(error)) => assert_eq!(error.code, ErrorCode::NotObserved),
        _ => panic!("old request survived reconnect"),
    }
}

/// Bob's relay forwards Alice's host claims only when her connection omits
/// claims. An explicitly empty or different connection map stays authoritative.
/// A captured upstream request observes the scope without relying on row policy
/// results (catalogue coverage is deliberately a zero-row read).
#[test]
fn relay_catalogue_requests_preserve_absent_empty_and_distinct_claims() {
    for connection_claims in [None, Some(BTreeMap::new()), Some(claims("connection"))] {
        let relay = open(2, schema("notes", "text"), None);
        relay.set_identity_claims(user("alice"), claims("host"));
        let (upstream, mut authority) = duplex();
        block_on(relay.connect_upstream(upstream));
        let alice = open(1, schema("notes", "text"), None);
        connect(&alice, &relay, user("alice"), connection_claims.clone());
        let mut read = pin!(alice.cover_catalogue("notes", None));
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        turn(&[&alice, &relay]);
        let mut forwarded = Vec::new();
        while let Some(message) = authority.try_recv() {
            if let SyncMessage::CatalogueFetchV1 { scope, .. } = message {
                forwarded.push(scope.unwrap());
            }
        }
        assert_eq!(
            forwarded,
            vec![(
                user("alice").canonical().to_owned(),
                connection_claims.unwrap_or_else(|| claims("host"))
            )]
        );
    }
}

#[derive(Default)]
struct Scheduler {
    immediate: std::cell::Cell<usize>,
    delays: RefCell<Vec<u64>>,
}
impl jazz::db::TickScheduler for Scheduler {
    fn schedule_tick(&self, _: jazz::db::TickUrgency) {
        self.immediate.set(self.immediate.get() + 1);
    }
    fn schedule_tick_after(&self, delay_ms: u64) {
        self.delays.borrow_mut().push(delay_ms);
    }
}

/// Alice's pending coverage schedules both immediate work and its deadline.
/// Cancelling releases the deadline guard, so Bob's later idle tick does not
/// re-arm an abandoned request. A host hook records the real scheduler calls.
#[test]
fn catalogue_deadline_is_scheduled_and_cancelled_with_the_read() {
    let alice = open(1, schema("notes", "text"), None);
    let (a, _bob) = duplex();
    block_on(alice.connect_upstream(a));
    let scheduler = Rc::new(Scheduler::default());
    alice.set_tick_scheduler(Some(scheduler.clone()));
    {
        let mut read = pin!(alice.cover_catalogue("notes", None));
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(scheduler.immediate.get(), 1);
        assert!(
            scheduler
                .delays
                .borrow()
                .iter()
                .any(|delay| (1..=15_000).contains(delay))
        );
    }
    let timers = scheduler.delays.borrow().len();
    block_on(alice.tick()).unwrap();
    assert_eq!(scheduler.delays.borrow().len(), timers);
}

/// Alice's queued request cannot be sent to Bob's replacement connection, even
/// if she reconnects before the original request's first drive turn.
#[test]
fn reconnect_removes_queued_catalogue_request_before_send() {
    let alice = open(1, schema("notes", "text"), None);
    let (a, _old) = duplex();
    block_on(alice.connect_upstream(a));
    let mut read = pin!(alice.cover_catalogue("notes", None));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let (a, mut bob) = duplex();
    block_on(alice.connect_upstream(a));
    block_on(alice.tick()).unwrap();
    while let Some(message) = bob.try_recv() {
        assert!(!matches!(message, SyncMessage::CatalogueFetchV1 { .. }));
    }
    assert!(matches!(
        read.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(_))
    ));
}

struct BlockedSend {
    inner: Box<dyn Transport>,
    blocked: Rc<std::cell::Cell<bool>>,
}
impl Transport for BlockedSend {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if self.blocked.get() {
            Err(TransportError::Backpressure)
        } else {
            self.inner.send(message)
        }
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
}

/// When Alice disconnects, Bob's relay drops her pending upstream request even
/// while output is backpressured. The gated transport exposes queued sends;
/// releasing it must not send work owned by a closed subscriber slot.
#[test]
fn disconnected_relay_session_releases_queued_coverage() {
    let s = schema("notes", "text");
    let relay = open(2, s.clone(), None);
    let blocked = Rc::new(std::cell::Cell::new(true));
    let (up, mut authority) = duplex();
    block_on(relay.connect_upstream(Box::new(BlockedSend {
        inner: up,
        blocked: blocked.clone(),
    })));
    let (mut alice, down) = duplex();
    let subscriber = relay.accept_subscriber(down, user("alice"));
    alice
        .send(SyncMessage::Schema {
            version: *s.version_id().as_bytes(),
        })
        .unwrap();
    alice
        .send(SyncMessage::CatalogueFetchV1 {
            id: 128,
            table: "notes".into(),
            opened: s.version_id(),
            scope: None,
        })
        .unwrap();
    block_on(relay.tick()).unwrap();
    relay.disconnect_subscriber(subscriber);
    blocked.set(false);
    block_on(relay.tick()).unwrap();
    while let Some(message) = authority.try_recv() {
        assert!(!matches!(message, SyncMessage::CatalogueFetchV1 { .. }));
    }
    assert_eq!(relay.open_subscribers(), 0);
}

/// Bob's authority broadcasts a renamed catalogue through Alice's idle relay in
/// the same receive turn. No fresh coverage request or unrelated row traffic is
/// needed to deliver the new accepted descriptor to her foreground.
#[test]
fn unsolicited_catalogue_publication_flushes_through_an_idle_relay() {
    let mut snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let relay = open(2, snapshot.schemas[0].schema.clone(), None);
    connect(&relay, &bob, AuthorSubject::SYSTEM, None);
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    connect(&alice, &relay, user("alice"), None);
    drive(
        &[&alice, &relay, &bob],
        alice.cover_catalogue("notes", None),
    )
    .unwrap();
    let table = alice.table_identity("notes").unwrap();
    let lens = rename(&mut snapshot, "tasks", "body");
    block_on(
        bob.activate_schema_with_lenses(snapshot.schemas.last().unwrap().schema.clone(), &[lens]),
    )
    .unwrap();
    bob.install_authority_catalogue(&snapshot).unwrap();
    turn(&[&bob, &relay, &alice]);
    assert_eq!(alice.table_identity("tasks").unwrap(), table);
    assert_eq!(alice.table_identity("notes").unwrap(), None);
}

/// Mallory's ordinary subscriber cannot publish catalogue metadata into Bob's
/// authority. Bob closes that session and retains his own accepted identities.
#[test]
fn subscriber_cannot_install_authority_metadata() {
    let snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let expected = bob.table_identity("notes").unwrap();
    let (mut mallory, down) = duplex();
    bob.accept_subscriber(down, user("mallory"));
    mallory
        .send(SyncMessage::CatalogueMetadataV1 {
            head: jazz::db::CatalogueHeadV1 {
                root: [9; 32],
                through: 0,
                revision: 1,
                current: snapshot.schemas[0].id,
            },
            schemas: vec![jazz::db::CatalogueSchemaV1 {
                schema: snapshot.schemas[0].clone(),
                identities: snapshot.genesis_physical_identities.clone(),
            }],
        })
        .unwrap();
    block_on(bob.tick()).unwrap();
    assert_eq!(bob.open_subscribers(), 0);
    assert_eq!(bob.take_closed_subscribers().len(), 1);
    assert_eq!(bob.table_identity("notes").unwrap(), expected);
}

struct ByteLink {
    outbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
    inbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
    observed: Rc<RefCell<Vec<Vec<u8>>>>,
}
impl jazz::wire::WireTransport for ByteLink {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.observed.borrow_mut().push(frame.clone());
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }
    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}
fn connect_wire(client: &Db, server: &Db, who: AuthorSubject, observed: Rc<RefCell<Vec<Vec<u8>>>>) {
    let up = Rc::default();
    let down = Rc::default();
    let a = ByteLink {
        outbound: Rc::clone(&up),
        inbound: Rc::clone(&down),
        observed: observed.clone(),
    };
    let b = ByteLink {
        outbound: down,
        inbound: up,
        observed,
    };
    block_on(client.connect_upstream(Box::new(jazz::db::WireTransportAdapter::current(a))));
    server.accept_subscriber(Box::new(jazz::db::WireTransportAdapter::current(b)), who);
}

/// Alice obtains Bob's exact physical IDs through a real byte-framed relay
/// path. Every ordinary and catalogue frame uses v4; zero-row coverage sends
/// no row bodies. The byte queues isolate the encoder/decoder carrier boundary.
#[test]
fn catalogue_coverage_crosses_both_byte_transport_hops() {
    let snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let relay = open(2, snapshot.schemas[0].schema.clone(), None);
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    let observed = Rc::new(RefCell::new(Vec::new()));
    connect_wire(&relay, &bob, AuthorSubject::SYSTEM, observed.clone());
    connect_wire(&alice, &relay, user("alice"), observed.clone());
    drive(
        &[&alice, &relay, &bob],
        alice.cover_catalogue("notes", None),
    )
    .unwrap();
    assert_eq!(
        alice.table_identity("notes").unwrap(),
        bob.table_identity("notes").unwrap()
    );
    let mut kinds = std::collections::BTreeSet::new();
    for frame in observed.borrow().iter() {
        assert!(frame.starts_with(jazz::db::SYNC_ENCODING_PREFIX));
        let decoded = jazz::db::decode_message(frame).unwrap();
        assert!(!matches!(decoded, SyncMessage::Rows { .. }));
        kinds.insert(decoded.kind());
    }
    for expected in [
        "Schema",
        "CatalogueMetadataV1",
        "CatalogueFetchV1",
        "CatalogueCoveredV1",
        "PolicyDescriptorV1",
    ] {
        assert!(kinds.contains(expected), "missing {expected}");
    }
}
