//! Ordinary relay fetches wait for upstream and retain their admitted scope.
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ErrorCode, ReadOpts, ReadTier, SerializedReadResult,
    SyncMessage, Transport, TransportError, Upstream, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, provider_claim_key};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, VecDeque},
    future::Future,
    rc::Rc,
    task::{Context, Poll, Waker},
};

fn user(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", name).unwrap()
}
fn claims(code: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(provider_claim_key("code"), Value::String(code.into()))])
}
fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .column("code", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::and(vec![
                        PolicyExpr::eq_session(
                            "owner",
                            vec!["user".into(), "identity".into(), "subject".into()],
                        ),
                        PolicyExpr::eq_session("code", vec!["claims".into(), "code".into()]),
                    ]))),
            )
            .build(),
    )
    .unwrap()
}
fn open(node: u8, author: AuthorSubject, policy_free: bool) -> Db {
    let schema = if policy_free {
        schema().without_permissions()
    } else {
        schema()
    };
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    let db = if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap();
    db.set_identity_claims(author, claims("a"));
    db
}
fn authority() -> Db {
    let db = open(0, AuthorSubject::SYSTEM, false);
    for (n, title, owner, code) in [
        (1, "alpha", "alice", "a"),
        (2, "beta", "alice", "a"),
        (3, "gamma", "alice", "a"),
        (4, "private token", "alice", "b"),
        (5, "bob", "bob", "a"),
    ] {
        db.seed_settled_mergeable_for_bootstrap(
            "notes",
            RowUuid::from_bytes([n; 16]),
            AuthorSubject::SYSTEM,
            BTreeMap::from([
                ("title".into(), Value::String(title.into())),
                ("owner".into(), Value::String(owner.into())),
                ("code".into(), Value::String(code.into())),
            ]),
        )
        .unwrap();
    }
    db
}
fn connect(
    server: &Db,
    client: &Db,
    identity: AuthorSubject,
    scope: BTreeMap<String, Value>,
) -> Upstream {
    let (up, down) = duplex();
    let upstream = block_on(client.connect_upstream(up));
    server.accept_subscriber_with_claims(down, identity, Some(scope));
    upstream
}
fn turns(nodes: &[&Db]) {
    for _ in 0..8 {
        for db in nodes {
            block_on(db.tick()).unwrap();
        }
    }
}
fn drive<T>(nodes: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    for _ in 0..150 {
        if let Poll::Ready(result) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return result;
        }
        turns(nodes);
    }
    panic!("relay fetch did not finish");
}
fn query() -> Query {
    Query::from("notes")
        .select(["title"])
        .order_by("title", OrderDirection::Asc)
}
async fn read(db: &Db, query: Query) -> Result<Vec<String>, Error> {
    let bytes = postcard::to_allocvec(&query).unwrap();
    let result = db
        .all_serialized_query(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            None,
            false,
            || false,
            |_| {},
        )
        .await?;
    let SerializedReadResult::Rows(rows) = result else {
        panic!("rows")
    };
    Ok(rows
        .iter()
        .map(|row| {
            let cells = row.cells().collect::<Vec<_>>();
            assert_eq!(cells.len(), 1);
            assert_eq!(cells[0].0, "title");
            let Some(Value::String(title)) = cells[0].1 else {
                panic!("title")
            };
            title.clone()
        })
        .collect())
}

/// Alice's ordinary relay inherits host-bound claims when its subscriber has
/// no connection override. An explicitly empty or different token must still
/// fail the relay's upstream-scope check, even for the same author.
#[test]
fn ordinary_relay_fetch_inherits_only_unoverridden_host_claims() {
    for override_claims in [None, Some(BTreeMap::new()), Some(claims("b"))] {
        let server = authority();
        let relay = open(1, user("alice"), false);
        let foreground = open(2, user("alice"), true);
        connect(&server, &relay, user("alice"), claims("a"));
        let (up, down) = duplex();
        block_on(foreground.connect_upstream(up));
        relay.accept_subscriber_with_claims(down, user("alice"), override_claims.clone());
        let result = drive(&[&foreground, &relay, &server], read(&foreground, query()));
        match override_claims {
            None => assert_eq!(result.unwrap(), ["alpha", "beta", "gamma"]),
            Some(_) => assert_eq!(result.unwrap_err().code, ErrorCode::WriteRejected),
        }
    }
}

// Gate only real authority answer frames: public reads cannot pause between
// delivered row images and their receipt. No storage or permission hooks.
#[derive(Default)]
struct Gate {
    hold: Cell<bool>,
    held: RefCell<VecDeque<SyncMessage>>,
    ordinary: Cell<usize>,
    delegated: Cell<usize>,
}
struct Gated {
    wire: Box<dyn Transport>,
    gate: Rc<Gate>,
}
impl Transport for Gated {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        match &message {
            SyncMessage::Fetch { .. } => self.gate.ordinary.set(self.gate.ordinary.get() + 1),
            SyncMessage::ScopedFetchV1 { .. } => {
                self.gate.delegated.set(self.gate.delegated.get() + 1)
            }
            _ => {}
        }
        self.wire.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if !self.gate.hold.get()
            && let Some(message) = self.gate.held.borrow_mut().pop_front()
        {
            return Some(message);
        }
        while let Some(message) = self.wire.try_recv() {
            if self.gate.hold.get() && matches!(message, SyncMessage::Answer { .. }) {
                self.gate.held.borrow_mut().push_back(message);
            } else {
                return Some(message);
            }
        }
        None
    }
}
fn gated(server: &Db, relay: &Db, gate: Rc<Gate>) -> Upstream {
    let (up, down) = duplex();
    let upstream = block_on(relay.connect_upstream(Box::new(Gated { wire: up, gate })));
    server.accept_subscriber_with_claims(down, user("alice"), Some(claims("a")));
    upstream
}

/// Alice's ordered one-shot read crosses two cold relays and applies its
/// offset only after the authority's permitted prefix has arrived.
#[test]
fn cold_multi_hop_fetch_keeps_authority_scope_and_window() {
    let server = authority();
    let first = open(1, user("alice"), false);
    let second = open(2, user("alice"), false);
    let client = open(3, user("alice"), false);
    connect(&server, &first, user("alice"), claims("a"));
    connect(&first, &second, user("alice"), claims("a"));
    connect(&second, &client, user("alice"), claims("a"));
    assert_eq!(
        drive(
            &[&client, &second, &first, &server],
            read(&client, query().offset(1).limit(1))
        )
        .unwrap(),
        ["beta"]
    );
}

/// Alice's private owner starts with no policies. Its first authenticated
/// catalogue can arrive during the fetch without losing or widening the read.
#[test]
fn cold_policy_free_private_owner_admits_its_first_fetch() {
    let server = authority();
    let relay = open(1, user("alice"), true);
    relay.set_scope_isolated_storage_owner().unwrap();
    let client = open(2, user("alice"), false);
    connect(&server, &relay, user("alice"), claims("a"));
    connect(&relay, &client, user("alice"), claims("a"));
    assert_eq!(
        drive(&[&client, &relay, &server], read(&client, query())).unwrap(),
        ["alpha", "beta", "gamma"]
    );
}

/// Mallory cannot borrow Alice's upstream identity, and Alice's second token
/// cannot borrow the first token's claims. A rejected read sends no fetch upstream.
#[test]
fn ordinary_relay_refuses_different_identity_or_claims_before_forwarding() {
    let server = authority();
    let relay = open(1, user("alice"), false);
    let gate = Rc::new(Gate::default());
    gated(&server, &relay, gate.clone());
    for (identity, scope, expected) in [
        (user("mallory"), claims("a"), "identity"),
        (user("alice"), claims("b"), "claims"),
    ] {
        let (mut peer, down) = duplex();
        relay.accept_subscriber_with_claims(down, identity, Some(scope));
        peer.send(SyncMessage::Schema {
            version: *schema().version_id().as_bytes(),
        })
        .unwrap();
        peer.send(SyncMessage::Fetch {
            view: jazz::protocol::ReadViewSourceSpec::Current,
            tier: jazz::db::ReadTier::Remote,
            wait: None,
            id: 77,
            query: query(),
            binding: BTreeMap::new(),
            at: None,
            include_deleted: false,
        })
        .unwrap();
        turns(&[&relay, &server]);
        let mut refused = false;
        while let Some(message) = peer.try_recv() {
            match message {
                SyncMessage::Refused {
                    id: 77,
                    code,
                    message,
                } => {
                    assert_eq!(code, ErrorCode::WriteRejected);
                    assert!(message.contains(expected), "{message}");
                    refused = true;
                }
                SyncMessage::Rows { images } => {
                    assert!(images.is_empty(), "wrong scope received rows")
                }
                SyncMessage::Answer { id: 77, .. } => panic!("wrong scope answered"),
                _ => {}
            }
        }
        assert!(refused);
    }
    assert_eq!(gate.ordinary.get(), 0);
    assert_eq!(gate.delegated.get(), 0);
    let client = open(2, user("alice"), false);
    connect(&relay, &client, user("alice"), claims("a"));
    assert_eq!(
        drive(&[&client, &relay, &server], read(&client, query())).unwrap(),
        ["alpha", "beta", "gamma"]
    );
    assert_eq!(gate.ordinary.get(), 1);
    assert_eq!(gate.delegated.get(), 0);
}

/// Alice's pending read retains token a even if the relay's ambient claims
/// become token b before the authority receipt is delivered.
#[test]
fn ordinary_forwarded_read_freezes_claims_until_its_receipt() {
    let server = authority();
    let relay = open(1, user("alice"), false);
    let client = open(2, user("alice"), false);
    let gate = Rc::new(Gate::default());
    gate.hold.set(true);
    gated(&server, &relay, gate.clone());
    connect(&relay, &client, user("alice"), claims("a"));
    let nodes = [&client, &relay, &server];
    let mut future = std::pin::pin!(read(&client, query()));
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    turns(&nodes);
    assert_eq!(gate.held.borrow().len(), 1);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    relay.set_identity_claims(user("alice"), claims("b"));
    gate.hold.set(false);
    assert_eq!(drive(&nodes, future).unwrap(), ["alpha", "beta", "gamma"]);
}

/// Alice's lost upstream cannot settle a pending relay fetch from cached rows.
/// Reconnecting retires that request, and a new request reaches the authority.
#[test]
fn ordinary_forwarded_read_refuses_disconnect_and_recovers_after_reconnect() {
    let server = authority();
    let relay = open(1, user("alice"), false);
    let client = open(2, user("alice"), false);
    let gate = Rc::new(Gate::default());
    gate.hold.set(true);
    let upstream = gated(&server, &relay, gate.clone());
    connect(&relay, &client, user("alice"), claims("a"));
    let nodes = [&client, &relay, &server];
    let mut future = std::pin::pin!(read(&client, query()));
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    turns(&nodes);
    assert_eq!(gate.held.borrow().len(), 1);
    assert!(relay.detach_connection(&upstream));
    let error = drive(&nodes, future).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    // A new Remote request waits for fresh authority coverage; cached rows
    // cannot turn the interrupted request into a success.
    let mut recovered = std::pin::pin!(read(&client, query()));
    for _ in 0..3 {
        assert!(
            recovered
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        turns(&nodes);
    }
    block_on(server.update(
        "notes",
        RowUuid::from_bytes([1; 16]),
        BTreeMap::from([("title".into(), Value::String("zeta".into()))]),
        Default::default(),
    ))
    .unwrap();
    connect(&server, &relay, user("alice"), claims("a"));
    assert_eq!(drive(&nodes, recovered).unwrap(), ["beta", "gamma", "zeta"]);
}

/// A host-trusted SYSTEM relay may delegate Alice's authenticated ordinary
/// request. Its upstream still enforces Alice's identity and token, not SYSTEM.
#[test]
fn trusted_relay_forwards_an_ordinary_read_with_its_session_scope() {
    let server = authority();
    let relay = open(1, AuthorSubject::SYSTEM, false);
    let client = open(2, user("alice"), false);
    client.set_identity_claims(user("alice"), claims("b"));
    connect(&server, &relay, AuthorSubject::SYSTEM, BTreeMap::new());
    connect(&relay, &client, user("alice"), claims("b"));
    assert_eq!(
        drive(&[&client, &relay, &server], read(&client, query())).unwrap(),
        ["private token"]
    );
}

/// Alice's pending request cannot survive a replacement upstream connection,
/// even when the old authority already delivered rows but its receipt is held.
#[test]
fn replacing_upstream_retires_an_in_flight_ordinary_fetch() {
    let server = authority();
    let relay = open(1, user("alice"), false);
    let client = open(2, user("alice"), false);
    let gate = Rc::new(Gate::default());
    gate.hold.set(true);
    gated(&server, &relay, gate.clone());
    connect(&relay, &client, user("alice"), claims("a"));
    let nodes = [&client, &relay, &server];
    let mut future = std::pin::pin!(read(&client, query()));
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    turns(&nodes);
    assert_eq!(gate.held.borrow().len(), 1);
    connect(&server, &relay, user("alice"), claims("a"));
    gate.hold.set(false);
    let error = drive(&nodes, future).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert!(error.message.contains("reconnect"), "{error}");
    assert_eq!(
        drive(&nodes, read(&client, query())).unwrap(),
        ["alpha", "beta", "gamma"]
    );
}

/// Alice's physical schema changes while a fetch is pending. That request
/// must fail instead of being rebound as if it were only a policy refresh.
#[test]
fn changing_relay_schema_retires_an_in_flight_ordinary_fetch() {
    let server = authority();
    let relay = open(1, user("alice"), false);
    let client = open(2, user("alice"), false);
    let gate = Rc::new(Gate::default());
    gate.hold.set(true);
    gated(&server, &relay, gate.clone());
    connect(&relay, &client, user("alice"), claims("a"));
    let nodes = [&client, &relay, &server];
    let mut future = std::pin::pin!(read(&client, query()));
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    turns(&nodes);
    assert_eq!(gate.held.borrow().len(), 1);
    let mut public = schema().public_schema().clone();
    public.extend(
        SchemaBuilder::new()
            .table(TableSchemaBuilder::new("added").column("value", ColumnType::Text))
            .build(),
    );
    block_on(relay.activate_schema(JazzSchema::new(&public).unwrap())).unwrap();
    gate.hold.set(false);
    let error = drive(&nodes, future).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert!(error.message.contains("schema change"), "{error}");
}
