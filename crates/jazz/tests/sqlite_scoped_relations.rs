//! A trusted backend carries each traversal's request claims through its authority.
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, InsertOptions, ReadOpts, ReadTier, SerializedReadResult,
    block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, provider_claim_key};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([1; 16])
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn claims(code: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(provider_claim_key("code"), Value::String(code.into()))])
}
fn schema() -> JazzSchema {
    schema_with_private_links(false)
}
fn schema_with_private_links(private: bool) -> JazzSchema {
    let permission = || {
        TablePolicies::new().with_select(PolicyExpr::eq_session(
            "code",
            vec!["claims".into(), "code".into()],
        ))
    };
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("title", ColumnType::Text)
                    .column("code", ColumnType::Text)
                    .policies(permission()),
            )
            .table(
                TableSchemaBuilder::new("links")
                    .fk_column("room", "rooms")
                    .column("code", ColumnType::Text)
                    .policies(if private {
                        TablePolicies::new().with_select(PolicyExpr::Exists {
                            table: "access".into(),
                            condition: Box::new(PolicyExpr::And(vec![
                                PolicyExpr::eq_session(
                                    "link",
                                    vec!["__jazz_outer_row".into(), "id".into()],
                                ),
                                PolicyExpr::eq_session(
                                    "code",
                                    vec!["claims".into(), "code".into()],
                                ),
                            ])),
                        })
                    } else {
                        permission()
                    }),
            )
            .table(
                TableSchemaBuilder::new("tags")
                    .fk_column("link", "links")
                    .column("code", ColumnType::Text)
                    .policies(permission()),
            )
            .table(
                TableSchemaBuilder::new("access")
                    .fk_column("link", "links")
                    .column("code", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::False)),
            )
            .build(),
    )
    .unwrap()
}
fn open(node: u8, authority: bool) -> Db {
    open_schema(schema(), node, authority)
}
fn open_schema(schema: JazzSchema, node: u8, authority: bool) -> Db {
    let schema = if authority {
        schema
    } else {
        schema.without_permissions()
    };
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    if authority {
        db.set_authority();
        db.initialize_authority_catalogue().unwrap();
    }
    db
}
fn insert(db: &Db, table: &str, n: u8, fields: &[(&str, Value)]) {
    block_on(
        db.insert(
            table,
            fields
                .iter()
                .map(|(k, v)| ((*k).into(), v.clone()))
                .collect(),
            InsertOptions {
                row_id: Some(id(n)),
                ..Default::default()
            },
        ),
    )
    .unwrap();
}
fn seed(db: &Db) {
    for (n, code) in [(10, "a"), (11, "b"), (12, "a")] {
        insert(
            db,
            "rooms",
            n,
            &[
                ("title", Value::String(format!("room-{n}"))),
                ("code", Value::String(code.into())),
            ],
        );
        insert(
            db,
            "links",
            n + 10,
            &[
                ("room", Value::Uuid(id(n).0)),
                ("code", Value::String(code.into())),
            ],
        );
        insert(
            db,
            "tags",
            n + 20,
            &[
                ("link", Value::Uuid(id(n + 10).0)),
                ("code", Value::String(code.into())),
            ],
        );
    }
    // Mallory's witness refers to Alice's visible root but is not readable by Alice.
    insert(
        db,
        "links",
        23,
        &[
            ("room", Value::Uuid(id(10).0)),
            ("code", Value::String("mallory".into())),
        ],
    );
}
fn connect(server: &Db, backend: &Db, relay: Option<&Db>) {
    let (up, down) = duplex();
    block_on(backend.connect_upstream(up));
    if let Some(relay) = relay {
        relay.accept_subscriber(down, AuthorSubject::SYSTEM);
        let (up, down) = duplex();
        block_on(relay.connect_upstream(up));
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
    } else {
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
    }
}
fn tick(nodes: &[&Db]) {
    for node in nodes {
        block_on(node.tick()).unwrap();
    }
}
fn drive<T>(nodes: &[&Db], future: impl Future<Output = T>) -> T {
    let mut pending = pin!(future);
    for _ in 0..200 {
        if let Poll::Ready(value) = pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        tick(nodes);
    }
    panic!("scoped relation read did not settle");
}
async fn read(db: &Db, query: &Query, code: Option<&str>) -> Result<Vec<RowUuid>, Error> {
    let bytes = postcard::to_allocvec(query).unwrap();
    let result = db
        .all_serialized_query_as(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            code.map(|code| (alice(), claims(code))),
            code.map(|_| alice()),
            false,
            || false,
            |_| {},
        )
        .await?;
    let SerializedReadResult::Rows(rows) = result else {
        panic!("rows");
    };
    Ok(rows.into_iter().map(|r| r.row_uuid()).collect())
}
fn query(nested: bool) -> Query {
    if nested {
        let tag = Query::from("links")
            .join_via("tags", "link", [])
            .joins
            .remove(0);
        Query::from("rooms").join_via_with_nested_joins("links", "room", [], [tag])
    } else {
        Query::from("rooms").join_via("links", "room", [])
    }
}

/// Alice has two simultaneous request claim snapshots; Bob's shared backend
/// cache and an optional trusted relay must not replace either snapshot.
///
/// Alice(code=a/b) -> backend -> [relay] -> authority -> exact witnesses
#[test]
fn scoped_traversals_isolate_concurrent_claims_directly_and_through_relay() {
    for relayed in [false, true] {
        for nested in [false, true] {
            let server = open(81, true);
            seed(&server);
            let backend = open(82, false);
            let relay = open(83, false);
            connect(&server, &backend, relayed.then_some(&relay));
            let nodes = [&server, &relay, &backend];
            let q = query(nested);
            let mut a = pin!(read(&backend, &q, Some("a")));
            let mut b = pin!(read(&backend, &q, Some("b")));
            let mut absent = pin!(read(&backend, &q, Some("none")));
            let (mut ra, mut rb, mut rn) = (None, None, None);
            for _ in 0..200 {
                for (slot, future) in [
                    (&mut ra, a.as_mut()),
                    (&mut rb, b.as_mut()),
                    (&mut rn, absent.as_mut()),
                ] {
                    if slot.is_none() {
                        if let Poll::Ready(value) =
                            future.poll(&mut Context::from_waker(Waker::noop()))
                        {
                            *slot = Some(value);
                        }
                    }
                }
                if ra.is_some() && rb.is_some() && rn.is_some() {
                    break;
                }
                tick(&nodes);
            }
            assert_eq!(
                ra.unwrap().unwrap(),
                vec![id(10), id(12)],
                "relayed={relayed}, nested={nested}"
            );
            assert_eq!(rb.unwrap().unwrap(), vec![id(11)]);
            assert!(rn.unwrap().unwrap().is_empty());
            assert_eq!(
                drive(&nodes, read(&backend, &Query::from("links"), Some("a"))).unwrap(),
                vec![id(20), id(22)]
            );
        }
    }
}

/// Alice's warm backend holds Bob's and Mallory's rows. Authority windows and
/// deleted or retargeted relation witnesses still govern each fresh request.
#[test]
fn scoped_traversal_windows_ignore_unrelated_cache_and_revoked_witnesses() {
    let server = open(84, true);
    seed(&server);
    let backend = open(85, false);
    let relay = open(86, false);
    connect(&server, &backend, Some(&relay));
    let nodes = [&server, &relay, &backend];
    for table in ["rooms", "links", "tags"] {
        drive(&nodes, read(&backend, &Query::from(table), None)).unwrap();
    }
    let mut window = query(true);
    window.offset = 1;
    window.limit = Some(1);
    assert_eq!(
        drive(&nodes, read(&backend, &window, Some("a"))).unwrap(),
        vec![id(12)]
    );
    block_on(server.delete("tags", id(32), Default::default())).unwrap();
    assert!(
        drive(&nodes, read(&backend, &window, Some("a")))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        drive(&nodes, read(&backend, &query(true), Some("a"))).unwrap(),
        vec![id(10)]
    );
    block_on(server.update(
        "links",
        id(20),
        BTreeMap::from([("code".into(), Value::String("b".into()))]),
        Default::default(),
    ))
    .unwrap();
    assert!(
        drive(&nodes, read(&backend, &query(false), Some("a")))
            .unwrap()
            .iter()
            .all(|row| *row != id(10))
    );
}

// Record and optionally alter real transport receipts: no storage hooks or
// alternate read path participate in these request-isolation assertions.
struct EvidenceTransport {
    wire: Box<dyn jazz::db::Transport>,
    seen: std::rc::Rc<std::cell::RefCell<Vec<jazz::db::SyncMessage>>>,
    omit_links: bool,
}
impl jazz::db::Transport for EvidenceTransport {
    fn send(&mut self, message: jazz::db::SyncMessage) -> Result<(), jazz::db::TransportError> {
        self.wire.send(message)
    }
    fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
        let mut message = self.wire.try_recv()?;
        self.seen.borrow_mut().push(message.clone());
        if self.omit_links {
            if let jazz::db::SyncMessage::ScopedEvidenceAnswerV1 { evidence, .. } = &mut message {
                evidence.retain(|row| row.table != "links");
            }
        }
        Some(message)
    }
}

/// Alice receives only request-readable query witnesses. Removing one exact
/// witness from the authority receipt must fail even after Bob warms the cache.
#[test]
fn scoped_relation_receipts_pin_witnesses_and_omit_unreadable_rows() {
    for omit_links in [false, true] {
        let server = open(87, true);
        seed(&server);
        let backend = open(88, false);
        let seen = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let (up, down) = duplex();
        block_on(backend.connect_upstream(Box::new(EvidenceTransport {
            wire: up,
            seen: seen.clone(),
            omit_links,
        })));
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
        let nodes = [&server, &backend];
        if omit_links {
            drive(&nodes, read(&backend, &Query::from("links"), None)).unwrap();
            seen.borrow_mut().clear();
        }
        let answer = drive(&nodes, read(&backend, &query(false), Some("a")));
        if omit_links {
            let error = answer.unwrap_err();
            assert_eq!(error.code, jazz::db::ErrorCode::NotObserved);
            assert!(error.message.contains("does not reproduce"), "{error:?}");
        } else {
            assert_eq!(answer.unwrap(), vec![id(10), id(12)]);
        }
        for message in seen.borrow().iter() {
            match message {
                jazz::db::SyncMessage::Rows { images } => {
                    assert!(
                        !images
                            .iter()
                            .any(|image| image.table == "links" && image.id == *id(23).as_bytes()),
                        "Mallory's unreadable witness shipped"
                    );
                }
                jazz::db::SyncMessage::ScopedEvidenceAnswerV1 { evidence, .. } => {
                    let links: Vec<_> = evidence
                        .iter()
                        .filter(|row| row.table == "links")
                        .map(|row| row.id)
                        .collect();
                    assert_eq!(links, vec![*id(20).as_bytes(), *id(22).as_bytes()]);
                }
                _ => {}
            }
        }
    }
}

/// Alice's relation query must be rejected before execution when its witness
/// lookup lacks an indexed UUID correlation, including over an empty table.
#[test]
fn scoped_relation_reads_reject_unbounded_correlations() {
    let server = open(89, true);
    let backend = open(90, false);
    connect(&server, &backend, None);
    let nodes = [&server, &backend];
    let q = Query::from("rooms").join_via_column("links", "code", "code", []);
    let error = drive(&nodes, read(&backend, &q, Some("a"))).unwrap_err();
    assert_eq!(error.code, jazz::db::ErrorCode::Query);
    assert!(
        error.message.contains("indexed scalar UUID correlation"),
        "{error:?}"
    );
}

/// Mallory's ordinary session cannot submit delegated relation-read claims,
/// even though the same request is valid on a host-admitted trusted backend.
#[test]
fn ordinary_sessions_cannot_delegate_scoped_relation_reads() {
    use jazz::db::SyncMessage;
    let server = open(91, true);
    seed(&server);
    let (mut client, peer) = duplex();
    server.accept_subscriber(peer, AuthorSubject::for_test_bytes([9; 16]));
    client
        .send(SyncMessage::Schema {
            version: *schema().version_id().as_bytes(),
        })
        .unwrap();
    client
        .send(SyncMessage::ScopedFetchV1 {
            id: 700,
            query: query(false),
            binding: BTreeMap::new(),
            author: alice().canonical().into(),
            claims: claims("a"),
            include_deleted: false,
        })
        .unwrap();
    block_on(server.tick()).unwrap();
    let mut refused = false;
    while let Some(message) = client.try_recv() {
        match message {
            SyncMessage::Refused { id: 700, .. } => refused = true,
            SyncMessage::Rows { images } => assert!(images.is_empty()),
            SyncMessage::ScopedEvidenceAnswerV1 { id: 700, .. }
            | SyncMessage::Answer { id: 700, .. } => panic!("forged delegation answered"),
            _ => {}
        }
    }
    assert!(refused);
}

/// Alice's readable link depends on a private grant. A trusted relay carries
/// that exact policy input without exposing the grant as a session result.
#[test]
fn scoped_relation_witnesses_keep_their_private_permission_dependencies() {
    let server = open_schema(schema_with_private_links(true), 92, true);
    seed(&server);
    insert(
        &server,
        "access",
        40,
        &[
            ("link", Value::Uuid(id(20).0)),
            ("code", Value::String("a".into())),
        ],
    );
    let backend = open_schema(schema_with_private_links(true), 93, false);
    let relay = open_schema(schema_with_private_links(true), 94, false);
    connect(&server, &backend, Some(&relay));
    let nodes = [&server, &relay, &backend];
    assert_eq!(
        drive(&nodes, read(&backend, &query(false), Some("a"))).unwrap(),
        vec![id(10)]
    );
    assert!(
        drive(&nodes, read(&backend, &query(false), Some("b")))
            .unwrap()
            .is_empty()
    );
    assert!(
        drive(&nodes, read(&backend, &Query::from("access"), Some("a")))
            .unwrap()
            .is_empty()
    );
    block_on(server.delete("access", id(40), Default::default())).unwrap();
    assert!(
        drive(&nodes, read(&backend, &query(false), Some("a")))
            .unwrap()
            .is_empty()
    );
}
