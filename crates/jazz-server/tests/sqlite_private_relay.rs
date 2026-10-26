//! An authenticated browser owner retains ordinary session permissions.
use futures_util::{SinkExt, StreamExt};
use jazz::db::{Db, DbConfig, DbIdentity, RemoteLinkHint, SyncMessage, Transport, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::AppId;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::wire::{FEATURE_SCOPE_ISOLATED_CLIENT_RELAY, TransportError};
use jazz::wire::{WireFrame, WireHello, WirePeerRole, decode_frame, encode_frame};
use jazz_server::runtime::{
    CommitUnitTrust, ServerLinkAdmission, ServerRuntimeFrameStream, ServerRuntimeHandle,
    ServerSession, StorageConfig,
};
use jazz_server::{AuthConfig, BuiltServer, ServerBuilder, StorageBackend};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc, time::Duration};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

struct WebSocketServer {
    built: BuiltServer,
    listener: tokio::task::JoinHandle<()>,
    url: String,
}

impl WebSocketServer {
    async fn start(app_id: AppId, storage: StorageBackend) -> Self {
        let built = ServerBuilder::new(app_id)
            .with_auth_config(AuthConfig {
                backend_secret: Some("private-relay-test-secret".into()),
                ..Default::default()
            })
            .with_storage(storage)
            .with_shutdown_timeout(Duration::from_secs(1))
            .with_core_server_shell_schema(schema())
            .build()
            .await
            .unwrap();
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/apps/{app_id}/ws", socket.local_addr().unwrap());
        let router = built.app.clone();
        let listener = tokio::spawn(async move { axum::serve(socket, router).await.unwrap() });
        Self {
            built,
            listener,
            url,
        }
    }

    async fn connect(&self, identity: AuthorSubject, owner: &str) -> Socket {
        let (mut socket, _) = connect_async(&self.url).await.unwrap();
        let (issuer, subject) = identity.principal_parts();
        socket
            .send(Message::Text(
                serde_json::json!({
                    "peer_identity": identity.canonical(),
                    "requested_link": "scope_isolated_client_relay",
                    "auth": {
                        "backend_secret": "private-relay-test-secret",
                        "backend_session": {
                            "account_id": identity.account_id(),
                            "issuer": issuer,
                            "user_id": subject,
                            "claims": {"owner": owner},
                            "authMode": "external"
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        let hello = encode_frame(&WireFrame::Hello(WireHello::current(
            WirePeerRole::Client,
            FEATURE_SCOPE_ISOLATED_CLIENT_RELAY,
        )))
        .unwrap();
        socket
            .send(Message::Binary(
                postcard::to_allocvec(&vec![hello]).unwrap().into(),
            ))
            .await
            .unwrap();
        let frames = ws_frames(&mut socket).await;
        assert_eq!(frames.len(), 1);
        let WireFrame::Hello(hello) = decode_frame(&frames[0]).unwrap() else {
            panic!(
                "private relay was not admitted: {:?}",
                decode_frame(&frames[0])
            );
        };
        assert_eq!(hello.role, WirePeerRole::Core);
        assert_eq!(hello.features, FEATURE_SCOPE_ISOLATED_CLIENT_RELAY);
        assert!(hello.authority.is_some());
        ws_send(
            &mut socket,
            vec![SyncMessage::Schema {
                version: *schema().version_id().as_bytes(),
            }],
        )
        .await;
        socket
    }

    async fn shutdown(self) {
        self.built.shutdown().await;
        self.listener.abort();
        let _ = self.listener.await;
    }
}

async fn ws_frames(socket: &mut Socket) -> Vec<Vec<u8>> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Binary(bytes) => return postcard::from_bytes(&bytes).unwrap(),
                Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await.unwrap(),
                other => panic!("unexpected WebSocket frame: {other:?}"),
            }
        }
    })
    .await
    .unwrap()
}

async fn ws_send(socket: &mut Socket, messages: Vec<SyncMessage>) {
    let frames: Vec<_> = messages
        .iter()
        .map(|message| jazz::db::encode_message(message).unwrap())
        .collect();
    socket
        .send(Message::Binary(
            postcard::to_allocvec(&frames).unwrap().into(),
        ))
        .await
        .unwrap();
}

async fn ws_messages(socket: &mut Socket) -> Vec<SyncMessage> {
    ws_frames(socket)
        .await
        .iter()
        .map(|frame| jazz::sync_codec::decode_sync_message(frame).unwrap())
        .collect()
}

fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::eq_session(
                                "owner",
                                vec!["claims".into(), "owner".into()],
                            ))
                            .with_insert(PolicyExpr::eq_session(
                                "owner",
                                vec!["claims".into(), "owner".into()],
                            )),
                    ),
            )
            .build(),
    )
    .unwrap()
}
fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([1; 16])
}
fn bob() -> AuthorSubject {
    AuthorSubject::for_test_bytes([2; 16])
}
fn claims(owner: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        jazz::query::provider_claim_key("owner"),
        Value::String(owner.into()),
    )])
}
fn admission() -> ServerLinkAdmission {
    ServerLinkAdmission::ScopeIsolatedClientRelay { admission_epoch: 1 }
}
struct Capture(Rc<RefCell<Vec<SyncMessage>>>);
impl Transport for Capture {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.0.borrow_mut().push(message);
        Ok(())
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        None
    }
}
// Produce uploads through the public replica API, not a duplicate wire encoder.
fn upload(node: u8, author: AuthorSubject, owner: &str) -> SyncMessage {
    let db = block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap();
    db.set_remote_link_hint(RemoteLinkHint::Failed);
    db.set_identity_claims(author, claims(owner));
    block_on(db.insert(
        "notes",
        BTreeMap::from([("owner".into(), Value::String(owner.into()))]),
        Default::default(),
    ))
    .unwrap();
    let messages = Rc::new(RefCell::new(Vec::new()));
    block_on(db.connect_upstream(Box::new(Capture(messages.clone()))));
    for _ in 0..3 {
        block_on(db.tick()).unwrap();
    }
    let mut messages = messages.borrow_mut();
    let index = messages
        .iter()
        .position(|m| matches!(m, SyncMessage::Commit { .. }))
        .unwrap();
    messages.remove(index)
}
fn send(runtime: &ServerRuntimeHandle, session: ServerSession, messages: Vec<SyncMessage>) {
    runtime
        .receive_wire_frames(
            session,
            messages
                .iter()
                .map(|m| jazz::db::encode_message(m).unwrap())
                .collect(),
        )
        .unwrap();
}
async fn receive(stream: &mut ServerRuntimeFrameStream) -> Vec<SyncMessage> {
    tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .iter()
        .map(|m| jazz::sync_codec::decode_sync_message(m).unwrap())
        .collect()
}

/// Alice's owner forwards two tabs' original transaction numbers. The server
/// accepts her writes but rejects forged Bob attribution and forged claims;
/// the relay never gains SYSTEM access by requesting the private-owner link.
#[tokio::test]
async fn private_relay_keeps_origin_receipts_and_session_write_checks() {
    let runtime =
        ServerRuntimeHandle::start_with_storage(schema(), StorageConfig::InMemory).unwrap();
    let session = runtime
        .open_with_session_context(
            alice(),
            claims("alice"),
            CommitUnitTrust::Session,
            FEATURE_SCOPE_ISOLATED_CLIENT_RELAY,
            None,
            admission(),
        )
        .await
        .unwrap();
    let mut stream = runtime.open_wire_stream(session).unwrap();
    let mut forged_author = upload(12, bob(), "alice");
    if let SyncMessage::Commit { author, .. } = &mut forged_author {
        *author = Some(bob().canonical().into());
    }
    let mut forged_claims = upload(14, alice(), "bob");
    if let SyncMessage::Commit { session, .. } = &mut forged_claims {
        *session = Some(claims("bob"));
    }
    let mut messages = vec![
        SyncMessage::Schema {
            version: *schema().version_id().as_bytes(),
        },
        upload(10, alice(), "alice"),
        upload(11, alice(), "alice"),
        forged_author,
        upload(13, alice(), "bob"),
        forged_claims,
    ];
    let originals: BTreeMap<_, _> = messages
        .iter()
        .filter_map(|message| {
            if let SyncMessage::Commit { node, tx, .. } = message {
                Some((node[0], *tx))
            } else {
                None
            }
        })
        .collect();
    send(&runtime, session, std::mem::take(&mut messages));
    let mut fates = BTreeMap::new();
    while fates.len() < 5 {
        for message in receive(&mut stream).await {
            if let SyncMessage::OriginFateV2 {
                node, tx, result, ..
            } = message
            {
                assert_eq!(tx, originals[&node[0]]);
                fates.insert(node[0], result);
            }
        }
    }
    for node in [10, 11] {
        assert!(fates[&node].is_ok(), "{fates:?}");
    }
    for node in [12, 13, 14] {
        assert!(fates[&node].is_err(), "{fates:?}");
    }
    send(
        &runtime,
        session,
        vec![SyncMessage::AcceptedFetchV1 {
            id: 100,
            query: Query::from("notes"),
            binding: BTreeMap::new(),
            at: None,
            include_deleted: false,
            scope: Some((alice().canonical().into(), claims("alice"))),
        }],
    );
    'query: loop {
        for message in receive(&mut stream).await {
            match message {
                SyncMessage::AcceptedAnswerV1 { id: 100, rows, .. } => {
                    assert_eq!(rows.len(), 2);
                    break 'query;
                }
                SyncMessage::Refused { message, .. } => panic!("{message}"),
                _ => {}
            }
        }
    }
    runtime.close(session);
    runtime.shutdown().await.unwrap();
}

/// Mallory cannot use Alice's relay connection to read as Bob, override Alice's
/// claims, or submit the backend-only scoped fetch operation.
#[tokio::test]
async fn private_relay_cannot_delegate_reads() {
    let runtime =
        ServerRuntimeHandle::start_with_storage(schema(), StorageConfig::InMemory).unwrap();
    let session = runtime
        .open_with_session_context(
            alice(),
            claims("alice"),
            CommitUnitTrust::Session,
            FEATURE_SCOPE_ISOLATED_CLIENT_RELAY,
            None,
            admission(),
        )
        .await
        .unwrap();
    let mut stream = runtime.open_wire_stream(session).unwrap();
    send(
        &runtime,
        session,
        vec![
            SyncMessage::Schema {
                version: *schema().version_id().as_bytes(),
            },
            SyncMessage::AcceptedFetchV1 {
                id: 1,
                query: Query::from("notes"),
                binding: BTreeMap::new(),
                at: None,
                include_deleted: false,
                scope: Some((bob().canonical().into(), claims("alice"))),
            },
            SyncMessage::AcceptedFetchV1 {
                id: 2,
                query: Query::from("notes"),
                binding: BTreeMap::new(),
                at: None,
                include_deleted: false,
                scope: Some((alice().canonical().into(), claims("bob"))),
            },
            SyncMessage::ScopedFetchV1 {
                id: 3,
                query: Query::from("notes"),
                binding: BTreeMap::new(),
                include_deleted: false,
                author: bob().canonical().into(),
                claims: claims("bob"),
            },
            SyncMessage::CatalogueFetchV1 {
                id: 4,
                table: "notes".into(),
                opened: schema().version_id(),
                scope: Some((bob().canonical().into(), claims("bob"))),
            },
        ],
    );
    let mut refused = std::collections::BTreeSet::new();
    while refused.len() < 4 {
        for message in receive(&mut stream).await {
            match message {
                SyncMessage::Refused { id, .. } => {
                    refused.insert(id);
                }
                SyncMessage::AcceptedAnswerV1 { .. }
                | SyncMessage::Answer { .. }
                | SyncMessage::CatalogueCoveredV1 { .. } => {
                    panic!("forged scope answered: {message:?}")
                }
                _ => {}
            }
        }
    }
    assert_eq!(refused, [1, 2, 3, 4].into());
    runtime.close(session);
    runtime.shutdown().await.unwrap();
}

/// Bob's host cannot mislabel a backend, SYSTEM, anonymous or unnegotiated
/// connection as Alice's private storage-owner connection.
#[tokio::test]
async fn private_relay_admission_requires_session_and_feature() {
    let runtime =
        ServerRuntimeHandle::start_with_storage(schema(), StorageConfig::InMemory).unwrap();
    for (identity, trust, features, epoch) in [
        (
            alice(),
            CommitUnitTrust::TrustedBackend,
            FEATURE_SCOPE_ISOLATED_CLIENT_RELAY,
            1,
        ),
        (
            alice(),
            CommitUnitTrust::TrustedAuthority,
            FEATURE_SCOPE_ISOLATED_CLIENT_RELAY,
            1,
        ),
        (
            AuthorSubject::SYSTEM,
            CommitUnitTrust::Session,
            FEATURE_SCOPE_ISOLATED_CLIENT_RELAY,
            1,
        ),
        (
            AuthorSubject::reserved(AuthorSubject::ANONYMOUS_ISSUER, "bob").unwrap(),
            CommitUnitTrust::Session,
            FEATURE_SCOPE_ISOLATED_CLIENT_RELAY,
            1,
        ),
        (alice(), CommitUnitTrust::Session, 0, 1),
        (
            alice(),
            CommitUnitTrust::Session,
            FEATURE_SCOPE_ISOLATED_CLIENT_RELAY,
            0,
        ),
    ] {
        assert!(
            runtime
                .open_with_session_context(
                    identity,
                    claims("alice"),
                    trust,
                    features,
                    None,
                    ServerLinkAdmission::ScopeIsolatedClientRelay {
                        admission_epoch: epoch
                    }
                )
                .await
                .is_err()
        );
    }
    runtime.shutdown().await.unwrap();
}

/// Alice's two browser tabs and Bob connect through the real WebSocket route.
/// The relay preserves each origin receipt and cannot forge another author,
/// override authenticated claims, or borrow backend-only read capabilities.
/// Raw messages are used only to inject these invalid authority requests.
#[tokio::test]
async fn websocket_private_owner_keeps_session_permissions_and_origin_receipts() {
    let server = WebSocketServer::start(AppId::random(), StorageBackend::InMemory).await;
    let mut alice_socket = server.connect(alice(), "alice").await;
    let mut bob_socket = server.connect(bob(), "bob").await;
    let mut forged_author = upload(22, bob(), "alice");
    if let SyncMessage::Commit { author, .. } = &mut forged_author {
        *author = Some(bob().canonical().into());
    }
    let mut forged_claims = upload(23, alice(), "bob");
    if let SyncMessage::Commit { session, .. } = &mut forged_claims {
        *session = Some(claims("bob"));
    }
    let writes = vec![
        upload(20, alice(), "alice"),
        upload(21, alice(), "alice"),
        forged_author,
        forged_claims,
    ];
    let original: BTreeMap<_, _> = writes
        .iter()
        .filter_map(|message| match message {
            SyncMessage::Commit { node, tx, .. } => Some((node[0], *tx)),
            _ => None,
        })
        .collect();
    ws_send(&mut alice_socket, writes).await;
    let mut fates = BTreeMap::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while fates.len() < 4 {
            for message in ws_messages(&mut alice_socket).await {
                if let SyncMessage::OriginFateV2 {
                    node, tx, result, ..
                } = message
                {
                    assert_eq!(tx, original[&node[0]]);
                    fates.insert(node[0], result);
                }
            }
        }
    })
    .await
    .unwrap();
    for node in [20, 21] {
        assert!(fates[&node].is_ok(), "{fates:?}");
    }
    for node in [22, 23] {
        assert!(fates[&node].is_err(), "{fates:?}");
    }
    ws_send(&mut bob_socket, vec![upload(24, bob(), "bob")]).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if ws_messages(&mut bob_socket).await.iter().any(|message| matches!(message, SyncMessage::OriginFateV2 {node,result:Ok(_),..} if node[0]==24)) { break; }
        }
    }).await.unwrap();
    for (socket, expected) in [(&mut alice_socket, 2), (&mut bob_socket, 1)] {
        ws_send(
            socket,
            vec![SyncMessage::AcceptedFetchV1 {
                id: 100,
                query: Query::from("notes"),
                binding: BTreeMap::new(),
                at: None,
                include_deleted: false,
                scope: None,
            }],
        )
        .await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                for message in ws_messages(socket).await {
                    match message {
                        SyncMessage::AcceptedAnswerV1 { id: 100, rows, .. } => {
                            assert_eq!(rows.len(), expected);
                            return;
                        }
                        SyncMessage::Refused { message, .. } => {
                            panic!("authorised read failed: {message}")
                        }
                        _ => {}
                    }
                }
            }
        })
        .await
        .unwrap();
    }
    ws_send(
        &mut alice_socket,
        vec![
            SyncMessage::AcceptedFetchV1 {
                id: 101,
                query: Query::from("notes"),
                binding: BTreeMap::new(),
                at: None,
                include_deleted: false,
                scope: Some((bob().canonical().into(), claims("bob"))),
            },
            SyncMessage::AcceptedFetchV1 {
                id: 102,
                query: Query::from("notes"),
                binding: BTreeMap::new(),
                at: None,
                include_deleted: false,
                scope: Some((alice().canonical().into(), claims("bob"))),
            },
            SyncMessage::ScopedFetchV1 {
                id: 103,
                query: Query::from("notes"),
                binding: BTreeMap::new(),
                include_deleted: false,
                author: alice().canonical().into(),
                claims: claims("alice"),
            },
            SyncMessage::AcceptedOverlayCheckV1 {
                id: 104,
                query: Query::from("notes"),
                binding: BTreeMap::new(),
                through: 0,
                include_deleted: false,
                author: alice().canonical().into(),
                claims: claims("alice"),
                ops: vec![],
            },
        ],
    )
    .await;
    let mut refused = std::collections::BTreeSet::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while refused.len() < 4 {
            for message in ws_messages(&mut alice_socket).await {
                match message {
                    SyncMessage::Refused { id, .. } => {
                        refused.insert(id);
                    }
                    SyncMessage::AcceptedAnswerV1 { .. }
                    | SyncMessage::Answer { .. }
                    | SyncMessage::AcceptedOverlayCheckedV1 { .. } => {
                        panic!("forged request answered: {message:?}")
                    }
                    _ => {}
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(refused, [101, 102, 103, 104].into());
    alice_socket.close(None).await.unwrap();
    bob_socket.close(None).await.unwrap();
    server.shutdown().await;
}

/// Alice retries the same two tabs' uploads after a persistent authority
/// restart. Their original receipts survive, and retrying creates no new rows.
#[tokio::test]
async fn websocket_private_owner_replays_original_receipts_after_authority_restart() {
    let directory = tempfile::tempdir().unwrap();
    let app_id = AppId::random();
    let storage = StorageBackend::Persistent {
        path: directory.path().to_path_buf(),
    };
    let uploads = vec![upload(30, alice(), "alice"), upload(31, alice(), "alice")];
    let mut first_receipts = BTreeMap::new();
    for round in 0..2 {
        let server = WebSocketServer::start(app_id, storage.clone()).await;
        let mut socket = server.connect(alice(), "alice").await;
        ws_send(&mut socket, uploads.clone()).await;
        let mut receipts = BTreeMap::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while receipts.len() < 2 {
                for mut message in ws_messages(&mut socket).await {
                    if let SyncMessage::OriginFateV2 {
                        node,
                        ref result,
                        ref mut held,
                        ..
                    } = message
                    {
                        assert!(result.is_ok(), "{result:?}");
                        if round == 1 {
                            assert!(
                                held.is_empty(),
                                "a new connection has no previous echo hints"
                            );
                        }
                        // Echo hints belong to the current connection. Every
                        // other authority receipt byte must survive restart.
                        held.clear();
                        receipts.insert(node, jazz::db::encode_message(&message).unwrap());
                    }
                }
            }
        })
        .await
        .unwrap();
        if round == 0 {
            first_receipts = receipts;
        } else {
            assert_eq!(receipts, first_receipts);
        }
        ws_send(
            &mut socket,
            vec![SyncMessage::AcceptedFetchV1 {
                id: 100,
                query: Query::from("notes"),
                binding: BTreeMap::new(),
                at: None,
                include_deleted: false,
                scope: None,
            }],
        )
        .await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                for message in ws_messages(&mut socket).await {
                    if let SyncMessage::AcceptedAnswerV1 { id: 100, rows, .. } = message {
                        assert_eq!(rows.len(), 2);
                        return;
                    }
                }
            }
        })
        .await
        .unwrap();
        socket.close(None).await.unwrap();
        server.shutdown().await;
    }
}
