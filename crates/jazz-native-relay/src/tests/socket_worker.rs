//! The scope-owned upstream socket worker against a scripted connector:
//! which failures a foreground tick reports, when the worker stops, and how a
//! stopped worker is replaced.
//!
//! Host-level rather than through JSI: the behaviour is the relay host's
//! worker lifecycle and tick gate, and only a scripted connector can hold the
//! worker at a given attempt deterministically.
use super::*;
use jazz::tools::native_transport_connector::{
    ConnectedNativeTransport, NativeTransportError, NativeTransportRequest, NativeTransportTerminal,
};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::wire::{TransportError, WireTransport};
use std::collections::VecDeque;
use std::thread;
use std::time::Duration;

fn schema() -> JazzSchema {
    let allow = PolicyExpr::True;
    let policies = TablePolicies::new()
        .with_select(allow.clone())
        .with_insert(allow.clone())
        .with_update(Some(allow.clone()), allow.clone())
        .with_delete(allow);
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    )
    .unwrap()
}

struct IdleWire;

impl WireTransport for IdleWire {
    fn send_frame(&mut self, _frame: Vec<u8>) -> Result<(), TransportError> {
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        None
    }
}

/// One scripted connection attempt of the host-owned socket worker.
enum Step {
    /// Connect, then lose the established transport non-retryably once the
    /// test releases it.
    ConnectThenTerminal(tokio::sync::oneshot::Receiver<()>),
    /// Hold the attempt until released, then connect and stay connected.
    ConnectWhenReleased(tokio::sync::oneshot::Receiver<()>),
    /// Fail the attempt non-retryably for a reason other than the credential.
    Refuse,
    /// Refuse the attempt's credential, as Core refuses an expired bearer
    /// (`AuthFailed`, `WireRetry::Never`).
    RejectAuth,
}

/// Plays [`Step`]s in order and records each attempt's bearer; further
/// attempts never complete.
struct ScriptedConnector {
    steps: Mutex<VecDeque<Step>>,
    bearers: Mutex<Vec<String>>,
}

impl ScriptedConnector {
    fn new(steps: impl IntoIterator<Item = Step>) -> Arc<Self> {
        Arc::new(Self {
            steps: Mutex::new(steps.into_iter().collect()),
            bearers: Mutex::new(Vec::new()),
        })
    }

    fn bearers(&self) -> Vec<String> {
        self.bearers.lock().unwrap().clone()
    }
}

impl NativeTransportConnector for ScriptedConnector {
    fn connect(
        &self,
        request: NativeTransportRequest,
    ) -> jazz::tools::native_transport_connector::NativeTransportFuture {
        let step = self.steps.lock().unwrap().pop_front();
        self.bearers
            .lock()
            .unwrap()
            .push(request.auth.jwt_token.clone().unwrap_or_default());
        Box::pin(async move {
            let connected = |terminal| ConnectedNativeTransport {
                transport: Box::new(IdleWire),
                protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                features: jazz::wire::current_wire_features(),
                terminal,
            };
            match step {
                Some(Step::ConnectThenTerminal(release)) => Ok(connected(Box::pin(async move {
                    let _ = release.await;
                    NativeTransportTerminal::Failed(NativeTransportError::Terminal(
                        "websocket peer sent a non-binary wire batch".into(),
                    ))
                }))),
                Some(Step::ConnectWhenReleased(release)) => {
                    let _ = release.await;
                    Ok(connected(Box::pin(std::future::pending())))
                }
                Some(Step::Refuse) => Err(NativeTransportError::Terminal(
                    "websocket rejected: unsupported feature".into(),
                )),
                Some(Step::RejectAuth) => Err(NativeTransportError::AuthRejected(
                    "websocket rejected: AuthFailed: token expired".into(),
                )),
                None => std::future::pending().await,
            }
        })
    }
}

fn bearer(generation: u32) -> String {
    use base64::Engine;
    let claims = serde_json::json!({
        "iss": "https://issuer.example",
        "sub": "alice",
        "generation": generation,
    });
    format!(
        "x.{}.x",
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&claims).unwrap())
    )
}

/// Admit alice's private scope against a scripted upstream and open one
/// foreground on it. Returns the host, the admitted scope and alice's
/// foreground.
fn scripted_foreground(
    root: &std::path::Path,
    app_id: &str,
    connector: Arc<ScriptedConnector>,
) -> (NativeRelayHost, AdmissionCapability, u64) {
    let mut host = NativeRelayHost {
        socket_connector: Some(connector),
        ..NativeRelayHost::default()
    };
    let pending = host
        .begin_private_session(PrivateSessionSetupJson {
            server_url: "https://edge.example".to_owned(),
            app_id: app_id.to_owned(),
            jwt: bearer(1),
            storage_root: root.display().to_string(),
        })
        .unwrap();
    let admitted = host
        .attach_canonical_schema(
            pending,
            &serde_json::to_string(schema().public_schema()).unwrap(),
        )
        .unwrap();
    let foreground = host
        .open_foreground(admitted, DIRECT_FOREGROUND_RUNTIME_TOKEN)
        .unwrap();
    (host, admitted, foreground)
}

fn wait_for(host: &NativeRelayHost, what: &str, done: impl Fn(&NativeRelayHost) -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !done(host) {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting: {what}"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

/// (connected, auth_rejected) as alice's foreground reports them.
fn status(host: &mut NativeRelayHost, foreground: u64) -> (bool, Option<String>) {
    match host.foreground_connectivity(foreground, None).unwrap() {
        ForegroundDbCommandResponse::NativeConnectionStatus {
            connected,
            auth_rejected,
            ..
        } => (connected, auth_rejected),
        other => panic!("unexpected connectivity response: {other:?}"),
    }
}

fn connected(host: &NativeRelayHost, scope: &RelayScope) -> bool {
    host.private_scope_workers[scope].connected()
}

/// Losing an established upstream link that the worker is retrying is
/// connection state, not a foreground failure (#3630), even when the
/// transport classifies the loss as non-retryable. Alice's foreground keeps
/// ticking while the socket reports disconnected, and the worker's next
/// connection restores it.
///
/// ```text
/// alice ─open─► relay ══socket══► edge   (connected)
///                 │        ✗ terminal transport loss, worker retries
/// alice ─tick─► relay                    (Ok, connected=false)
///                 │ ══socket══► edge     (reconnected)
/// alice ─tick─► relay                    (Ok, connected=true)
/// ```
#[test]
fn retrying_upstream_failure_keeps_foreground_ticking() {
    let root = tempfile::tempdir().unwrap();
    let (fail_first, first_failure) = tokio::sync::oneshot::channel();
    let (reconnect, reconnected) = tokio::sync::oneshot::channel();
    let connector = ScriptedConnector::new([
        Step::ConnectThenTerminal(first_failure),
        Step::ConnectWhenReleased(reconnected),
    ]);
    let (mut host, _, alice) = scripted_foreground(root.path(), "retrying-upstream", connector);
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for(&host, "first connection", |host| connected(host, &scope));
    assert_eq!(host.tick_foreground(alice), Ok(()));

    fail_first.send(()).unwrap();
    wait_for(&host, "established link lost", |host| {
        !connected(host, &scope)
    });
    assert_eq!(status(&mut host, alice), (false, None));
    assert_eq!(
        host.tick_foreground(alice),
        Ok(()),
        "a retrying upstream failure must not fail a valid foreground tick"
    );

    reconnect.send(()).unwrap();
    wait_for(&host, "worker reconnects", |host| connected(host, &scope));
    assert_eq!(host.tick_foreground(alice), Ok(()));
}

/// A connection attempt the upstream refuses for another reason than the
/// credential (TLS, a proxy refusing the upgrade, a rejected session) is
/// reported to alice as a retrying upstream failure and retried: it can
/// clear without a new credential, and the next connection clears it.
/// #4337: it is connection state, so alice's ticks keep running meanwhile,
/// as on the web and Node transports.
///
/// ```text
/// alice ─open─► relay ══socket══✗ edge refuses   (worker retries)
/// alice ─tick─► relay                            (Ok, upstream failure, link Failed)
///                 ══socket══► edge               (connected)
/// alice ─tick─► relay                            (Ok, no failure)
/// ```
#[test]
fn a_refused_connection_is_surfaced_and_retried() {
    let root = tempfile::tempdir().unwrap();
    let (release, released) = tokio::sync::oneshot::channel();
    let connector = ScriptedConnector::new([Step::Refuse, Step::ConnectWhenReleased(released)]);
    let (mut host, _, alice) =
        scripted_foreground(root.path(), "refused-upstream", connector.clone());
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for(&host, "the refusal is surfaced", |host| {
        host.private_scope_terminal_error(&scope).is_some()
    });
    assert_eq!(
        host.tick_foreground(alice),
        Ok(()),
        "a refused connection must not fail local ticks"
    );
    let (auth_rejection, failure) = tick_events(&mut host, alice);
    assert_eq!(auth_rejection, None, "not a credential problem");
    let failure = failure.expect("the refusal is reported");
    assert!(failure.retrying, "{failure:?}");
    assert!(!host.private_scope_workers[&scope].terminally_stopped());
    host.sync_foreground_remote_link(alice);
    assert_eq!(
        host.foregrounds[&alice].remote_link_hint,
        Some(RemoteLinkHint::Failed),
        "reads must not wait on an upstream that refuses the connection"
    );

    release.send(()).unwrap();
    wait_for(&host, "the retried connection lands", |host| {
        connected(host, &scope)
    });
    assert_eq!(connector.bearers().len(), 2, "the same request was retried");
    assert!(host.private_scope_terminal_error(&scope).is_none());
    assert_eq!(tick_events(&mut host, alice), (None, None));
    assert_eq!(host.tick_foreground(alice), Ok(()));
}

/// #4248: alice's bearer expires while the app is in the background, so the
/// worker's reconnect after the link drops is refused. The worker stops
/// instead of resending the dead bearer forever, the status tells the host
/// the credential was refused (so it renews), and the renewed bearer
/// replaces the worker and clears the failure. #4296: a refused credential
/// is auth state, not a failed session, so alice's ticks keep running
/// meanwhile, as on the web and Node transports.
///
/// ```text
/// alice ─open─► relay ══socket(bearer 1)══► edge   (connected)
///                 │     ✗ link lost (backgrounded)
///                 │ ══socket(bearer 1)══✗ edge     AuthFailed: worker stops
/// alice ─tick─► relay                              (Ok, auth_rejected, link Failed)
/// alice ─renew(bearer 2)─► relay ══socket(bearer 2)══► edge
/// alice ─tick─► relay                              (Ok)
/// ```
#[test]
fn an_expired_bearer_stops_the_worker_and_a_renewed_one_recovers() {
    let root = tempfile::tempdir().unwrap();
    let (drop_link, link_dropped) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    release.send(()).unwrap();
    let connector = ScriptedConnector::new([
        Step::ConnectThenTerminal(link_dropped),
        Step::RejectAuth,
        Step::ConnectWhenReleased(released),
    ]);
    let (mut host, admitted, alice) =
        scripted_foreground(root.path(), "expired-bearer", connector.clone());
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for(&host, "first connection", |host| connected(host, &scope));
    assert_eq!(host.tick_foreground(alice), Ok(()));

    drop_link.send(()).unwrap();
    wait_for(&host, "the refused worker stops", |host| {
        host.private_scope_workers[&scope].terminally_stopped()
    });
    assert_eq!(
        host.tick_foreground(alice),
        Ok(()),
        "a refused credential must not fail local ticks"
    );
    let (is_connected, rejected) = status(&mut host, alice);
    assert!(!is_connected);
    host.sync_foreground_remote_link(alice);
    assert_eq!(
        host.foregrounds[&alice].remote_link_hint,
        Some(RemoteLinkHint::Failed),
        "reads must not wait on an upstream that refused the credential"
    );
    assert!(
        rejected
            .as_deref()
            .is_some_and(|reason| reason.contains("token expired")),
        "the status names the refused credential: {rejected:?}"
    );
    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        connector.bearers(),
        [bearer(1), bearer(1)],
        "the dead bearer is sent once more, not forever"
    );

    // The account renewal path (`refreshAccountSession`) promotes the new
    // bearer. Called directly: driving it through the C ABI needs a live
    // account registry, and the promotion is what replaces the worker.
    host.account_session_owners.insert(
        admitted,
        account_session::AccountSessionOwner {
            runtime_token: DIRECT_FOREGROUND_RUNTIME_TOKEN,
            app_id: "expired-bearer".to_owned(),
        },
    );
    host.promote_account_scope_snapshot(admitted, &bearer(2), BTreeMap::new())
        .unwrap();
    wait_for(&host, "the renewed worker connects", |host| {
        connected(host, &scope)
    });
    assert_eq!(connector.bearers().last(), Some(&bearer(2)));
    assert_eq!(status(&mut host, alice), (true, None));
    assert_eq!(host.tick_foreground(alice), Ok(()));
}

/// alice's drained tick events as the host reports them after a tick:
/// (credential refusal, other upstream failure).
fn tick_events(
    host: &mut NativeRelayHost,
    foreground: u64,
) -> (Option<NativeAuthRejection>, Option<NativeUpstreamFailure>) {
    match host
        .execute_foreground_command(foreground, ForegroundDbCommandRequest::DrainTickEvents)
        .unwrap()
    {
        ForegroundDbCommandResponse::TickEvents {
            events_json,
            auth_rejection,
            upstream_failure,
        } => {
            assert_eq!(events_json, "[]");
            (auth_rejection, upstream_failure)
        }
        other => panic!("unexpected tick events: {other:?}"),
    }
}

fn tick_auth_rejection(host: &mut NativeRelayHost, foreground: u64) -> Option<NativeAuthRejection> {
    let (auth_rejection, upstream_failure) = tick_events(host, foreground);
    assert_eq!(upstream_failure, None, "a refusal is not another failure");
    auth_rejection
}

/// #4296: the host reports each refusal once. A renewed bearer that is
/// refused for the same reason is a new refusal, so the tick events give it
/// a new sequence and the host renews again instead of waiting forever.
///
/// ```text
/// alice ─open─► relay ══socket(bearer 1)══► edge   (connected)
///                 │     ✗ link lost
///                 │ ══socket(bearer 1)══✗ edge     refusal #n
/// alice ─renew(bearer 2)─► relay ══socket(bearer 2)══✗ edge   refusal #m > n
/// ```
#[test]
fn a_renewed_bearer_refused_for_the_same_reason_is_a_new_refusal() {
    let root = tempfile::tempdir().unwrap();
    let (drop_link, link_dropped) = tokio::sync::oneshot::channel();
    let connector = ScriptedConnector::new([
        Step::ConnectThenTerminal(link_dropped),
        Step::RejectAuth,
        Step::RejectAuth,
    ]);
    let (mut host, admitted, alice) =
        scripted_foreground(root.path(), "refused-twice", connector.clone());
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for(&host, "first connection", |host| connected(host, &scope));
    assert_eq!(tick_auth_rejection(&mut host, alice), None);

    drop_link.send(()).unwrap();
    wait_for(&host, "the refused worker stops", |host| {
        host.private_scope_workers[&scope].terminally_stopped()
    });
    let first = tick_auth_rejection(&mut host, alice).expect("the first refusal");
    assert!(first.reason.contains("token expired"), "{first:?}");
    assert_eq!(
        tick_auth_rejection(&mut host, alice),
        Some(first.clone()),
        "a refusal keeps its sequence until it clears"
    );

    host.account_session_owners.insert(
        admitted,
        account_session::AccountSessionOwner {
            runtime_token: DIRECT_FOREGROUND_RUNTIME_TOKEN,
            app_id: "refused-twice".to_owned(),
        },
    );
    host.promote_account_scope_snapshot(admitted, &bearer(2), BTreeMap::new())
        .unwrap();
    wait_for(&host, "the renewed worker is refused", |host| {
        host.private_scope_auth_rejection(&scope)
            .is_some_and(|rejection| rejection.sequence != first.sequence)
    });
    let second = tick_auth_rejection(&mut host, alice).expect("the second refusal");
    assert_eq!(second.reason, first.reason);
    assert!(
        second.sequence > first.sequence,
        "{first:?} then {second:?}"
    );
    assert_eq!(connector.bearers(), [bearer(1), bearer(1), bearer(2)]);
    assert_eq!(host.tick_foreground(alice), Ok(()));
}

/// No actors: pin the additive tick-events command and its response bytes
/// independently of the TypeScript decoder. Existing ordinals stay fixed.
#[test]
fn tick_events_command_and_response_have_pinned_bytes() {
    assert_eq!(
        postcard::to_allocvec(&ForegroundDbCommandRequest::DrainTickEvents).unwrap(),
        vec![35]
    );
    let response = ForegroundDbCommandResponse::TickEvents {
        events_json: "[]".to_owned(),
        auth_rejection: Some(NativeAuthRejection {
            sequence: 300,
            reason: "no".to_owned(),
        }),
        upstream_failure: Some(NativeUpstreamFailure {
            sequence: 2,
            reason: "tls".to_owned(),
            retrying: true,
        }),
    };
    assert_eq!(
        postcard::to_allocvec(&response).unwrap(),
        vec![
            26, 2, b'[', b']', 1, 0xac, 0x02, 2, b'n', b'o', 1, 2, 3, b't', b'l', b's', 1
        ]
    );
    let response = ForegroundDbCommandResponse::TickEvents {
        events_json: "[]".to_owned(),
        auth_rejection: None,
        upstream_failure: None,
    };
    assert_eq!(
        postcard::to_allocvec(&response).unwrap(),
        vec![26, 2, b'[', b']', 0, 0]
    );
}
