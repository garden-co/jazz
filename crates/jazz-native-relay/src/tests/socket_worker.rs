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
    /// Fail the attempt as an unreachable server does, retryably.
    Unreachable,
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
                Some(Step::Unreachable) => Err(NativeTransportError::Retryable(
                    "failed to connect websocket: connection refused".into(),
                )),
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
    scripted_foreground_on(
        NativeRelayHost {
            socket_connector: Some(connector),
            ..NativeRelayHost::default()
        },
        root,
        app_id,
    )
}

/// [`scripted_foreground`] on a host the test configured further.
fn scripted_foreground_on(
    mut host: NativeRelayHost,
    root: &std::path::Path,
    app_id: &str,
) -> (NativeRelayHost, AdmissionCapability, u64) {
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

/// A host whose network path the test reports, on Wi-Fi to begin with,
/// with retry waits long enough that only the path can cut them short.
fn host_on_path(
    connector: Arc<ScriptedConnector>,
) -> (NativeRelayHost, tokio::sync::watch::Sender<NetworkPath>) {
    let (path, network_path) = tokio::sync::watch::channel(NetworkPath {
        satisfied: true,
        interfaces: NETWORK_INTERFACE_WIFI,
    });
    let host = NativeRelayHost {
        socket_connector: Some(connector),
        network_path: Some(network_path),
        link_policy: Some(jazz::link::LinkPolicy {
            retry_initial_ms: 20_000,
            retry_unreported_max_ms: 40_000,
            retry_max_ms: 40_000,
            ..jazz::link::LinkPolicy::default()
        }),
        ..NativeRelayHost::default()
    };
    (host, path)
}

fn wait_within(
    host: &NativeRelayHost,
    within: Duration,
    what: &str,
    done: impl Fn(&NativeRelayHost) -> bool,
) {
    let deadline = std::time::Instant::now() + within;
    while !done(host) {
        assert!(
            std::time::Instant::now() < deadline,
            "not within {within:?}: {what}"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

/// The device loses its network (Wi-Fi switched off, no cellular). The
/// socket is dead, but nothing on it says so: no close, no error, only
/// silence. The operating system says so at once, and the worker takes its
/// word instead of waiting out the transport's silence deadline.
///
/// ```text
/// alice ─open─► relay ══socket══► edge     (connected, on Wi-Fi)
/// os: path unsatisfied
///                 relay ══✗                (dropped at once, connected=false)
/// ```
#[test]
fn a_lost_network_path_drops_the_live_link_at_once() {
    let root = tempfile::tempdir().unwrap();
    let (release, released) = tokio::sync::oneshot::channel();
    release.send(()).unwrap();
    let connector = ScriptedConnector::new([Step::ConnectWhenReleased(released)]);
    let (host, path) = host_on_path(connector);
    let (host, _, alice) = scripted_foreground_on(host, root.path(), "path-lost");
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for(&host, "first connection", |host| connected(host, &scope));

    path.send_replace(NetworkPath {
        satisfied: false,
        interfaces: 0,
    });
    wait_within(
        &host,
        Duration::from_secs(1),
        "the dead link is dropped",
        |host| !connected(host, &scope),
    );
}

/// A link opened on Wi-Fi is bound to it. Cellular coming up alongside
/// changes nothing; Wi-Fi going away leaves the socket on an interface the
/// device no longer routes through, so the worker reconnects at once.
///
/// ```text
/// alice ─open─► relay ══socket══► edge     (on Wi-Fi)
/// os: Wi-Fi + cellular                     (kept)
/// os: cellular only                        (dropped, reconnected on cellular)
/// ```
#[test]
fn a_path_leaving_the_links_interface_reconnects_at_once() {
    let root = tempfile::tempdir().unwrap();
    let (first, first_released) = tokio::sync::oneshot::channel();
    let (second, second_released) = tokio::sync::oneshot::channel();
    first.send(()).unwrap();
    second.send(()).unwrap();
    let connector = ScriptedConnector::new([
        Step::ConnectWhenReleased(first_released),
        Step::ConnectWhenReleased(second_released),
    ]);
    let (host, path) = host_on_path(connector.clone());
    let (host, _, alice) = scripted_foreground_on(host, root.path(), "path-moved");
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for(&host, "first connection", |host| connected(host, &scope));

    path.send_replace(NetworkPath {
        satisfied: true,
        interfaces: NETWORK_INTERFACE_WIFI | NETWORK_INTERFACE_CELLULAR,
    });
    thread::sleep(Duration::from_millis(300));
    assert!(
        connected(&host, &scope),
        "an added interface keeps the link"
    );
    assert_eq!(connector.bearers().len(), 1);

    path.send_replace(NetworkPath {
        satisfied: true,
        interfaces: NETWORK_INTERFACE_CELLULAR,
    });
    wait_within(
        &host,
        Duration::from_secs(2),
        "reconnected on cellular",
        |_| connector.bearers().len() == 2,
    );
    wait_within(
        &host,
        Duration::from_secs(2),
        "the new link is up",
        |host| connected(host, &scope),
    );
}

/// The link dropped while the device had no network, and the worker is
/// waiting out its backoff. The backoff spares a server that is down; the
/// device's path coming back says nothing about the server, but it is the
/// one moment a retry is most likely to land, so the worker retries at once.
///
/// ```text
/// alice ─open─► relay ══socket══► edge     (connected)
/// os: path unsatisfied; link lost          (backoff >= 10 s)
/// os: path satisfied                       (retry at once, connected)
/// ```
#[test]
fn a_returning_network_path_retries_without_waiting_out_the_backoff() {
    let root = tempfile::tempdir().unwrap();
    let (drop_link, link_dropped) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    release.send(()).unwrap();
    let connector = ScriptedConnector::new([
        Step::ConnectThenTerminal(link_dropped),
        Step::ConnectWhenReleased(released),
    ]);
    let (host, path) = host_on_path(connector.clone());
    let (host, _, alice) = scripted_foreground_on(host, root.path(), "path-back");
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for(&host, "first connection", |host| connected(host, &scope));

    path.send_replace(NetworkPath {
        satisfied: false,
        interfaces: 0,
    });
    let _ = drop_link.send(()); // the path loss may already have dropped the link
    wait_for(&host, "the link is lost", |host| !connected(host, &scope));
    thread::sleep(Duration::from_millis(200));
    assert_eq!(connector.bearers().len(), 1, "waiting out the backoff");

    path.send_replace(NetworkPath {
        satisfied: true,
        interfaces: NETWORK_INTERFACE_WIFI,
    });
    wait_within(&host, Duration::from_secs(2), "retried at once", |host| {
        connected(host, &scope)
    });
    assert_eq!(connector.bearers().len(), 2);
}

/// The monitor can lag: a link may get through while the device still
/// reports no path. Once the device reports one, the link is watched against
/// it, so it is dropped when that interface goes, like any other.
///
/// ```text
/// os: path unsatisfied (lagging)
/// alice ─open─► relay ══socket══► edge     (connected anyway)
/// os: Wi-Fi                                (kept)
/// os: cellular only                        (dropped, reconnected)
/// ```
#[test]
fn a_link_admitted_while_the_monitor_lagged_is_watched_on_the_path_it_reports_next() {
    let root = tempfile::tempdir().unwrap();
    let (first, first_released) = tokio::sync::oneshot::channel();
    let (second, second_released) = tokio::sync::oneshot::channel();
    first.send(()).unwrap();
    second.send(()).unwrap();
    let connector = ScriptedConnector::new([
        Step::ConnectWhenReleased(first_released),
        Step::ConnectWhenReleased(second_released),
    ]);
    let (host, path) = host_on_path(connector.clone());
    path.send_replace(NetworkPath {
        satisfied: false,
        interfaces: 0,
    });
    let (host, _, alice) = scripted_foreground_on(host, root.path(), "path-lagged");
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for(&host, "connected while the monitor lagged", |host| {
        connected(host, &scope)
    });

    path.send_replace(NetworkPath {
        satisfied: true,
        interfaces: NETWORK_INTERFACE_WIFI,
    });
    thread::sleep(Duration::from_millis(300));
    assert!(connected(&host, &scope), "the live link was dropped");
    assert_eq!(connector.bearers().len(), 1);

    path.send_replace(NetworkPath {
        satisfied: true,
        interfaces: NETWORK_INTERFACE_CELLULAR,
    });
    wait_within(
        &host,
        Duration::from_secs(2),
        "reconnected on cellular",
        |_| connector.bearers().len() == 2,
    );
}

/// The path leaves cellular the moment Wi-Fi is preferred, but cellular
/// stays up and keeps carrying the sockets opened on it: a live link there is
/// kept, not dropped for a reconnect it does not need.
///
/// ```text
/// alice ─open─► relay ══socket══► edge     (on cellular)
/// os: Wi-Fi only                           (kept)
/// ```
#[test]
fn a_link_on_cellular_is_kept_when_wifi_takes_over_the_path() {
    let root = tempfile::tempdir().unwrap();
    let (release, released) = tokio::sync::oneshot::channel();
    release.send(()).unwrap();
    let connector = ScriptedConnector::new([Step::ConnectWhenReleased(released)]);
    let (host, path) = host_on_path(connector.clone());
    path.send_replace(NetworkPath {
        satisfied: true,
        interfaces: NETWORK_INTERFACE_CELLULAR,
    });
    let (host, _, alice) = scripted_foreground_on(host, root.path(), "path-upgraded");
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for(&host, "first connection", |host| connected(host, &scope));

    path.send_replace(NetworkPath {
        satisfied: true,
        interfaces: NETWORK_INTERFACE_WIFI,
    });
    thread::sleep(Duration::from_millis(300));
    assert!(connected(&host, &scope), "the cellular link was dropped");
    assert_eq!(connector.bearers().len(), 1);
}

/// An attempt dialled while the device had no network can hang until its
/// timeout. When the path comes back, a fresh attempt is the one that lands.
///
/// ```text
/// os: path unsatisfied
/// alice ─open─► relay ══socket…            (attempt hangs)
/// os: path satisfied                       (abandoned, retried, connected)
/// ```
#[test]
fn an_attempt_made_without_a_network_is_retried_when_the_network_returns() {
    let root = tempfile::tempdir().unwrap();
    let (_hang, hung) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    release.send(()).unwrap();
    let connector = ScriptedConnector::new([
        Step::ConnectWhenReleased(hung),
        Step::ConnectWhenReleased(released),
    ]);
    let (host, path) = host_on_path(connector.clone());
    path.send_replace(NetworkPath {
        satisfied: false,
        interfaces: 0,
    });
    let (host, _, alice) = scripted_foreground_on(host, root.path(), "path-returned");
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for(&host, "the first attempt", |_| {
        connector.bearers().len() == 1
    });
    thread::sleep(Duration::from_millis(100));

    path.send_replace(NetworkPath {
        satisfied: true,
        interfaces: NETWORK_INTERFACE_WIFI,
    });
    wait_within(
        &host,
        Duration::from_secs(2),
        "connected on the returned path",
        |host| connected(host, &scope),
    );
    assert_eq!(connector.bearers().len(), 2);
}

/// A path that keeps changing while the server is unreachable cuts each
/// backoff short, but not below a floor: a flapping network does not become
/// a reconnect loop.
#[test]
fn a_flapping_path_does_not_become_a_reconnect_loop() {
    let root = tempfile::tempdir().unwrap();
    let connector = ScriptedConnector::new(std::iter::repeat_with(|| Step::Unreachable).take(200));
    let (host, path) = host_on_path(connector.clone());
    let (host, _, alice) = scripted_foreground_on(host, root.path(), "path-flapping");
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for(&host, "the first attempt", |_| {
        connector.bearers().len() == 1
    });

    let flapping = std::time::Instant::now();
    let mut on_wifi = true;
    while flapping.elapsed() < Duration::from_millis(2_500) {
        on_wifi = !on_wifi;
        path.send_replace(NetworkPath {
            satisfied: true,
            interfaces: if on_wifi {
                NETWORK_INTERFACE_WIFI
            } else {
                NETWORK_INTERFACE_CELLULAR
            },
        });
        thread::sleep(Duration::from_millis(20));
    }
    let attempts = connector.bearers().len();
    assert!(
        (2..=4).contains(&attempts),
        "{attempts} attempts in 2.5 s of a flapping path"
    );
    assert!(!connected(&host, &scope));
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
            code: None,
        }),
    };
    assert_eq!(
        postcard::to_allocvec(&response).unwrap(),
        vec![
            26, 2, b'[', b']', 1, 0xac, 0x02, 2, b'n', b'o', 1, 2, 3, b't', b'l', b's', 1, 0
        ]
    );
    // ABI 6: a refusal's failure ends with its error code.
    let response = ForegroundDbCommandResponse::TickEvents {
        events_json: "[]".to_owned(),
        auth_rejection: None,
        upstream_failure: Some(NativeUpstreamFailure {
            sequence: 3,
            reason: "no".to_owned(),
            retrying: false,
            code: Some("authority_changed".to_owned()),
        }),
    };
    let mut expected = vec![26, 2, b'[', b']', 0, 1, 3, 2, b'n', b'o', 0, 1, 17];
    expected.extend_from_slice(b"authority_changed");
    assert_eq!(postcard::to_allocvec(&response).unwrap(), expected);
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

/// A core with schema history (a root version, then a head) stores a column
/// where the version that introduced it put it. A replica attached with the
/// head alone would name it after the head instead, and read nothing from
/// that core. So the attach carries the app's migrations beside its tables:
///
/// ```
/// app ─{tables, migrations}─► relay store: same column     as core
/// app ─{tables}────────────►  relay store: another column  than core
/// ```
#[test]
fn an_attached_schema_stores_columns_where_its_shipped_migrations_put_them() {
    use jazz::tools::deployment::published::with_client_migrations;
    use jazz::tools::{Schema, SchemaHash};
    let todos = |column: &str| -> Schema {
        let allow = PolicyExpr::True;
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column(column, ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(allow.clone())
                            .with_insert(allow.clone())
                            .with_update(Some(allow.clone()), allow.clone())
                            .with_delete(allow),
                    ),
            )
            .build()
    };
    let (root_schema, head) = (todos("title"), todos("name"));
    let (root_hash, head_hash) = (
        SchemaHash::compute(&root_schema).to_string(),
        SchemaHash::compute(&head).to_string(),
    );
    let bundle = serde_json::json!({
        "schemas": [
            {"hash": root_hash, "schema": root_schema},
            {"hash": head_hash, "schema": head},
        ],
        "migrations": [{
            "fromHash": root_hash,
            "toHash": head_hash,
            "forward": [{
                "table": "todos",
                "operations": [{"type": "rename", "column": "title", "value": "name"}],
            }],
        }],
    });
    let head_schema = JazzSchema::new(&head).unwrap();
    let stored_column = |path: &std::path::Path| -> String {
        rusqlite::Connection::open(path)
            .unwrap()
            .query_row(
                "SELECT group_concat(name) FROM pragma_table_info('todos') \
                 WHERE name LIKE 'title%' OR name LIKE 'name%'",
                [],
                |row| row.get(0),
            )
            .unwrap()
    };
    let identity = DbIdentity {
        node: jazz::ids::NodeUuid::from_bytes([0x5a; 16]),
        author: jazz::ids::AuthorSubject::for_test_bytes([0x5a; 16]),
    };
    let core_dir = tempfile::tempdir().unwrap();
    let core_path = core_dir.path().join("core.sqlite");
    let core = jazz::db::block_on(jazz::db::Db::open_at(
        with_client_migrations(
            jazz::db::DbConfig::new(head_schema.clone(), identity),
            Some(&serde_json::to_vec(&bundle).unwrap()),
        )
        .unwrap(),
        &core_path,
        jazz::db::FileOptions::default(),
    ))
    .unwrap();
    let on_core = stored_column(&core_path);
    drop(core);

    let attach = |payload: serde_json::Value, app_id: &str, root: &std::path::Path| {
        let (_release, never) = tokio::sync::oneshot::channel();
        let mut host = NativeRelayHost {
            socket_connector: Some(ScriptedConnector::new([Step::ConnectWhenReleased(never)])),
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
            .attach_canonical_schema(pending, &payload.to_string())
            .unwrap();
        host.open_foreground(admitted, DIRECT_FOREGROUND_RUNTIME_TOKEN)
            .unwrap();
        stored_column(&host.admitted_scopes[&admitted].config.sqlite_path)
    };
    let tables = serde_json::to_value(head_schema.public_schema()).unwrap();
    let mut shipped = tables.clone();
    shipped["migrations"] = bundle;

    let root = tempfile::tempdir().unwrap();
    assert_eq!(attach(shipped, "with-history", root.path()), on_core);
    assert_ne!(attach(tables, "head-only", root.path()), on_core);
}

/// A bundle the relay cannot lower refuses the attach: the store must not open
/// with columns stored where the authority does not keep them.
#[test]
fn an_attached_schema_with_malformed_migrations_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let (_release, never) = tokio::sync::oneshot::channel();
    let mut host = NativeRelayHost {
        socket_connector: Some(ScriptedConnector::new([Step::ConnectWhenReleased(never)])),
        ..NativeRelayHost::default()
    };
    let pending = host
        .begin_private_session(PrivateSessionSetupJson {
            server_url: "https://edge.example".to_owned(),
            app_id: "malformed-migrations".to_owned(),
            jwt: bearer(1),
            storage_root: root.path().display().to_string(),
        })
        .unwrap();
    let mut payload = serde_json::to_value(schema().public_schema()).unwrap();
    payload["migrations"] = serde_json::json!({"schemas": "nope"});
    assert!(matches!(
        host.attach_canonical_schema(pending, &payload.to_string()),
        Err(JazzNativeRelayStatus::LifecycleFailure)
    ));
}

/// When the relay's database refuses its upstream (it cannot mirror the
/// server's catalogue, say), the socket worker stops instead of keeping a
/// connection nothing syncs over, and alice hears why as a non-retrying
/// upstream failure. Before, the socket stayed open and nothing reported
/// anything.
///
/// Lower-level than the other receipts: a scripted connector carries no
/// server, so the test hands the relay owner a refusal, as its drive does
/// with `Link::upstream_refusal()`; the host test
/// `a_relay_attached_without_the_apps_migrations_stops_its_refused_upstream`
/// covers the owner's half against a real refusing server.
///
/// ```text
/// alice ─open─► relay ══socket══► edge      (connected)
///                 │ relay refuses its upstream
///                 ══socket══✗               (worker stops, no retry)
/// alice ─tick─► relay                       (Ok, failure: refused, not retrying)
/// ```
#[test]
fn a_refused_upstream_stops_the_worker_and_is_reported() {
    let root = tempfile::tempdir().unwrap();
    let (release, released) = tokio::sync::oneshot::channel();
    release.send(()).unwrap();
    let connector = ScriptedConnector::new([Step::ConnectWhenReleased(released)]);
    let (mut host, _, alice) =
        scripted_foreground(root.path(), "refused-catalogue", connector.clone());
    let scope = host.foregrounds[&alice].scope.clone();
    wait_for(&host, "the connection lands", |host| {
        connected(host, &scope)
    });

    let relay = host
        .relays
        .values()
        .find(|opened| opened.scope == scope)
        .expect("alice's relay is open")
        .relay
        .clone();
    relay
        .run(|worker| {
            worker.refuse_upstream(&jazz::db::Error::new(
                jazz::db::ErrorCode::AuthorityChanged,
                "the upstream serves another authority history",
            ));
            Ok(())
        })
        .unwrap();

    wait_for(&host, "the worker stops", |host| {
        host.private_scope_workers[&scope].terminally_stopped()
    });
    assert_eq!(
        host.tick_foreground(alice),
        Ok(()),
        "a refused upstream must not fail local ticks"
    );
    let (auth_rejection, failure) = tick_events(&mut host, alice);
    assert_eq!(auth_rejection, None, "not a credential problem");
    let failure = failure.expect("the refusal is reported");
    assert!(!failure.retrying, "a refusal is not retried: {failure:?}");
    assert!(failure.reason.contains("refused"), "{failure:?}");
    assert_eq!(
        failure.code.as_deref(),
        Some("authority_changed"),
        "the refusal's code reaches alice, as napi and wasm report it"
    );
    assert_eq!(
        connector.bearers().len(),
        1,
        "nothing reconnects on its own"
    );
}
