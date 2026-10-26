//! Public ABI lifecycle tests. A connector spy and private worker inspection
//! are necessary to prove that a retained login makes no connection attempt;
//! successful foreground operations alone cannot establish that absence.
use super::*;
use base64::Engine;
use jazz::tools::native_transport_connector::NativeTransportRequest;

const APP: &str = "07070707-0707-0707-0707-070707070707";
const ISSUER: &str = "https://issuer.example";

#[derive(Default)]
struct Connector(Mutex<Vec<String>>);

impl NativeTransportConnector for Connector {
    fn connect(
        &self,
        request: NativeTransportRequest,
    ) -> jazz::tools::native_transport_connector::NativeTransportFuture {
        self.0.lock().unwrap().push(request.auth.jwt_token.unwrap());
        Box::pin(std::future::pending())
    }
}

fn token(subject: &str) -> String {
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&serde_json::json!({"iss":ISSUER,"sub":subject})).unwrap());
    format!("e30.{payload}.signature")
}

fn retained() -> serde_json::Value {
    serde_json::json!({
        "registry":format!("https://edge.example/apps/{APP}/accounts"),
        "app_id":APP,"env":"test",
        "account_id":"08080808-0808-0808-0808-080808080808",
        "issuer":ISSUER,"subject":"alice","server_url":"https://edge.example"
    })
}

struct Host {
    root: tempfile::TempDir,
    host: *mut JazzNativeRelayHost,
    lease: *mut JazzNativeRelayHostLease,
    connector: Arc<Connector>,
}

impl Host {
    fn new() -> Self {
        let host = jazz_native_relay_host_new();
        let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        let connector = Arc::new(Connector::default());
        unsafe { (*host).inner.lock().unwrap().socket_connector = Some(connector.clone()) };
        Self {
            root: tempfile::tempdir().unwrap(),
            host,
            lease,
            connector,
        }
    }

    fn begin(&self, request: &serde_json::Value) -> Result<Vec<u8>, JazzNativeRelayStatus> {
        let bytes = serde_json::to_vec(request).unwrap();
        let root = self.root.path().to_str().unwrap().as_bytes();
        let mut out = JazzNativeRelayBytes::EMPTY;
        let status = unsafe {
            jazz_native_relay_host_lease_begin_account_session_json(
                self.lease,
                bytes.as_ptr(),
                bytes.len(),
                root.as_ptr(),
                root.len(),
                &mut out,
            )
        };
        if status != JazzNativeRelayStatus::Ok {
            assert_eq!(out.len, 0);
            return Err(status);
        }
        Ok(take(&mut out))
    }

    fn attach(&self, pending: &[u8]) -> (Vec<u8>, u64) {
        let schema = jazz::tools::SchemaBuilder::new().build();
        let schema = JazzSchema::new(&schema).unwrap();
        let schema = serde_json::to_vec(schema.public_schema()).unwrap();
        let mut out = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_attach_account_schema_json(
                    self.lease,
                    pending.as_ptr(),
                    pending.len(),
                    schema.as_ptr(),
                    schema.len(),
                    &mut out,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        let capability = take(&mut out);
        let mut foreground = 0;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_open_attached_foreground(
                    self.lease,
                    capability.as_ptr(),
                    capability.len(),
                    &mut foreground,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        (capability, foreground)
    }

    fn refresh(&self, capability: &[u8], subject: &str) -> JazzNativeRelayStatus {
        let refresh = serde_json::to_vec(&serde_json::json!({
            "jwt":token(subject),"claims":{"role":"editor"}
        }))
        .unwrap();
        unsafe {
            jazz_native_relay_host_lease_refresh_account_session(
                self.lease,
                capability.as_ptr(),
                capability.len(),
                refresh.as_ptr(),
                refresh.len(),
            )
        }
    }

    fn command(
        &self,
        foreground: u64,
        request: ForegroundDbCommandRequest,
    ) -> ForegroundDbCommandResponse {
        let request = postcard::to_allocvec(&request).unwrap();
        let mut out = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_execute_foreground(
                    self.lease,
                    foreground,
                    request.as_ptr(),
                    request.len(),
                    &mut out,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        postcard::from_bytes(&take(&mut out)).unwrap()
    }

    fn disconnected(&self, foreground: u64, explicitly_offline: bool) {
        assert!(matches!(
            self.command(foreground, ForegroundDbCommandRequest::NativeConnectionStatus),
            ForegroundDbCommandResponse::NativeConnectionStatus {
                configured:true, connected:false, explicitly_offline:actual,
            } if actual == explicitly_offline
        ));
        assert!(unsafe {
            (*self.host)
                .inner
                .lock()
                .unwrap()
                .private_scope_workers
                .is_empty()
        });
        assert!(self.connector.0.lock().unwrap().is_empty());
    }

    fn connected_with(&self, bearer: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let seen = self.connector.0.lock().unwrap().clone();
            if !seen.is_empty() {
                assert_eq!(seen, [bearer]);
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "credential did not start a socket"
            );
            std::thread::yield_now();
        }
    }
}

fn take(out: &mut JazzNativeRelayBytes) -> Vec<u8> {
    let bytes = unsafe { std::slice::from_raw_parts(out.data, out.len) }.to_vec();
    unsafe { jazz_native_relay_bytes_free(out) };
    bytes
}

impl Drop for Host {
    fn drop(&mut self) {
        unsafe {
            jazz_native_relay_host_lease_free(self.lease);
            jazz_native_relay_host_free(self.host);
        }
    }
}

/// Retained Alice opens locally without a bearer. Bob's credential is refused;
/// Alice's first credential starts only her existing foreground's upstream.
#[test]
fn retained_start_waits_for_exact_identity_credential() {
    let host = Host::new();
    let (capability, foreground) = host.attach(&host.begin(&retained()).unwrap());
    host.disconnected(foreground, false);
    host.command(
        foreground,
        ForegroundDbCommandRequest::ReconnectNativeUpstream,
    );
    host.disconnected(foreground, false);
    assert_eq!(
        host.refresh(&capability, "bob"),
        JazzNativeRelayStatus::LifecycleFailure
    );
    host.disconnected(foreground, false);
    assert_eq!(
        host.refresh(&capability, "alice"),
        JazzNativeRelayStatus::Ok
    );
    host.connected_with(&token("alice"));
    assert_eq!(
        unsafe { jazz_native_relay_host_lease_tick_attached_foreground(host.lease, foreground) },
        JazzNativeRelayStatus::Ok
    );
}

/// Alice's deliberate offline setting survives arrival of her first token;
/// only an explicit reconnect starts the native transport.
#[test]
fn first_credential_preserves_explicit_disconnect() {
    let host = Host::new();
    let (capability, foreground) = host.attach(&host.begin(&retained()).unwrap());
    host.command(
        foreground,
        ForegroundDbCommandRequest::DisconnectNativeUpstream,
    );
    assert_eq!(
        host.refresh(&capability, "alice"),
        JazzNativeRelayStatus::Ok
    );
    host.disconnected(foreground, true);
    host.command(
        foreground,
        ForegroundDbCommandRequest::ReconnectNativeUpstream,
    );
    host.connected_with(&token("alice"));
}

/// Alice can refresh before schema attachment; a later retained sibling must
/// borrow that active credential and claims without restarting its transport.
#[test]
fn pending_refresh_and_retained_sibling_keep_active_snapshot() {
    let host = Host::new();
    let pending = host.begin(&retained()).unwrap();
    assert_eq!(host.refresh(&pending, "alice"), JazzNativeRelayStatus::Ok);
    let (_, foreground) = host.attach(&pending);
    host.connected_with(&token("alice"));
    let (sibling, _) = host.attach(&host.begin(&retained()).unwrap());
    let sibling: AdmissionCapability = postcard::from_bytes(&sibling).unwrap();
    let guard = unsafe { (*host.host).inner.lock().unwrap() };
    assert_eq!(guard.private_scope_workers.len(), 1);
    assert!(guard.account_session_owners[&sibling].credential_present);
    assert_eq!(
        guard.private_socket_sessions[&sibling].bearer,
        token("alice")
    );
    assert_eq!(
        guard.admitted_scopes[&sibling].claims["\0claims:role"],
        Value::String("editor".into())
    );
    drop(guard);
    assert_eq!(
        unsafe { jazz_native_relay_host_lease_tick_attached_foreground(host.lease, foreground) },
        JazzNativeRelayStatus::Ok
    );
    host.connected_with(&token("alice"));
}

/// Authenticated Alice joining retained Alice promotes both native capabilities
/// in place; the original foreground stays usable with the new shared claims.
#[test]
fn authenticated_sibling_activates_retained_foreground() {
    let host = Host::new();
    let (first, foreground) = host.attach(&host.begin(&retained()).unwrap());
    host.disconnected(foreground, false);
    let mut request = retained();
    request["jwt"] = token("alice").into();
    request["claims"] = serde_json::json!({"role":"editor"});
    let _ = host.attach(&host.begin(&request).unwrap());
    host.connected_with(&token("alice"));
    let first: AdmissionCapability = postcard::from_bytes(&first).unwrap();
    let guard = unsafe { (*host.host).inner.lock().unwrap() };
    assert!(guard.account_session_owners[&first].credential_present);
    assert_eq!(guard.private_socket_sessions[&first].bearer, token("alice"));
    assert_eq!(
        guard.admitted_scopes[&first].claims["\0claims:role"],
        Value::String("editor".into())
    );
}

/// Mallory cannot provide credential-free provider claims, an invalid bearer,
/// or a reserved principal through the retained external-account route.
#[test]
fn retained_admission_rejects_unproved_claims_and_reserved_identities() {
    let host = Host::new();
    for (field, value) in [
        ("claims", serde_json::json!({"role":"admin"})),
        ("jwt", serde_json::json!("malformed")),
        ("jwt", serde_json::json!(token("bob"))),
        (
            "issuer",
            serde_json::json!(jazz::tools::identity::LOCAL_FIRST_ISSUER),
        ),
        ("issuer", serde_json::json!(AuthorSubject::SYSTEM_ISSUER)),
        (
            "issuer",
            serde_json::json!(AuthorSubject::STATIC_BEARER_ISSUER),
        ),
        ("issuer", serde_json::json!(AuthorSubject::ANONYMOUS_ISSUER)),
    ] {
        let mut request = retained();
        request[field] = value;
        assert!(host.begin(&request).is_err(), "accepted {request}");
    }
    let guard = unsafe { (*host.host).inner.lock().unwrap() };
    assert!(guard.pending_private_sessions.is_empty());
    assert!(guard.account_session_owners.is_empty());
    assert!(host.connector.0.lock().unwrap().is_empty());
}
