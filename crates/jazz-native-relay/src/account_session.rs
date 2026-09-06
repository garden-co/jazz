//! Logical account admission for the private RN adapter. Shared TypeScript
//! owns the enrolled handle; native code owns proof checking and OS paths.

use super::*;
use jazz::ids::AuthorSubject;

pub(super) struct AccountSessionOwner {
    pub runtime_token: u64,
    pub app_id: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct AccountSessionRequest {
    registry: String,
    app_id: String,
    env: String,
    account_id: jazz::account_registry::AccountId,
    issuer: String,
    subject: String,
    jwt: String,
    server_url: Option<String>,
    #[serde(default)]
    claims: BTreeMap<String, serde_json::Value>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountSessionRefresh {
    jwt: String,
    claims: BTreeMap<String, serde_json::Value>,
}

fn advisory_claims(
    author: AuthorSubject,
    claims: BTreeMap<String, serde_json::Value>,
) -> Result<BTreeMap<String, Value>, JazzNativeRelayStatus> {
    use jazz::tools::policy_claims::{
        NumericClaimOrigin, canonical_policy_binding_claims, json_value_to_policy_claim,
    };
    let mut projected = BTreeMap::new();
    for (name, value) in claims {
        if let Some(value) = json_value_to_policy_claim(value, NumericClaimOrigin::JavaScript)
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?
        {
            projected.insert(name, value);
        }
    }
    Ok(canonical_policy_binding_claims(&author, projected))
}

impl AccountSessionRequest {
    fn prepare(
        mut self,
        storage_root: &std::path::Path,
    ) -> Result<PendingPrivateSession, JazzNativeRelayStatus> {
        let app = jazz::tools::AppId::from_string(&self.app_id)
            .unwrap_or_else(|_| jazz::tools::AppId::from_name(&self.app_id));
        let canonical_app = app.uuid().to_string();
        let registry = validate_private_session_endpoint(&self.registry)?;
        let suffix = format!("/apps/{}/accounts", app.uuid());
        if registry.as_str() != self.registry
            || registry.query().is_some()
            || registry.fragment().is_some()
            || !registry.username().is_empty()
            || registry.password().is_some()
            || !registry.path().ends_with(&suffix)
            || !storage_root.is_absolute()
        {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        if let Some(server) = &self.server_url {
            let mut transport =
                url::Url::parse(server).map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
            match transport.scheme() {
                "ws" => {
                    transport
                        .set_scheme("http")
                        .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                }
                "wss" => {
                    transport
                        .set_scheme("https")
                        .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                }
                _ => {}
            }
            let mut expected = validate_private_session_endpoint(transport.as_str())?;
            if expected.query().is_some()
                || expected.fragment().is_some()
                || !expected.username().is_empty()
                || expected.password().is_some()
            {
                return Err(JazzNativeRelayStatus::LifecycleFailure);
            }
            expected.set_path(&format!(
                "{}{}",
                expected.path().trim_end_matches('/'),
                suffix
            ));
            if expected != registry {
                return Err(JazzNativeRelayStatus::LifecycleFailure);
            }
            self.server_url = Some(transport.to_string());
        }
        let projected = jazz::tools::unverified_jwt_scope_subject(&self.jwt)
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)?;
        if projected != (self.issuer.clone(), self.subject.clone()) {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        let author = if self.issuer == jazz::tools::identity::LOCAL_FIRST_ISSUER {
            let claimed = serde_json::to_string(&[
                self.account_id.0.to_string(),
                self.issuer.clone(),
                self.subject.clone(),
            ])
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
            jazz::tools::identity::verify_client_runtime_author(&self.jwt, &self.app_id, &claimed)
                .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?
        } else {
            // Provider verification and account assignment remain server
            // authority. This projection cannot admit a forged remote write.
            AuthorSubject::authenticated(&self.issuer, &self.subject)
                .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?
                .with_account(self.account_id)
        };
        // Versioned local filename derivation: domain, BE-u32 byte lengths
        // plus UTF-8 registry/app/environment, then the 16 account UUID bytes.
        let mut root = b"jazz-native-account-root-v1\0".to_vec();
        for part in [&self.registry, &canonical_app, &self.env] {
            let length =
                u32::try_from(part.len()).map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
            root.extend_from_slice(&length.to_be_bytes());
            root.extend_from_slice(part.as_bytes());
        }
        root.extend_from_slice(self.account_id.0.as_bytes());
        let scope = RelayScopeRequest {
            app_namespace: self.registry,
            storage_namespace: serde_json::to_string(&(&canonical_app, &self.env))
                .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?,
            auth_scope: Some(author.canonical().to_owned()),
        };
        RelayScope::from(scope.clone())
            .validate()
            .map_err(relay_status)?;
        Ok(PendingPrivateSession {
            scope,
            sqlite_path: storage_root
                .join(format!("{}.sqlite", blake3::hash(&root).to_hex()))
                .display()
                .to_string(),
            identity: fresh_client_identity(author).map_err(relay_status)?,
            claims: advisory_claims(author, self.claims)?,
            socket: self.server_url.map(|server_url| PrivateRelaySocketSession {
                server_url,
                app_id: self.app_id,
                bearer: self.jwt,
            }),
        })
    }
}

/// Start an account session using logical handle metadata and a platform-owned
/// absolute storage root. Claims are the shared JS advisory projection, never
/// remote authority. No caller-selected filename is accepted. Attach the
/// canonical schema separately.
///
/// # Safety
/// `lease` must be live; request/root pointers must reference their stated byte
/// lengths; `out` must be writable and empty or previously freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_begin_account_session_json(
    lease: *mut JazzNativeRelayHostLease,
    request: *const u8,
    request_len: usize,
    storage_root: *const u8,
    storage_root_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    if out.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe {
        *out = JazzNativeRelayBytes::EMPTY;
    }
    if lease.is_null()
        || request.is_null()
        || storage_root.is_null()
        || request_len == 0
        || request_len > NATIVE_RELAY_ADMISSION_MAX_BYTES
        || storage_root_len == 0
        || storage_root_len > 16 * 1024
    {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let request = unsafe { std::slice::from_raw_parts(request, request_len) };
    let root = unsafe { std::slice::from_raw_parts(storage_root, storage_root_len) };
    let root = match std::str::from_utf8(root) {
        Ok(root) if !root.contains('\0') => std::path::Path::new(root),
        _ => return JazzNativeRelayStatus::InvalidArgument,
    };
    let request = match serde_json::from_slice::<AccountSessionRequest>(request) {
        Ok(request) => request,
        Err(_) => return JazzNativeRelayStatus::InvalidCommand,
    };
    let app_id = request.app_id.clone();
    let pending = match request.prepare(root) {
        Ok(pending) => pending,
        Err(status) => return status,
    };
    let lease = unsafe { &*lease };
    let mut host = match lease.inner.lock() {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    if host
        .invalidated_runtime_tokens
        .contains(&lease.runtime_token)
    {
        return JazzNativeRelayStatus::InvalidHandle;
    }
    let capability = match host.allocate_admission_capability() {
        Ok(capability) => capability,
        Err(error) => return relay_status(error),
    };
    host.pending_private_sessions.insert(capability, pending);
    host.account_session_owners.insert(
        capability,
        AccountSessionOwner {
            runtime_token: lease.runtime_token,
            app_id,
        },
    );
    let bytes = capability.0.to_vec().into_boxed_slice();
    unsafe {
        *out = JazzNativeRelayBytes {
            len: bytes.len(),
            data: Box::into_raw(bytes).cast(),
        };
    }
    JazzNativeRelayStatus::Ok
}

/// Attach a schema to a setup owned by this runtime lease.
///
/// # Safety
/// All pointers must be live for their stated lengths; `out` must be writable
/// and empty or previously freed. The capability has exactly 32 bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_attach_account_schema_json(
    lease: *mut JazzNativeRelayHostLease,
    capability: *const u8,
    capability_len: usize,
    schema: *const u8,
    schema_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    if out.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe {
        *out = JazzNativeRelayBytes::EMPTY;
    }
    if lease.is_null()
        || capability.is_null()
        || capability_len != 32
        || schema.is_null()
        || schema_len == 0
        || schema_len > NATIVE_RELAY_ADMISSION_MAX_BYTES
    {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let mut bytes = [0; 32];
    bytes.copy_from_slice(unsafe { std::slice::from_raw_parts(capability, 32) });
    let capability = AdmissionCapability(bytes);
    let schema =
        match std::str::from_utf8(unsafe { std::slice::from_raw_parts(schema, schema_len) }) {
            Ok(schema) => schema,
            Err(_) => return JazzNativeRelayStatus::InvalidCommand,
        };
    let lease = unsafe { &*lease };
    let mut host = match lease.inner.lock() {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    if host
        .account_session_owners
        .get(&capability)
        .map(|owner| owner.runtime_token)
        != Some(lease.runtime_token)
    {
        return JazzNativeRelayStatus::InvalidHandle;
    }
    let admitted = match host.attach_canonical_schema(capability, schema) {
        Ok(admitted) => admitted,
        Err(status) => return status,
    };
    let bytes = admitted.0.to_vec().into_boxed_slice();
    unsafe {
        *out = JazzNativeRelayBytes {
            len: bytes.len(),
            data: Box::into_raw(bytes).cast(),
        };
    }
    JazzNativeRelayStatus::Ok
}

/// Release pending or admitted account setup owned by this runtime. Close the
/// foreground normally first when a clean shutdown is required.
///
/// # Safety
/// `lease` must be live and `capability` must reference 32 readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_release_account_session(
    lease: *mut JazzNativeRelayHostLease,
    capability: *const u8,
    capability_len: usize,
) -> JazzNativeRelayStatus {
    if lease.is_null() || capability.is_null() || capability_len != 32 {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let mut bytes = [0; 32];
    bytes.copy_from_slice(unsafe { std::slice::from_raw_parts(capability, 32) });
    let capability = AdmissionCapability(bytes);
    let lease = unsafe { &*lease };
    let mut host = match lease.inner.lock() {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    match host.account_session_owners.get(&capability) {
        Some(owner) if owner.runtime_token != lease.runtime_token => {
            return JazzNativeRelayStatus::InvalidHandle;
        }
        None => return JazzNativeRelayStatus::Ok,
        _ => {}
    }
    match host.revoke_scope(capability) {
        Ok(_) => JazzNativeRelayStatus::Ok,
        Err(status) => status,
    }
}

/// Refresh the bearer for the exact identity already admitted by this handle.
/// No account, principal, registry, or transport change is accepted here.
///
/// # Safety
/// `lease` must be live; capability and token pointers must be readable for
/// their stated lengths. The capability has exactly 32 bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_refresh_account_session(
    lease: *mut JazzNativeRelayHostLease,
    capability: *const u8,
    capability_len: usize,
    token: *const u8,
    token_len: usize,
) -> JazzNativeRelayStatus {
    if lease.is_null()
        || capability.is_null()
        || capability_len != 32
        || token.is_null()
        || token_len == 0
        || token_len > NATIVE_RELAY_ADMISSION_MAX_BYTES
    {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let mut bytes = [0; 32];
    bytes.copy_from_slice(unsafe { std::slice::from_raw_parts(capability, 32) });
    let capability = AdmissionCapability(bytes);
    let token = match std::str::from_utf8(unsafe { std::slice::from_raw_parts(token, token_len) }) {
        Ok(token) => token,
        Err(_) => return JazzNativeRelayStatus::InvalidCommand,
    };
    let refresh: AccountSessionRefresh = match serde_json::from_str(token) {
        Ok(refresh) => refresh,
        Err(_) => return JazzNativeRelayStatus::InvalidCommand,
    };
    let token = refresh.jwt.as_str();
    let lease = unsafe { &*lease };
    let mut host = match lease.inner.lock() {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    let owner = match host.account_session_owners.get(&capability) {
        Some(owner) if owner.runtime_token == lease.runtime_token => owner,
        _ => return JazzNativeRelayStatus::InvalidHandle,
    };
    let author = if let Some(pending) = host.pending_private_sessions.get(&capability) {
        pending.identity.author
    } else if let Some(admitted) = host.admitted_scopes.get(&capability) {
        admitted.config.identity.author
    } else {
        return JazzNativeRelayStatus::InvalidHandle;
    };
    if jazz::tools::unverified_jwt_scope_subject(token) != Some(author.principal_parts()) {
        return JazzNativeRelayStatus::LifecycleFailure;
    }
    if author.principal_parts().0 == jazz::tools::identity::LOCAL_FIRST_ISSUER
        && jazz::tools::identity::verify_client_runtime_author(
            token,
            &owner.app_id,
            author.canonical(),
        )
        .is_err()
    {
        return JazzNativeRelayStatus::LifecycleFailure;
    }
    let claims = match advisory_claims(author, refresh.claims) {
        Ok(claims) => claims,
        Err(status) => return status,
    };
    if let Some(pending) = host.pending_private_sessions.get_mut(&capability) {
        pending.claims = claims;
        if let Some(socket) = &mut pending.socket {
            socket.bearer = token.to_owned();
        }
        return JazzNativeRelayStatus::Ok;
    }
    match host.promote_account_scope_snapshot(capability, token, claims) {
        Ok(()) => JazzNativeRelayStatus::Ok,
        Err(status) => status,
    }
}

impl NativeRelayHost {
    pub(super) fn promote_account_scope_snapshot(
        &mut self,
        capability: AdmissionCapability,
        token: &str,
        claims: BTreeMap<String, Value>,
    ) -> Result<(), JazzNativeRelayStatus> {
        let config = self.admitted_scopes[&capability].config.clone();
        let scope = config.scope.clone();
        let author = config.identity.author;
        let changed = self.admitted_scopes.iter().any(|(id, entry)| {
            entry.config.scope == scope
                && self.account_session_owners.contains_key(id)
                && (entry.claims != claims
                    || self
                        .private_socket_sessions
                        .get(id)
                        .is_some_and(|socket| socket.bearer != token))
        });
        if !changed {
            return Ok(());
        }
        let mut live_relay = self
            .relays
            .values()
            .find(|relay| relay.scope == scope)
            .map(|relay| relay.relay.clone());
        if live_relay.is_none() && self.private_scope_workers.contains_key(&scope) {
            live_relay = match self.registry.open(config) {
                Ok(relay) => Some(relay),
                Err(error) => return Err(relay_status(error)),
            };
        }
        let replacement = if !self.explicitly_offline_scopes.contains(&scope) {
            match (
                live_relay.as_ref(),
                self.private_socket_sessions.get(&capability),
            ) {
                (Some(relay), Some(session)) => {
                    let mut session = session.clone();
                    session.bearer = token.to_owned();
                    Some(Self::prepare_private_scope_worker(
                        capability,
                        relay.clone(),
                        author,
                        session,
                    )?)
                }
                _ => None,
            }
        } else {
            None
        };
        if let Some(relay) = &live_relay {
            let next = claims.clone();
            if let Err(error) = relay.run(move |worker| {
                worker.refresh_account_claims(author, next);
                Ok(())
            }) {
                return Err(relay_status(error));
            }
        }
        let siblings = self
            .admitted_scopes
            .iter()
            .filter_map(|(id, entry)| {
                (entry.config.scope == scope && self.account_session_owners.contains_key(id))
                    .then_some(*id)
            })
            .collect::<Vec<_>>();
        for sibling in siblings {
            self.admitted_scopes
                .get_mut(&sibling)
                .expect("admitted sibling")
                .claims = claims.clone();
            if let Some(socket) = self.private_socket_sessions.get_mut(&sibling) {
                socket.bearer = token.to_owned();
            }
        }
        // Foregrounds remain attached to the same durable relay. Replacing only
        // its upstream socket is the same lifecycle used by disconnect/reconnect.
        self.private_scope_workers.remove(&scope);
        if let Some(replacement) = replacement {
            replacement._worker.activate();
            self.private_scope_workers.insert(scope, replacement);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn external(subject: &str) -> AccountSessionRequest {
        let app_id = "07070707-0707-0707-0707-070707070707".to_owned();
        AccountSessionRequest {
            registry: format!("https://edge.example/apps/{app_id}/accounts"),
            app_id,
            env: "test".into(),
            account_id: jazz::account_registry::AccountId(
                "08080808-0808-0808-0808-080808080808".parse().unwrap(),
            ),
            issuer: "https://issuer.example".into(),
            subject: subject.into(),
            jwt: format!(
                "x.{}.x",
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
                    serde_json::to_vec(
                        &serde_json::json!({"iss":"https://issuer.example", "sub":subject})
                    )
                    .unwrap()
                )
            ),
            server_url: None,
            claims: BTreeMap::new(),
        }
    }

    #[test]
    fn account_setup_separates_durable_account_from_live_identity_and_transport() {
        // Pre-schema private admission has no public Db to inspect; verify
        // the resulting authority/root before any database is opened.
        let root = tempfile::tempdir().unwrap();
        let alice = external("alice").prepare(root.path()).unwrap();
        let bob = external("bob").prepare(root.path()).unwrap();
        assert_eq!(
            std::path::Path::new(&alice.sqlite_path)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap(),
            "248c40d90d29c2c916e1e96dc490075b2b6b7fe0c4976bab8803929fdddde263.sqlite"
        );
        assert_eq!(alice.sqlite_path, bob.sqlite_path);
        assert_ne!(alice.scope.auth_scope, bob.scope.auth_scope);
        assert!(alice.socket.is_none());
        let mut online = external("alice");
        online.server_url = Some("wss://edge.example".into());
        let online = online.prepare(root.path()).unwrap();
        assert_eq!(alice.sqlite_path, online.sqlite_path);
        assert!(online.socket.is_some());
        let mut other_registry = external("alice");
        other_registry.registry = other_registry
            .registry
            .replace("edge.example", "other.example");
        assert_ne!(
            alice.sqlite_path,
            other_registry.prepare(root.path()).unwrap().sqlite_path
        );
        let mut other_env = external("alice");
        other_env.env = "production".into();
        assert_ne!(
            alice.sqlite_path,
            other_env.prepare(root.path()).unwrap().sqlite_path
        );
        let mut wrong_transport = external("alice");
        wrong_transport.server_url = Some("https://other.example".into());
        assert!(wrong_transport.prepare(root.path()).is_err());
        let mut wrong_subject = external("alice");
        wrong_subject.subject = "bob".into();
        assert!(wrong_subject.prepare(root.path()).is_err());
        assert!(
            external("alice")
                .prepare(std::path::Path::new("relative"))
                .is_err()
        );
        let mut named = external("alice");
        named.app_id = "named-account-app".into();
        let canonical = jazz::tools::AppId::from_name(&named.app_id)
            .uuid()
            .to_string();
        named.registry = format!("https://edge.example/apps/{canonical}/accounts");
        let mut uuid = external("alice");
        uuid.app_id = canonical;
        uuid.registry = named.registry.clone();
        let named = named.prepare(root.path()).unwrap();
        let uuid = uuid.prepare(root.path()).unwrap();
        assert_eq!(named.sqlite_path, uuid.sqlite_path);
        assert_eq!(named.scope.storage_namespace, uuid.scope.storage_namespace);
    }

    #[test]
    fn account_context_shutdown_preserves_sibling_foreground() {
        // Native capability ownership is below the public query API: prove
        // separate foreground lifetimes across two logical context admissions.
        let root = tempfile::tempdir().unwrap();
        let root_bytes = root.path().to_str().unwrap().as_bytes();
        let request = serde_json::to_vec(&external("alice")).unwrap();
        let schema = JazzSchema::new(&jazz::tools::SchemaBuilder::new().build()).unwrap();
        let schema = serde_json::to_vec(schema.public_schema()).unwrap();
        unsafe {
            let host = jazz_native_relay_host_new();
            let lease = jazz_native_relay_host_retain(host, 1);
            let attach = || {
                let mut out = JazzNativeRelayBytes::EMPTY;
                assert_eq!(
                    jazz_native_relay_host_lease_begin_account_session_json(
                        lease,
                        request.as_ptr(),
                        request.len(),
                        root_bytes.as_ptr(),
                        root_bytes.len(),
                        &mut out
                    ),
                    JazzNativeRelayStatus::Ok
                );
                let pending = std::slice::from_raw_parts(out.data, out.len).to_vec();
                jazz_native_relay_bytes_free(&mut out);
                assert_eq!(
                    jazz_native_relay_host_lease_attach_account_schema_json(
                        lease,
                        pending.as_ptr(),
                        pending.len(),
                        schema.as_ptr(),
                        schema.len(),
                        &mut out
                    ),
                    JazzNativeRelayStatus::Ok
                );
                let capability = std::slice::from_raw_parts(out.data, out.len).to_vec();
                jazz_native_relay_bytes_free(&mut out);
                let mut foreground = 0;
                assert_eq!(
                    jazz_native_relay_host_lease_open_attached_foreground(
                        lease,
                        capability.as_ptr(),
                        capability.len(),
                        &mut foreground
                    ),
                    JazzNativeRelayStatus::Ok
                );
                (capability, foreground)
            };
            let (first, first_foreground) = attach();
            let (second, second_foreground) = attach();
            assert_ne!(first_foreground, second_foreground);
            let mut closed = false;
            assert_eq!(
                jazz_native_relay_host_lease_close_attached_foreground(
                    lease,
                    first_foreground,
                    &mut closed
                ),
                JazzNativeRelayStatus::Ok
            );
            assert!(closed);
            assert_eq!(
                jazz_native_relay_host_lease_release_account_session(
                    lease,
                    first.as_ptr(),
                    first.len()
                ),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                jazz_native_relay_host_lease_tick_attached_foreground(lease, second_foreground),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                jazz_native_relay_host_lease_close_attached_foreground(
                    lease,
                    second_foreground,
                    &mut closed
                ),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                jazz_native_relay_host_lease_release_account_session(
                    lease,
                    second.as_ptr(),
                    second.len()
                ),
                JazzNativeRelayStatus::Ok
            );
            let (reopened, foreground) = attach();
            assert_eq!(
                jazz_native_relay_host_lease_tick_attached_foreground(lease, foreground),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                jazz_native_relay_host_lease_release_account_session(
                    lease,
                    reopened.as_ptr(),
                    reopened.len()
                ),
                JazzNativeRelayStatus::Ok
            );
            jazz_native_relay_host_lease_free(lease);
            jazz_native_relay_host_free(host);
        }
    }

    #[test]
    fn refresh_of_unopened_sibling_keeps_active_scope_worker() {
        // A retained socket worker has no public row result while its endpoint
        // is offline; inspect ownership to catch a silent transport loss.
        let root = tempfile::tempdir().unwrap();
        let root_bytes = root.path().to_str().unwrap().as_bytes();
        let mut request = external("alice");
        request.registry = request
            .registry
            .replace("https://edge.example", "http://127.0.0.1:9");
        request.server_url = Some("http://127.0.0.1:9".into());
        let refresh_jwt = request.jwt.clone();
        let request = serde_json::to_vec(&request).unwrap();
        let schema = JazzSchema::new(&jazz::tools::SchemaBuilder::new().build()).unwrap();
        let schema = serde_json::to_vec(schema.public_schema()).unwrap();
        unsafe {
            let host = jazz_native_relay_host_new();
            let lease = jazz_native_relay_host_retain(host, 1);
            let attach = |open| {
                let mut out = JazzNativeRelayBytes::EMPTY;
                assert_eq!(
                    jazz_native_relay_host_lease_begin_account_session_json(
                        lease,
                        request.as_ptr(),
                        request.len(),
                        root_bytes.as_ptr(),
                        root_bytes.len(),
                        &mut out
                    ),
                    JazzNativeRelayStatus::Ok
                );
                let pending = std::slice::from_raw_parts(out.data, out.len).to_vec();
                jazz_native_relay_bytes_free(&mut out);
                assert_eq!(
                    jazz_native_relay_host_lease_attach_account_schema_json(
                        lease,
                        pending.as_ptr(),
                        pending.len(),
                        schema.as_ptr(),
                        schema.len(),
                        &mut out
                    ),
                    JazzNativeRelayStatus::Ok
                );
                let capability = std::slice::from_raw_parts(out.data, out.len).to_vec();
                jazz_native_relay_bytes_free(&mut out);
                if open {
                    let mut foreground = 0;
                    assert_eq!(
                        jazz_native_relay_host_lease_open_attached_foreground(
                            lease,
                            capability.as_ptr(),
                            capability.len(),
                            &mut foreground
                        ),
                        JazzNativeRelayStatus::Ok
                    );
                }
                capability
            };
            let first = attach(true);
            let second = attach(false);
            assert_eq!((*host).inner.lock().unwrap().private_scope_workers.len(), 1);
            let refresh = serde_json::to_vec(&serde_json::json!({
                "jwt": refresh_jwt,
                "claims": {"role": "editor"}
            }))
            .unwrap();
            assert_eq!(
                jazz_native_relay_host_lease_refresh_account_session(
                    lease,
                    second.as_ptr(),
                    second.len(),
                    refresh.as_ptr(),
                    refresh.len(),
                ),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                (*host).inner.lock().unwrap().private_scope_workers.len(),
                1,
                "refreshing an unopened sibling must not drop the active scope worker"
            );
            assert_eq!(
                jazz_native_relay_host_lease_release_account_session(
                    lease,
                    second.as_ptr(),
                    second.len()
                ),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                jazz_native_relay_host_lease_release_account_session(
                    lease,
                    first.as_ptr(),
                    first.len()
                ),
                JazzNativeRelayStatus::Ok
            );
            jazz_native_relay_host_lease_free(lease);
            jazz_native_relay_host_free(host);
        }
    }

    #[test]
    fn rotated_sibling_promotes_only_after_valid_schema() {
        // Credential snapshots and worker ownership are private native state;
        // the ABI canary verifies failed attachment cannot mutate live siblings.
        let root = tempfile::tempdir().unwrap();
        let root_bytes = root.path().to_str().unwrap().as_bytes();
        let online = |request: &mut AccountSessionRequest| {
            request.registry = request
                .registry
                .replace("https://edge.example", "http://127.0.0.1:9");
            request.server_url = Some("http://127.0.0.1:9".into());
        };
        let mut first_request = external("alice");
        online(&mut first_request);
        let mut second_request = external("alice");
        online(&mut second_request);
        second_request.jwt = second_request.jwt.replacen("x.", "y.", 1);
        let rotated_bearer = second_request.jwt.clone();
        second_request
            .claims
            .insert("role".into(), serde_json::Value::String("editor".into()));
        let mut malformed_request = external("alice");
        online(&mut malformed_request);
        malformed_request.jwt = malformed_request.jwt.replacen("x.", "z.", 1);
        malformed_request
            .claims
            .insert("role".into(), serde_json::Value::String("admin".into()));
        let first_request = serde_json::to_vec(&first_request).unwrap();
        let second_request = serde_json::to_vec(&second_request).unwrap();
        let malformed_request = serde_json::to_vec(&malformed_request).unwrap();
        let schema = JazzSchema::new(&jazz::tools::SchemaBuilder::new().build()).unwrap();
        let schema = serde_json::to_vec(schema.public_schema()).unwrap();
        unsafe {
            let host = jazz_native_relay_host_new();
            let lease = jazz_native_relay_host_retain(host, 1);
            let begin = |request: &[u8]| {
                let mut out = JazzNativeRelayBytes::EMPTY;
                assert_eq!(
                    jazz_native_relay_host_lease_begin_account_session_json(
                        lease,
                        request.as_ptr(),
                        request.len(),
                        root_bytes.as_ptr(),
                        root_bytes.len(),
                        &mut out
                    ),
                    JazzNativeRelayStatus::Ok
                );
                let pending = std::slice::from_raw_parts(out.data, out.len).to_vec();
                jazz_native_relay_bytes_free(&mut out);
                pending
            };
            let attach = |pending: &[u8]| {
                let mut out = JazzNativeRelayBytes::EMPTY;
                assert_eq!(
                    jazz_native_relay_host_lease_attach_account_schema_json(
                        lease,
                        pending.as_ptr(),
                        pending.len(),
                        schema.as_ptr(),
                        schema.len(),
                        &mut out
                    ),
                    JazzNativeRelayStatus::Ok
                );
                let admitted = std::slice::from_raw_parts(out.data, out.len).to_vec();
                jazz_native_relay_bytes_free(&mut out);
                admitted
            };
            let first = attach(&begin(&first_request));
            let mut first_foreground = 0;
            assert_eq!(
                jazz_native_relay_host_lease_open_attached_foreground(
                    lease,
                    first.as_ptr(),
                    first.len(),
                    &mut first_foreground
                ),
                JazzNativeRelayStatus::Ok
            );
            let second = attach(&begin(&second_request));
            let mut second_foreground = 0;
            assert_eq!(
                jazz_native_relay_host_lease_open_attached_foreground(
                    lease,
                    second.as_ptr(),
                    second.len(),
                    &mut second_foreground
                ),
                JazzNativeRelayStatus::Ok
            );
            {
                let inner = (*host).inner.lock().unwrap();
                assert_eq!(inner.private_scope_workers.len(), 1);
                assert!(
                    inner
                        .admitted_scopes
                        .values()
                        .all(|entry| entry.claims.get("\0claims:role")
                            == Some(&Value::String("editor".into())))
                );
                assert!(
                    inner
                        .private_socket_sessions
                        .values()
                        .all(|socket| socket.bearer == rotated_bearer)
                );
            }
            let malformed = begin(&malformed_request);
            let mut out = JazzNativeRelayBytes::EMPTY;
            assert_eq!(
                jazz_native_relay_host_lease_attach_account_schema_json(
                    lease,
                    malformed.as_ptr(),
                    malformed.len(),
                    b"not json".as_ptr(),
                    b"not json".len(),
                    &mut out
                ),
                JazzNativeRelayStatus::LifecycleFailure
            );
            {
                let inner = (*host).inner.lock().unwrap();
                assert_eq!(inner.admitted_scopes.len(), 2);
                assert_eq!(inner.private_scope_workers.len(), 1);
                assert!(
                    inner
                        .admitted_scopes
                        .values()
                        .all(|entry| entry.claims.get("\0claims:role")
                            == Some(&Value::String("editor".into())))
                );
                assert!(
                    inner
                        .private_socket_sessions
                        .values()
                        .all(|socket| socket.bearer == rotated_bearer)
                );
            }
            let mut closed = false;
            assert_eq!(
                jazz_native_relay_host_lease_close_attached_foreground(
                    lease,
                    first_foreground,
                    &mut closed
                ),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                jazz_native_relay_host_lease_close_attached_foreground(
                    lease,
                    second_foreground,
                    &mut closed
                ),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                jazz_native_relay_host_lease_release_account_session(
                    lease,
                    first.as_ptr(),
                    first.len()
                ),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                jazz_native_relay_host_lease_release_account_session(
                    lease,
                    second.as_ptr(),
                    second.len()
                ),
                JazzNativeRelayStatus::Ok
            );
            jazz_native_relay_host_lease_free(lease);
            jazz_native_relay_host_free(host);
        }
    }

    #[test]
    fn local_first_setup_verifies_proof_and_founding_account() {
        let root = tempfile::tempdir().unwrap();
        let mut request = external("unused");
        request.jwt = jazz::tools::identity::mint_jazz_self_signed_token(
            &[9; 32],
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
            &request.app_id,
            3600,
        )
        .unwrap();
        let (issuer, subject) = jazz::tools::unverified_jwt_scope_subject(&request.jwt).unwrap();
        request.issuer = issuer;
        request.subject = subject.clone();
        request.account_id = jazz::account_registry::local_first_account_id(
            request.app_id.parse().unwrap(),
            &subject,
        );
        let account = request.account_id;
        let token = request.jwt.clone();
        let prepared = request.prepare(root.path()).unwrap();
        assert_eq!(prepared.identity.author.account_id(), Some(account));
        let mut forged = external("unused");
        forged.jwt = token;
        forged.issuer = jazz::tools::identity::LOCAL_FIRST_ISSUER.into();
        forged.subject = subject;
        assert!(
            forged.prepare(root.path()).is_err(),
            "valid proof cannot select another account"
        );
    }

    #[test]
    fn account_capabilities_are_runtime_owned_and_retired_before_schema_attachment() {
        use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
        let root = tempfile::tempdir().unwrap();
        let root_bytes = root.path().to_str().unwrap().as_bytes();
        let request = serde_json::to_vec(&external("alice")).unwrap();
        let schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("notes").column("title", ColumnType::Text))
                .build(),
        )
        .unwrap();
        let schema = serde_json::to_vec(schema.public_schema()).unwrap();
        let host = jazz_native_relay_host_new();
        // All pointers below are owned until their final frees; the ABI test
        // proves lifecycle behavior without inspecting host implementation.
        unsafe {
            let first = jazz_native_relay_host_retain(host, 1);
            let other = jazz_native_relay_host_retain(host, 2);
            let mut out = JazzNativeRelayBytes::EMPTY;
            assert_eq!(
                jazz_native_relay_host_lease_begin_account_session_json(
                    first,
                    request.as_ptr(),
                    request.len(),
                    root_bytes.as_ptr(),
                    root_bytes.len(),
                    &mut out,
                ),
                JazzNativeRelayStatus::Ok
            );
            let capability = std::slice::from_raw_parts(out.data, out.len).to_vec();
            jazz_native_relay_bytes_free(&mut out);
            assert_eq!(capability.len(), 32);
            let same_identity =
                serde_json::json!({"jwt": external("alice").jwt, "claims": {"role": "editor"}})
                    .to_string();
            let other_identity =
                serde_json::json!({"jwt": external("bob").jwt, "claims": {}}).to_string();
            assert_eq!(
                jazz_native_relay_host_lease_refresh_account_session(
                    first,
                    capability.as_ptr(),
                    capability.len(),
                    same_identity.as_ptr(),
                    same_identity.len(),
                ),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                jazz_native_relay_host_lease_refresh_account_session(
                    first,
                    capability.as_ptr(),
                    capability.len(),
                    other_identity.as_ptr(),
                    other_identity.len(),
                ),
                JazzNativeRelayStatus::LifecycleFailure
            );
            assert_eq!(
                jazz_native_relay_host_lease_release_account_session(
                    other,
                    capability.as_ptr(),
                    capability.len(),
                ),
                JazzNativeRelayStatus::InvalidHandle
            );
            assert_eq!(
                jazz_native_relay_host_lease_attach_account_schema_json(
                    other,
                    capability.as_ptr(),
                    capability.len(),
                    schema.as_ptr(),
                    schema.len(),
                    &mut out,
                ),
                JazzNativeRelayStatus::InvalidHandle
            );
            assert_eq!(
                jazz_native_relay_host_lease_invalidate_foreground_runtime(first),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                jazz_native_relay_host_lease_begin_account_session_json(
                    first,
                    request.as_ptr(),
                    request.len(),
                    root_bytes.as_ptr(),
                    root_bytes.len(),
                    &mut out,
                ),
                JazzNativeRelayStatus::InvalidHandle
            );
            assert_eq!(
                jazz_native_relay_host_lease_attach_account_schema_json(
                    first,
                    capability.as_ptr(),
                    capability.len(),
                    schema.as_ptr(),
                    schema.len(),
                    &mut out,
                ),
                JazzNativeRelayStatus::InvalidHandle
            );
            assert_eq!(
                jazz_native_relay_host_lease_release_account_session(
                    first,
                    capability.as_ptr(),
                    capability.len(),
                ),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                jazz_native_relay_host_lease_begin_account_session_json(
                    other,
                    request.as_ptr(),
                    request.len(),
                    root_bytes.as_ptr(),
                    root_bytes.len(),
                    &mut out,
                ),
                JazzNativeRelayStatus::Ok
            );
            let pending = std::slice::from_raw_parts(out.data, out.len).to_vec();
            jazz_native_relay_bytes_free(&mut out);
            assert_eq!(
                jazz_native_relay_host_lease_attach_account_schema_json(
                    other,
                    pending.as_ptr(),
                    pending.len(),
                    schema.as_ptr(),
                    schema.len(),
                    &mut out,
                ),
                JazzNativeRelayStatus::Ok
            );
            let admitted = std::slice::from_raw_parts(out.data, out.len).to_vec();
            jazz_native_relay_bytes_free(&mut out);
            assert_eq!(
                jazz_native_relay_host_lease_refresh_account_session(
                    other,
                    admitted.as_ptr(),
                    admitted.len(),
                    same_identity.as_ptr(),
                    same_identity.len(),
                ),
                JazzNativeRelayStatus::Ok
            );
            assert_eq!(
                jazz_native_relay_host_lease_refresh_account_session(
                    other,
                    admitted.as_ptr(),
                    admitted.len(),
                    other_identity.as_ptr(),
                    other_identity.len(),
                ),
                JazzNativeRelayStatus::LifecycleFailure
            );
            let mut foreground = 0;
            assert_eq!(
                jazz_native_relay_host_lease_open_attached_foreground(
                    first,
                    admitted.as_ptr(),
                    admitted.len(),
                    &mut foreground,
                ),
                JazzNativeRelayStatus::InvalidHandle
            );
            assert_eq!(
                jazz_native_relay_host_lease_open_attached_foreground(
                    other,
                    admitted.as_ptr(),
                    admitted.len(),
                    &mut foreground,
                ),
                JazzNativeRelayStatus::Ok
            );
            let mut closed = false;
            assert_eq!(
                jazz_native_relay_host_lease_close_attached_foreground(
                    other,
                    foreground,
                    &mut closed
                ),
                JazzNativeRelayStatus::Ok
            );
            assert!(closed);
            assert_eq!(
                jazz_native_relay_host_lease_release_account_session(
                    other,
                    admitted.as_ptr(),
                    admitted.len(),
                ),
                JazzNativeRelayStatus::Ok
            );
            jazz_native_relay_host_lease_free(first);
            jazz_native_relay_host_lease_free(other);
            jazz_native_relay_host_free(host);
        }
    }
}
