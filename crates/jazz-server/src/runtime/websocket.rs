//! Host-independent WebSocket protocol handling.
//!
//! Hosts supply message transport, scheduling and credential verification.
//! Parsing a prelude or Hello does not authenticate a peer or admit a session.
//! These helpers retain the existing WebSocket carrier and wire encodings.

use jazz::wire::{WireError, WireErrorCode, WireHello, WireRetry};

/// Untrusted connection requests, to be verified by the hosting adapter.
#[derive(serde::Deserialize)]
pub struct Prelude {
    pub peer_identity: String,
    pub auth: jazz::tools::websocket_prelude_auth::AuthConfig,
    /// A one-shot authenticated authority-catalogue transfer. This is not a
    /// subscriber session and never admits application frames.
    #[serde(default)]
    pub bootstrap_catalogue: bool,
    /// Requested before any wire frame.  It is only a request: after JWT/cookie
    /// authentication and feature negotiation the server either creates its
    /// own immutable admitted relay capability or rejects the connection.
    #[serde(default)]
    pub requested_link: RequestedLink,
    /// The audience a trusted backend verified the client's local-first
    /// proof against; honoured only with that backend's secret (see
    /// the native host’s local-first audience header).
    #[serde(default)]
    pub local_first_audience: Option<String>,
    /// The published permissions bundle (hex id) the backend that opened
    /// this socket serves with; honoured only on that backend's word (its
    /// credential on the upgrade, or its secret here), as a client could
    /// otherwise pick an older bundle's laxer policies.
    #[serde(default)]
    pub bundle: Option<String>,
}

/// The only client-selectable *request* at the WebSocket boundary.  This is
/// intentionally separate from `WirePeerRole`: a wire hello says what a peer
/// implements, not what authority it receives.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestedLink {
    #[default]
    OrdinarySession,
    ScopeIsolatedClientRelay,
}

impl Prelude {
    /// Parse the client prelude without granting any requested identity or capability.
    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        serde_json::from_slice(bytes).map_err(|error| {
            WireError::new(
                WireErrorCode::AuthFailed,
                WireRetry::Never,
                format!("invalid websocket prelude: {error}"),
            )
        })
    }
}

/// Decode the opening `Hello`, which arrives as a one-frame batch or a bare
/// frame. A peer on another wire version gets `UnsupportedProtocolVersion`
/// even when the rest of its `Hello` uses that version's layout.
pub fn decode_hello(bytes: &[u8]) -> Result<WireHello, WireError> {
    let not_hello = || {
        WireError::new(
            WireErrorCode::MalformedFrame,
            WireRetry::Never,
            "websocket expects first wire frame to be WireFrame::Hello",
        )
    };
    let frame = match decode_frame_batch(bytes) {
        Ok(mut frames) if frames.len() == 1 => frames.remove(0),
        Ok(_) => return Err(not_hello()),
        Err(_) => bytes.to_vec(),
    };
    jazz::wire::decode_hello_frame(&frame).map_err(|error| match error.code {
        WireErrorCode::UnsupportedProtocolVersion => error,
        _ => not_hello(),
    })
}

/// The refusal for a client whose `Hello` advertises a wire version range
/// without this server's, encoded so that client decodes it: an alpha.59
/// client is told to upgrade instead of failing on a frame it cannot read.
/// `None` when the first frame is not a `Hello` or speaks this version.
pub fn foreign_version_refusal(bytes: &[u8]) -> Option<Vec<u8>> {
    let frame = match decode_frame_batch(bytes) {
        Ok(mut frames) if frames.len() == 1 => frames.remove(0),
        Ok(_) => return None,
        Err(_) => bytes.to_vec(),
    };
    let (min, max) = jazz::wire::wire_hello_version_range(&frame)?;
    let ours = jazz::wire::WIRE_PROTOCOL_VERSION;
    let message = if max < ours {
        format!(
            "this Jazz client speaks wire protocol {min}..={max}, older than this server's \
             {ours}: upgrade the client to the server's Jazz release (a client from \
             2.0.0-alpha.59 or earlier cannot connect to 2.0.0-alpha.60 or later)"
        )
    } else if min > ours {
        format!(
            "this Jazz client speaks wire protocol {min}..={max}, newer than this server's \
             {ours}: upgrade the server to the client's Jazz release"
        )
    } else if min == ours && max == ours {
        return None;
    } else {
        format!(
            "this Jazz client advertises wire protocol {min}..={max}; this server speaks \
             exactly {ours}: use the same Jazz release on client and server"
        )
    };
    let error = WireError::new(
        WireErrorCode::UnsupportedProtocolVersion,
        WireRetry::Never,
        message,
    );
    jazz::wire::encode_error_frame_for_peer(&error, max).ok()
}

/// Decode the bounded carrier into encoded engine frames.
pub fn decode_frame_batch(bytes: &[u8]) -> Result<Vec<Vec<u8>>, postcard::Error> {
    jazz::wire::decode_websocket_frame_batch(bytes)
}

/// Pack encoded wire frames into as few bounded WebSocket batch payloads as fit.
pub fn encode_frame_batches(frames: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, postcard::Error> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    for frame in frames {
        let mut candidate = current.clone();
        candidate.push(frame.clone());
        let candidate_fits = jazz::wire::encode_websocket_frame_batch(&candidate).is_ok();
        if !candidate_fits && !current.is_empty() {
            batches.push(jazz::wire::encode_websocket_frame_batch(&current)?);
            current.clear();
            // A singleton has its own count and length prefixes. Validate the
            // actual carrier after the flush instead of assuming a raw frame
            // at the frame limit can fit by itself.
            jazz::wire::encode_websocket_frame_batch(std::slice::from_ref(frame))?;
        } else if !candidate_fits {
            return Err(postcard::Error::SerializeBufferFull);
        }
        current.push(frame.clone());
    }
    if !current.is_empty() {
        batches.push(jazz::wire::encode_websocket_frame_batch(&current)?);
    }
    Ok(batches)
}

/// The identity served by an admitted connection. Trusted service links use
/// SYSTEM; backend-stated user sessions keep their policy-scoped identity.
pub struct BackendSession {
    pub identity: jazz::ids::AuthorSubject,
    pub claims: std::collections::BTreeMap<String, jazz::records::Value>,
    pub bundle: Option<jazz::catalogue::BundleId>,
}

/// Admit the backend-stated session mode used by createJazz. The expected
/// credential comes from host configuration, never from request material.
/// Other authentication modes require their own verified admission path.
pub fn admit_backend_session(
    prelude: Prelude,
    expected_secret: Option<&str>,
) -> Result<BackendSession, WireError> {
    use super::session_admission::{
        constant_time_eq, peer_identity, session_claims, validate_session_identity,
    };
    let denied =
        |message: String| WireError::new(WireErrorCode::AuthFailed, WireRetry::Never, message);
    let identity = peer_identity(&prelude.peer_identity).map_err(denied)?;
    if !matches!((expected_secret, prelude.auth.backend_secret.as_deref()),
        (Some(expected), Some(provided)) if constant_time_eq(expected, provided))
    {
        return Err(denied("Invalid backend secret".into()));
    }
    if prelude.auth.admin_secret.is_some() || prelude.auth.backend_session.is_none() {
        return Err(WireError::new(
            WireErrorCode::UnsupportedFeature,
            WireRetry::Never,
            "this host currently requires a backend-stated user session",
        ));
    }
    let session = serde_json::from_value(prelude.auth.backend_session.unwrap())
        .map_err(|_| denied("Invalid session format".into()))?;
    validate_session_identity(&session, identity).map_err(denied)?;
    let claims = session_claims(session).map_err(denied)?;
    let bundle = prelude
        .bundle
        .as_deref()
        .map(|text| {
            jazz::catalogue::BundleId::from_hex(text)
                .ok_or_else(|| denied(format!("invalid permissions bundle id: {text}")))
        })
        .transpose()?;
    if prelude.bootstrap_catalogue || prelude.requested_link != RequestedLink::OrdinarySession {
        return Err(WireError::new(
            WireErrorCode::UnsupportedFeature,
            WireRetry::Never,
            "this host currently supports ordinary sessions only",
        ));
    }
    Ok(BackendSession {
        identity,
        claims,
        bundle,
    })
}

/// Admit a socket whose HTTP upgrade is authenticated by the remote provider.
/// Only the verified header access grants authority; prelude secrets never do.
/// This mirrors the native workload gate and its backend/admin link semantics.
pub fn admit_remote_session(
    mut prelude: Prelude,
    credential: &str,
    access: &str,
    expected: &str,
) -> Result<BackendSession, WireError> {
    use super::session_admission::{constant_time_eq, peer_identity};
    let denied = |message: &str| {
        WireError::new(
            WireErrorCode::AuthFailed,
            WireRetry::Never,
            message.to_owned(),
        )
    };
    if expected.is_empty() || !constant_time_eq(credential, expected) {
        return Err(denied("Invalid backend credential"));
    }
    if !matches!(access, "" | "backend" | "admin") {
        return Err(denied("Invalid backend access"));
    }
    // Session-only upgrades cannot assert users or authenticate service links.
    // Guest proof verification needs the account registry and is not wired yet.
    if access.is_empty() {
        return Err(WireError::new(
            WireErrorCode::UnsupportedFeature,
            WireRetry::Never,
            "guest proof admission is not supported on this host yet",
        ));
    }
    if prelude.bootstrap_catalogue || prelude.requested_link != RequestedLink::OrdinarySession {
        return Err(WireError::new(
            WireErrorCode::UnsupportedFeature,
            WireRetry::Never,
            "this host currently supports ordinary sessions only",
        ));
    }
    peer_identity(&prelude.peer_identity).map_err(|message| denied(&message))?;
    prelude.auth.admin_secret = None;
    prelude.auth.backend_secret = Some(expected.to_owned());
    if access == "backend" && prelude.auth.backend_session.is_some() {
        return admit_backend_session(prelude, Some(expected));
    }
    if access == "backend" && prelude.auth.jwt_token.is_some() {
        return Err(WireError::new(
            WireErrorCode::UnsupportedFeature,
            WireRetry::Never,
            "guest proof admission is not supported on this host yet",
        ));
    }
    let bundle = prelude
        .bundle
        .as_deref()
        .map(|text| {
            jazz::catalogue::BundleId::from_hex(text)
                .ok_or_else(|| denied("invalid permissions bundle id"))
        })
        .transpose()?;
    Ok(BackendSession {
        identity: jazz::ids::AuthorSubject::SYSTEM,
        claims: Default::default(),
        bundle,
    })
}

/// Verify a client's own Jazz proof, then bind its immutable registry assignment.
/// The remote transport credential authenticates the backend hop, not the user.
pub fn admit_remote_guest(
    prelude: Prelude,
    credential: &str,
    access: &str,
    expected: &str,
    app: &str,
    registry: &super::accounts::LocalRegistry<'_>,
    now: u64,
) -> Result<BackendSession, WireError> {
    use super::session_admission::{
        constant_time_eq, peer_identity, session_claims, validate_session_identity,
    };
    use jazz::account_registry::{AccountCommand, Principal};
    let denied =
        |message: String| WireError::new(WireErrorCode::AuthFailed, WireRetry::Never, message);
    let registry_error = |error: super::accounts::RegistryError| match error {
        super::accounts::RegistryError::Unavailable(_) => WireError::new(
            WireErrorCode::NotReady,
            WireRetry::Later,
            "account registry unavailable",
        ),
        error => denied(error.to_string()),
    };
    if expected.is_empty()
        || !constant_time_eq(credential, expected)
        || !matches!(access, "" | "backend")
    {
        return Err(denied("Invalid backend credential or access".into()));
    }
    if prelude.bootstrap_catalogue
        || prelude.requested_link != RequestedLink::OrdinarySession
        || prelude.auth.backend_session.is_some()
    {
        return Err(denied("Invalid guest session mode".into()));
    }
    let audience = match prelude.local_first_audience.as_deref() {
        Some(audience) if access == "backend" && !audience.trim().is_empty() => audience.trim(),
        Some(_) => {
            return Err(denied(
                "A local-first audience must be vouched for with backend access".into(),
            ));
        }
        None => app,
    };
    let proof = jazz::tools::identity::verify_jazz_self_signed_proof_at(
        prelude
            .auth
            .jwt_token
            .as_deref()
            .ok_or_else(|| denied("Session proof required".into()))?,
        audience,
        now,
    )
    .map_err(denied)?;
    let identity = peer_identity(&prelude.peer_identity).map_err(denied)?;
    let account = identity
        .account_id()
        .ok_or_else(|| denied("public sessions require an enrolled account identity".into()))?;
    let principal = Principal {
        issuer: proof.issuer.to_owned(),
        subject: proof.user_id.clone(),
    };
    if proof.issuer == jazz::tools::identity::LOCAL_FIRST_ISSUER {
        let app = jazz::app_id::AppId::from_string(audience)
            .unwrap_or_else(|_| jazz::app_id::AppId::from_name(audience));
        registry
            .command(&AccountCommand::FoundLocalFirst {
                principal: principal.clone(),
                app: *app.uuid(),
            })
            .map_err(registry_error)?;
    }
    if registry.lookup(&principal).map_err(registry_error)?.account != account {
        return Err(denied(
            "account does not match authenticated identity assignment".into(),
        ));
    }
    let session = jazz::tools::Session {
        account_id: Some(account),
        issuer: proof.issuer.to_owned(),
        user_id: proof.user_id,
        claims: serde_json::json!({}),
        auth_mode: if proof.issuer == jazz::tools::identity::LOCAL_FIRST_ISSUER {
            jazz::tools::AuthMode::LocalFirst
        } else {
            jazz::tools::AuthMode::Anonymous
        },
    };
    validate_session_identity(&session, identity).map_err(denied)?;
    let bundle = prelude
        .bundle
        .as_deref()
        .map(|s| {
            jazz::catalogue::BundleId::from_hex(s)
                .ok_or_else(|| denied("invalid permissions bundle id".into()))
        })
        .transpose()?;
    Ok(BackendSession {
        identity,
        claims: session_claims(session).map_err(denied)?,
        bundle,
    })
}
