//! An admitted session's identity and policy claims: what a host binds to
//! a connection once its credential is verified (verification itself
//! stays with each host).

use std::collections::BTreeMap;

use jazz::ids::AuthorSubject;
use jazz::records::Value;
use jazz::tools::Session;

/// Inject the trusted session vocabulary once after provider claims are
/// admitted. Provider `sub` maps to the documented raw `user_id`; `user` is
/// the distinct reserved canonical `[iss,sub]` logical identity.
pub fn admitted_session_claims(
    issuer: &str,
    subject: &str,
    author: AuthorSubject,
    claims: BTreeMap<String, Value>,
) -> BTreeMap<String, Value> {
    let (author_issuer, author_subject) = author.principal_parts();
    debug_assert_eq!(
        (author_issuer, author_subject),
        (issuer.to_owned(), subject.to_owned())
    );
    jazz::model::policy_claims::canonical_policy_binding_claims(&author, claims)
}

/// Convert flat verified JWT metadata into scalar policy values. RFC 7519
/// registered transport/security fields are excluded because admission supplies
/// verified identity separately. Objects retain their nested fields for
/// policy evaluation.
pub fn policy_claims(
    extra: BTreeMap<String, serde_json::Value>,
) -> Result<BTreeMap<String, Value>, String> {
    let mut claims = BTreeMap::new();
    for (name, value) in extra {
        if matches!(
            name.as_str(),
            "sub" | "exp" | "nbf" | "iat" | "iss" | "aud" | "jti"
        ) {
            continue;
        }
        if let Some(value) = jazz::model::policy_claims::json_value_to_policy_claim(
            value,
            jazz::model::policy_claims::NumericClaimOrigin::ExactJson,
        )? {
            claims.insert(name, value);
        }
    }
    Ok(claims)
}

/// The policy claims of a session a host's middleware verified: its
/// provider claims, then the canonical binding vocabulary for its author.
pub fn session_claims(session: Session) -> Result<BTreeMap<String, Value>, String> {
    let author = session
        .author_subject()
        .map_err(|error| error.to_string())?;
    let provider_claims = match session.claims {
        serde_json::Value::Object(map) => {
            // Middleware exposes these convenient aliases to server handlers,
            // but they duplicate the verified transport identity. A relay
            // capability must use the one canonical binding vocabulary: the
            // shared admission constructor derives `claims.iss`/`claims.sub`,
            // `user`, and `authMode` from the verified AuthorSubject.
            map.into_iter().collect()
        }
        _ => BTreeMap::new(),
    };
    // Worded as `AuthAdmissionError::InvalidJwt` words it: the refusal a
    // client is shown.
    let provider_claims =
        policy_claims(provider_claims).map_err(|error| format!("invalid bearer JWT: {error}"))?;
    Ok(admitted_session_claims(
        &session.issuer,
        &session.user_id,
        author,
        provider_claims,
    ))
}

/// The author a connection's prelude names as its peer.
pub fn ws_peer_identity(identity: &str) -> Result<AuthorSubject, String> {
    AuthorSubject::from_canonical(identity).map_err(|error| error.to_string())
}

/// That a connection's verified session is the peer its prelude named.
pub fn ws_validate_session_identity(
    session: &Session,
    peer_identity: AuthorSubject,
) -> Result<(), String> {
    let session_identity = session
        .author_subject()
        .map_err(|error| error.to_string())?;
    if session_identity != peer_identity {
        return Err("websocket peer_identity must match authenticated session author".to_owned());
    }
    Ok(())
}
