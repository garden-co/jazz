//! Shared checks and policy claims for already authenticated sessions.
use jazz::ids::AuthorSubject;
use jazz::records::Value;
use jazz::tools::Session;
use std::collections::BTreeMap;

/// Compare credentials without returning at the first differing byte.
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub fn session_claims(
    session: jazz::tools::public_schema::Session,
) -> Result<BTreeMap<String, Value>, String> {
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
    let provider_claims = jwt_json_claims_to_policy_claims(provider_claims)
        .map_err(|error| format!("invalid bearer JWT: {error}"))?;
    Ok(admitted_session_claims(
        &session.issuer,
        &session.user_id,
        author,
        provider_claims,
    ))
}

pub fn peer_identity(identity: &str) -> Result<AuthorSubject, String> {
    AuthorSubject::from_canonical(identity).map_err(|error| error.to_string())
}

pub fn validate_session_identity(
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

pub fn jwt_json_claims_to_policy_claims(
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
