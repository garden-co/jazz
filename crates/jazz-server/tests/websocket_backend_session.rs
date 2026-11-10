//! Exercise the public admission boundary without a host runtime: no session
//! may be created before credential and identity checks finish.
use jazz::ids::AuthorSubject;
use jazz::records::Value;
use jazz::wire::WireErrorCode;
use jazz_server::runtime::websocket::{Prelude, admit_backend_session};
use serde_json::json;

fn prelude(secret: Option<&str>, claims: serde_json::Value) -> Prelude {
    Prelude::decode(&serde_json::to_vec(&json!({
        "peer_identity": AuthorSubject::authenticated("https://issuer.example", "alice").unwrap().canonical(),
        "auth": {
            "backend_secret": secret,
            "backend_session": {
                "issuer": "https://issuer.example", "user_id": "alice", "claims": claims
            }
        }
    })).unwrap()).unwrap()
}

/// Mallory's stated session is insufficient without the configured backend's
/// credential. An unconfigured host admits nobody, even if a secret is supplied.
#[test]
fn credentials_are_required_before_accepting_stated_identity() {
    for (configured, supplied) in [
        (None, Some("secret")),
        (Some("secret"), None),
        (Some("secret"), Some("wrong")),
    ] {
        let failure = admit_backend_session(prelude(supplied, json!({})), configured)
            .err()
            .unwrap();
        assert_eq!(failure.code, WireErrorCode::AuthFailed);
    }
}

/// Alice's backend may supply provider claims, but reserved identity claims
/// remain derived from Alice's verified session rather than those claims.
#[test]
fn provider_claims_cannot_replace_the_authenticated_subject() {
    let admitted = admit_backend_session(
        prelude(
            Some("secret"),
            json!({
                "iss": "https://mallory.example", "sub": "mallory", "role": "editor"
            }),
        ),
        Some("secret"),
    )
    .unwrap();
    assert_eq!(
        admitted.identity,
        AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
    );
    assert_eq!(
        admitted.claims.get(&jazz::query::provider_claim_key("sub")),
        Some(&Value::String("alice".into()))
    );
    assert_eq!(
        admitted
            .claims
            .get(&jazz::query::provider_claim_key("role")),
        Some(&Value::String("editor".into()))
    );
}

/// Alice's backend service link is trusted, but a session it states for Alice
/// remains Alice. Mallory cannot elevate a session-only upgrade using secrets
/// supplied inside the untrusted prelude.
#[test]
fn upgrade_access_controls_service_authority() {
    use jazz_server::runtime::websocket::admit_remote_session;
    let user = || prelude(Some("untrusted-prelude-secret"), json!({}));
    let alice = admit_remote_session(user(), "secret", "backend", "secret").unwrap();
    assert_ne!(alice.identity, AuthorSubject::SYSTEM);
    for access in ["", "unknown"] {
        let mut forged = user();
        forged.auth.admin_secret = Some("secret".into());
        assert!(admit_remote_session(forged, "secret", access, "secret").is_err());
    }
    assert!(admit_remote_session(user(), "wrong", "admin", "secret").is_err());
    assert!(admit_remote_session(user(), "", "admin", "").is_err());
    let mut backend = user();
    backend.auth.backend_session = None;
    assert_eq!(
        admit_remote_session(backend, "secret", "backend", "secret")
            .unwrap()
            .identity,
        AuthorSubject::SYSTEM
    );
    // Just like native admin links, explicit admin access is full authority.
    assert_eq!(
        admit_remote_session(user(), "secret", "admin", "secret")
            .unwrap()
            .identity,
        AuthorSubject::SYSTEM
    );
}
