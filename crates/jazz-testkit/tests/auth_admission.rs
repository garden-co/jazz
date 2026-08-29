use std::time::{SystemTime, UNIX_EPOCH};

use jazz::ids::{AuthorSubject, AuthorSubjectError};
use jazz::serving::auth_admission::{
    AdmissionSource, AuthAdmissionConfig, AuthAdmissionError, AuthHandshake, JwtVerifierConfig,
    LOCAL_FIRST_JWT_ISSUER, admit_bearer_jwt, admit_local_first_jwt, admit_static_bearer,
    admit_static_bearer_with_claims,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Serialize;

#[test]
fn auth_handshake_only_accepts_explicit_bearer_aliases() {
    for alias in ["jwt_token", "backend_session"] {
        let handshake: AuthHandshake = serde_json::from_value(serde_json::json!({
            alias: "alias-token",
            "sub": "alias-subject",
            "claims": {},
        }))
        .unwrap();
        assert_eq!(handshake.bearer_jwt.as_deref(), Some("alias-token"));
    }

    for privileged_name in ["admin_secret", "backend_secret"] {
        let handshake: AuthHandshake = serde_json::from_value(serde_json::json!({
            privileged_name: "privileged-secret",
            "sub": "alias-subject",
            "claims": {},
        }))
        .unwrap();
        assert!(
            handshake.bearer_jwt.is_none(),
            "{privileged_name} must not become an ordinary bearer credential"
        );
    }
}

#[test]
fn local_first_jwt_enabled_accepts_valid_token() {
    let config = local_first_config(true).with_expected_audience("auth-app");
    let token = local_first_token(
        "alice",
        expires_in(60),
        Some(LOCAL_FIRST_JWT_ISSUER),
        Some("auth-app"),
        Some("auth-app"),
    );

    let admitted = admit_local_first_jwt(&config, Some(&token)).unwrap();

    assert_eq!(admitted.subject, "alice");
    assert_eq!(admitted.source, AdmissionSource::LocalFirstJwt);
}

#[test]
fn local_first_jwt_wrong_audience_rejects() {
    let config = local_first_config(true).with_expected_audience("auth-app");
    let token = local_first_token(
        "alice",
        expires_in(60),
        Some(LOCAL_FIRST_JWT_ISSUER),
        Some("other-app"),
        Some("auth-app"),
    );

    assert!(matches!(
        admit_local_first_jwt(&config, Some(&token)),
        Err(AuthAdmissionError::InvalidJwt(_))
    ));
}

#[test]
fn local_first_jwt_wrong_app_id_rejects() {
    let config = local_first_config(true).with_expected_audience("auth-app");
    let token = local_first_token(
        "alice",
        expires_in(60),
        Some(LOCAL_FIRST_JWT_ISSUER),
        Some("auth-app"),
        Some("other-app"),
    );

    assert_eq!(
        admit_local_first_jwt(&config, Some(&token)),
        Err(AuthAdmissionError::InvalidJwt(
            "appId does not match expected audience".to_owned()
        ))
    );
}

#[test]
fn local_first_jwt_missing_subject_rejects() {
    let config = local_first_config(true);
    let token = local_first_token_with_subject(
        None,
        expires_in(60),
        Some(LOCAL_FIRST_JWT_ISSUER),
        None,
        None,
    );

    assert!(matches!(
        admit_local_first_jwt(&config, Some(&token)),
        Err(AuthAdmissionError::InvalidJwt(_))
    ));
}

#[test]
fn local_first_jwt_disabled_rejects_valid_token() {
    let config = local_first_config(false);
    let token = local_first_token(
        "alice",
        expires_in(60),
        Some(LOCAL_FIRST_JWT_ISSUER),
        None,
        None,
    );

    let error = admit_local_first_jwt(&config, Some(&token)).unwrap_err();

    assert_eq!(error, AuthAdmissionError::InvalidBearer);
}

#[test]
fn local_first_jwt_expired_rejects() {
    let config = local_first_config(true);
    let token = local_first_token(
        "alice",
        expires_in(-3600),
        Some(LOCAL_FIRST_JWT_ISSUER),
        None,
        None,
    );

    assert!(matches!(
        admit_local_first_jwt(&config, Some(&token)),
        Err(AuthAdmissionError::InvalidJwt(_))
    ));
}

#[test]
fn local_first_jwt_missing_issuer_rejects() {
    let config = local_first_config(true);
    let token = local_first_token("alice", expires_in(60), None, None, None);

    assert!(matches!(
        admit_local_first_jwt(&config, Some(&token)),
        Err(AuthAdmissionError::InvalidJwt(_))
    ));
}

#[test]
fn local_first_jwt_wrong_issuer_rejects() {
    let config = local_first_config(true);
    let token = local_first_token("alice", expires_in(60), Some("urn:jazz:other"), None, None);

    assert!(matches!(
        admit_local_first_jwt(&config, Some(&token)),
        Err(AuthAdmissionError::InvalidJwt(_))
    ));
}

#[test]
fn external_jwt_rejects_every_jazz_reserved_issuer_with_typed_error() {
    for issuer in [
        AuthorSubject::SYSTEM_ISSUER,
        AuthorSubject::LOCAL_FIRST_ISSUER,
        AuthorSubject::STATIC_BEARER_ISSUER,
        AuthorSubject::ANONYMOUS_ISSUER,
    ] {
        let config = local_first_config(false)
            .with_expected_issuer(issuer)
            .with_expected_audience("auth-app");
        let token = local_first_token(
            "alice",
            expires_in(60),
            Some(issuer),
            Some("auth-app"),
            None,
        );
        assert_eq!(
            admit_bearer_jwt(&config, Some(&token), AdmissionSource::AuthorizationHeader),
            Err(AuthAdmissionError::InvalidAuthorSubject(
                AuthorSubjectError::ReservedIssuer(issuer.to_owned())
            ))
        );
    }
}

#[test]
fn local_first_jwt_same_subject_maps_same_author() {
    let config = local_first_config(true);
    let first = local_first_token(
        "alice",
        expires_in(60),
        Some(LOCAL_FIRST_JWT_ISSUER),
        None,
        None,
    );
    let second = local_first_token(
        "alice",
        expires_in(120),
        Some(LOCAL_FIRST_JWT_ISSUER),
        None,
        None,
    );

    let first = admit_local_first_jwt(&config, Some(&first)).unwrap();
    let second = admit_local_first_jwt(&config, Some(&second)).unwrap();

    assert_eq!(first.author, second.author);
}

#[test]
fn local_first_jwt_different_subject_maps_different_author() {
    let config = local_first_config(true);
    let first = local_first_token(
        "alice",
        expires_in(60),
        Some(LOCAL_FIRST_JWT_ISSUER),
        None,
        None,
    );
    let second = local_first_token(
        "bob",
        expires_in(60),
        Some(LOCAL_FIRST_JWT_ISSUER),
        None,
        None,
    );

    let first = admit_local_first_jwt(&config, Some(&first)).unwrap();
    let second = admit_local_first_jwt(&config, Some(&second)).unwrap();

    assert_ne!(first.author, second.author);
}

#[test]
fn subject_admission_rejects_blank_but_preserves_opaque_spelling() {
    let config = local_first_config(true);
    for subject in ["", " \t\n\x0b\x0c\r "] {
        let token = local_first_token(
            subject,
            expires_in(60),
            Some(LOCAL_FIRST_JWT_ISSUER),
            None,
            None,
        );
        assert!(matches!(
            admit_local_first_jwt(&config, Some(&token)),
            Err(AuthAdmissionError::InvalidJwt(_))
        ));
    }

    let exact = " workos_user_01J8Y3K4M5N6P7Q8R9S0T1U2V3 ";
    let token = local_first_token(
        exact,
        expires_in(60),
        Some(LOCAL_FIRST_JWT_ISSUER),
        None,
        None,
    );
    let admitted = admit_local_first_jwt(&config, Some(&token)).unwrap();
    assert_eq!(admitted.subject, exact);
    assert_eq!(
        admitted.author.canonical(),
        serde_json::to_string(&(LOCAL_FIRST_JWT_ISSUER, exact)).unwrap()
    );

    let static_admitted = admit_static_bearer(
        &AuthAdmissionConfig::static_bearer("test-token"),
        Some("test-token"),
        exact,
        AdmissionSource::FirstFrameHandshake,
    )
    .unwrap();
    assert_eq!(static_admitted.subject, exact);
    assert_ne!(static_admitted.author, admitted.author);
    assert_eq!(
        static_admitted.author.canonical(),
        serde_json::to_string(&(jazz::serving::auth_admission::STATIC_BEARER_ISSUER, exact))
            .unwrap()
    );
}

#[test]
fn static_bearer_claim_admission_rejects_ascii_blank_and_preserves_exact_subject() {
    let config = AuthAdmissionConfig::static_bearer("test-token");
    for subject in ["", " \t\n\x0b\x0c\r "] {
        assert_eq!(
            admit_static_bearer_with_claims(
                &config,
                Some("test-token"),
                subject,
                Default::default(),
                AdmissionSource::FirstFrameHandshake,
            ),
            Err(AuthAdmissionError::InvalidHandshake(
                "sub must be non-empty".to_owned()
            ))
        );
    }

    let exact = " \u{0085}workos_user\u{feff} ";
    let admitted = admit_static_bearer_with_claims(
        &config,
        Some("test-token"),
        exact,
        Default::default(),
        AdmissionSource::FirstFrameHandshake,
    )
    .unwrap();
    assert_eq!(admitted.subject, exact);
    assert_eq!(
        admitted.author.canonical(),
        serde_json::to_string(&(jazz::serving::auth_admission::STATIC_BEARER_ISSUER, exact))
            .unwrap()
    );
}

fn local_first_config(allow_local_first_auth: bool) -> AuthAdmissionConfig {
    AuthAdmissionConfig {
        jwt_verifier: Some(JwtVerifierConfig::ed_public_key_pem(
            ED25519_PUBLIC_KEY_PEM.as_bytes(),
        )),
        allow_local_first_auth,
        ..AuthAdmissionConfig::default()
    }
}

fn local_first_token(
    sub: &str,
    exp: u64,
    iss: Option<&str>,
    aud: Option<&str>,
    app_id: Option<&str>,
) -> String {
    local_first_token_with_subject(Some(sub), exp, iss, aud, app_id)
}

fn local_first_token_with_subject(
    sub: Option<&str>,
    exp: u64,
    iss: Option<&str>,
    aud: Option<&str>,
    app_id: Option<&str>,
) -> String {
    let claims = LocalFirstClaims {
        iss,
        sub,
        exp,
        aud,
        app_id,
    };
    encode(
        &Header::new(Algorithm::EdDSA),
        &claims,
        &EncodingKey::from_ed_pem(ED25519_PRIVATE_KEY_PEM.as_bytes()).unwrap(),
    )
    .unwrap()
}

fn expires_in(seconds: i64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    (now + seconds) as u64
}

#[derive(Serialize)]
struct LocalFirstClaims<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    iss: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sub: Option<&'a str>,
    exp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    aud: Option<&'a str>,
    #[serde(rename = "appId", skip_serializing_if = "Option::is_none")]
    app_id: Option<&'a str>,
}

const ED25519_PRIVATE_KEY_PEM: &str = "\
-----BEGIN PRIVATE KEY-----\n\
MC4CAQAwBQYDK2VwBCIEIGrD/e7uKYqSY4twDEsRfMMuLSrODf14dpTiTK6K1YI0\n\
-----END PRIVATE KEY-----";

const ED25519_PUBLIC_KEY_PEM: &str = "\
-----BEGIN PUBLIC KEY-----\n\
MCowBQYDK2VwAyEA2+Jj2UvNCvQiUPNYRgSi0cJSPiJI6Rs6D0UTeEpQVj8=\n\
-----END PUBLIC KEY-----";
