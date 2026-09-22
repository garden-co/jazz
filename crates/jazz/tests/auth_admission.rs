use jazz::groove::records::Value;
use jazz::ids::{AuthorSubject, AuthorSubjectError};
use jazz::serving::auth_admission::{
    AdmissionSource, AuthAdmissionConfig, AuthAdmissionError, JwtVerifierConfig, admit_bearer_jwt,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::json;

const TEST_JWT_SECRET: &[u8] = b"auth-admission-test-secret";

fn signed_test_jwt(issuer: &str, subject: &str) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &json!({
            "iss": issuer,
            "aud": "jazz-audience",
            "sub": subject,
            "exp": 4_102_444_800_u64,
            "role": "editor",
        }),
        &EncodingKey::from_secret(TEST_JWT_SECRET),
    )
    .unwrap()
}

fn test_jwt_config(issuer: &str) -> AuthAdmissionConfig {
    AuthAdmissionConfig::jwt(JwtVerifierConfig::hmac_secret(
        Algorithm::HS256,
        TEST_JWT_SECRET,
    ))
    .with_expected_issuer(issuer)
    .with_expected_audience("jazz-audience")
}

#[test]
fn signed_external_jwt_preserves_exact_issuer_and_subject() {
    let token = signed_test_jwt(" https://issuer.example ", " user ");
    let admitted = admit_bearer_jwt(
        &test_jwt_config(" https://issuer.example "),
        Some(&token),
        AdmissionSource::AuthorizationHeader,
    )
    .unwrap();

    assert_eq!(admitted.issuer, " https://issuer.example ");
    assert_eq!(admitted.subject, " user ");
    assert_eq!(
        admitted.author.canonical(),
        r#"[" https://issuer.example "," user "]"#
    );
    assert_eq!(
        admitted.claims.get("\0claims:role"),
        Some(&Value::String("editor".to_owned()))
    );
    assert_eq!(
        admitted.claims.get("\0claims:iss"),
        Some(&Value::String(" https://issuer.example ".to_owned()))
    );
    assert_eq!(
        admitted.claims.get("\0claims:sub"),
        Some(&Value::String(" user ".to_owned()))
    );
    assert!(!admitted.claims.contains_key("iss"));
    assert!(!admitted.claims.contains_key("sub"));
}

#[test]
fn signed_external_jwt_rejects_reserved_issuers_during_resolution() {
    for issuer in [
        AuthorSubject::SYSTEM_ISSUER,
        AuthorSubject::LOCAL_FIRST_ISSUER,
        AuthorSubject::STATIC_BEARER_ISSUER,
        AuthorSubject::ANONYMOUS_ISSUER,
    ] {
        let token = signed_test_jwt(issuer, "user");
        assert_eq!(
            admit_bearer_jwt(
                &test_jwt_config(issuer),
                Some(&token),
                AdmissionSource::AuthorizationHeader,
            ),
            Err(AuthAdmissionError::InvalidAuthorSubject(
                AuthorSubjectError::ReservedIssuer(issuer.to_owned())
            ))
        );
    }
}
