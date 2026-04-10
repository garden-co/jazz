#![cfg(feature = "test")]

use axum::http::{HeaderMap, header::AUTHORIZATION};
use jazz_tools::identity;
use jazz_tools::middleware::auth::{AuthConfig, extract_session};
use jazz_tools::schema_manager::AppId;

fn test_app_id() -> AppId {
    AppId::from_name("self-signed-integration-test")
}

fn alice_seed() -> [u8; 32] {
    let mut seed = [0u8; 32];
    seed[0] = 0xAA;
    seed[31] = 0x01;
    seed
}

fn bob_seed() -> [u8; 32] {
    let mut seed = [0u8; 32];
    seed[0] = 0xBB;
    seed[31] = 0x02;
    seed
}

fn self_signed_config() -> AuthConfig {
    AuthConfig {
        allow_self_signed: true,
        ..Default::default()
    }
}

fn bearer_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap());
    headers
}

#[tokio::test]
async fn valid_self_signed_jwt_authenticates() {
    let app_id = test_app_id();
    let audience = app_id.to_string();
    let token = identity::mint_self_signed_token(&alice_seed(), &audience, 3600).unwrap();

    let headers = bearer_headers(&token);
    let config = self_signed_config();

    let result = extract_session(&headers, app_id, &config, None).await;

    let session = result
        .expect("should succeed")
        .expect("should have session");
    let expected_user_id = identity::derive_user_id(&alice_seed()).to_string();
    assert_eq!(session.user_id, expected_user_id);
}

#[tokio::test]
async fn same_seed_same_identity() {
    let app_id = test_app_id();
    let audience = app_id.to_string();

    let token1 = identity::mint_self_signed_token(&alice_seed(), &audience, 3600).unwrap();
    let token2 = identity::mint_self_signed_token(&alice_seed(), &audience, 3600).unwrap();

    let config = self_signed_config();

    let session1 = extract_session(&bearer_headers(&token1), app_id, &config, None)
        .await
        .unwrap()
        .unwrap();
    let session2 = extract_session(&bearer_headers(&token2), app_id, &config, None)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        session1.user_id, session2.user_id,
        "same seed must always yield the same user identity"
    );
}

#[tokio::test]
async fn different_seeds_different_identities() {
    let app_id = test_app_id();
    let audience = app_id.to_string();

    let alice_token = identity::mint_self_signed_token(&alice_seed(), &audience, 3600).unwrap();
    let bob_token = identity::mint_self_signed_token(&bob_seed(), &audience, 3600).unwrap();

    let config = self_signed_config();

    let alice_session = extract_session(&bearer_headers(&alice_token), app_id, &config, None)
        .await
        .unwrap()
        .unwrap();
    let bob_session = extract_session(&bearer_headers(&bob_token), app_id, &config, None)
        .await
        .unwrap()
        .unwrap();

    assert_ne!(
        alice_session.user_id, bob_session.user_id,
        "different seeds must yield different user identities"
    );
}

#[tokio::test]
async fn wrong_audience_rejected() {
    let token =
        identity::mint_self_signed_token(&alice_seed(), "wrong-app-audience", 3600).unwrap();

    let headers = bearer_headers(&token);
    let config = self_signed_config();
    let app_id = test_app_id();

    let result = extract_session(&headers, app_id, &config, None).await;

    assert!(
        result.is_err(),
        "token minted for a different audience must be rejected"
    );
}

#[tokio::test]
async fn self_signed_disabled_rejected() {
    let app_id = test_app_id();
    let audience = app_id.to_string();
    let token = identity::mint_self_signed_token(&alice_seed(), &audience, 3600).unwrap();

    let headers = bearer_headers(&token);
    let config = AuthConfig {
        allow_self_signed: false,
        ..Default::default()
    };

    let result = extract_session(&headers, app_id, &config, None).await;

    assert!(
        result.is_err(),
        "self-signed token must be rejected when allow_self_signed is false"
    );
}

#[tokio::test]
async fn expired_token_rejected() {
    let app_id = test_app_id();
    let audience = app_id.to_string();
    // TTL=0 means exp == iat, which is already expired
    let token = identity::mint_self_signed_token(&alice_seed(), &audience, 0).unwrap();

    let headers = bearer_headers(&token);
    let config = self_signed_config();

    let result = extract_session(&headers, app_id, &config, None).await;

    assert!(
        result.is_err(),
        "token with TTL=0 must be rejected as expired"
    );
}
