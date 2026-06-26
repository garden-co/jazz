//! WASM integration tests for jazz-wasm.
//!
//! Run with: wasm-pack test --node

#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

use jazz_wasm::{current_timestamp, derive_user_id, generate_id, mint_anonymous_token};

#[wasm_bindgen_test]
fn test_generate_id() {
    let id1 = generate_id();
    let id2 = generate_id();

    // IDs should be valid UUID format
    assert_eq!(id1.len(), 36);
    assert_eq!(id2.len(), 36);

    // IDs should be unique
    assert_ne!(id1, id2);
}

#[wasm_bindgen_test]
fn test_current_timestamp() {
    let ts1 = current_timestamp();
    let ts2 = current_timestamp();

    // Timestamps should be reasonable (after 2024)
    assert!(ts1 > 1704067200000000); // 2024-01-01 in microseconds

    // Second timestamp should be >= first
    assert!(ts2 >= ts1);
}

#[wasm_bindgen_test]
fn test_identity_helpers_accept_valid_seed() {
    let seed = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    let user_id = derive_user_id(seed.to_string()).expect("derive user id");
    assert!(!user_id.is_empty());

    let token = mint_anonymous_token(
        seed.to_string(),
        "test-audience".to_string(),
        60,
        1_704_067_200,
    )
    .expect("mint anonymous token");
    assert_eq!(token.split('.').count(), 3);
}

#[wasm_bindgen_test]
fn test_identity_helpers_reject_invalid_seed() {
    let err = derive_user_id("not-base64url".to_string()).expect_err("invalid seed should fail");
    let message = err.as_string().unwrap_or_default();
    assert!(message.contains("seed"));
}
