//! Pins the local-first auth wire format shared with the TypeScript package
//! `@garden-co/local-first-auth` (packages/local-first-auth). Both sides test
//! against the same vector file, so neither can drift on its own. Ed25519 is
//! deterministic, so the TypeScript side mints byte-identical tokens.
//!
//! Regenerate (only for an intentional, versioned format change):
//! `JAZZ_WRITE_LOCAL_FIRST_VECTORS=1 cargo test -p jazz-types --test local_first_vectors`

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jazz_types::account_registry::local_first_account_id;
use jazz_types::identity::{
    LOCAL_FIRST_ISSUER, derive_user_id, mint_jazz_self_signed_token_at,
    verify_jazz_self_signed_proof_at,
};
use serde_json::{Value, json};
use uuid::Uuid;

const VECTORS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../../packages/local-first-auth/test-vectors.json"
);
const NOW: u64 = 1_800_000_000;
const APP: &str = "6b0c3f8e-2d9a-4f61-9a57-1c2e3d4f5a6b";
const AUDIENCES: &[&str] = &[
    "6B0C3F8E-2D9A-4F61-9A57-1C2E3D4F5A6B",
    "6b0c3f8e2d9a4f619a571c2e3d4f5a6b",
    "{6b0c3f8e-2d9a-4f61-9a57-1c2e3d4f5a6b}",
    "urn:uuid:6b0c3f8e-2d9a-4f61-9a57-1c2e3d4f5a6b",
    "my-app",
    "https://app.example/jazz",
    "6b0c3f8e-2d9a-4f61-9a57-1c2e3d4f5a6",
];

fn seeds() -> Vec<[u8; 32]> {
    vec![
        [0u8; 32],
        [0xffu8; 32],
        std::array::from_fn(|i| i as u8),
        std::array::from_fn(|i| (i as u8).wrapping_mul(37).wrapping_add(11)),
    ]
}

fn generate() -> Value {
    let cases: Vec<Value> = seeds()
        .iter()
        .map(|seed| {
            let subject = derive_user_id(seed).to_string();
            let token =
                mint_jazz_self_signed_token_at(seed, LOCAL_FIRST_ISSUER, APP, 3600, NOW).unwrap();
            json!({
                "secret": format!("jazz-auth-v1:{}", URL_SAFE_NO_PAD.encode(seed)),
                "subject": subject,
                "founderAccountV1": local_first_account_id(Uuid::parse_str(APP).unwrap(), &subject).0.to_string(),
                "token": token,
            })
        })
        .collect();
    // How each audience form is normalized into `aud`: a UUID in any form Uuid
    // parses becomes its lowercase hyphenated form; anything else becomes
    // uuidv5(DNS, audience).
    let seed = seeds()[2];
    let audiences: Vec<Value> = AUDIENCES
        .iter()
        .map(|audience| {
            let token =
                mint_jazz_self_signed_token_at(&seed, LOCAL_FIRST_ISSUER, audience, 3600, NOW)
                    .unwrap();
            let claims = URL_SAFE_NO_PAD
                .decode(token.split('.').nth(1).unwrap())
                .unwrap();
            let claims: Value = serde_json::from_slice(&claims).unwrap();
            json!({ "audience": audience, "aud": claims["aud"], "token": token })
        })
        .collect();
    json!({
        "now": NOW,
        "audience": APP,
        "ttlSeconds": 3600,
        "cases": cases,
        "audienceSecret": format!("jazz-auth-v1:{}", URL_SAFE_NO_PAD.encode(seed)),
        "audiences": audiences,
    })
}

#[test]
fn local_first_wire_format_matches_shared_vectors() {
    let generated = generate();
    if std::env::var_os("JAZZ_WRITE_LOCAL_FIRST_VECTORS").is_some() {
        let text = serde_json::to_string_pretty(&generated).unwrap() + "\n";
        std::fs::write(VECTORS, text).unwrap();
    }
    let stored: Value = serde_json::from_str(&std::fs::read_to_string(VECTORS).unwrap()).unwrap();
    assert_eq!(
        stored, generated,
        "local-first wire format drifted from {VECTORS}"
    );

    for case in stored["cases"].as_array().unwrap() {
        let verified =
            verify_jazz_self_signed_proof_at(case["token"].as_str().unwrap(), APP, NOW).unwrap();
        assert_eq!(verified.user_id, case["subject"].as_str().unwrap());
    }
    for case in stored["audiences"].as_array().unwrap() {
        let token = case["token"].as_str().unwrap();
        verify_jazz_self_signed_proof_at(token, case["audience"].as_str().unwrap(), NOW).unwrap();
        verify_jazz_self_signed_proof_at(token, case["aud"].as_str().unwrap(), NOW).unwrap();
    }
}
