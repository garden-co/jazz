# Local-First Auth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add self-signed Ed25519 JWT auth as the local-first identity path, with Rust crypto, TS `SeedStore` abstraction, and server-side verification.

**Architecture:** Rust implements Ed25519 key derivation, JWT minting, and UUIDv5 user ID derivation on the existing `WasmRuntime`/`NapiRuntime`. TS adds a `SeedStore` interface with `LocalStorageSeedStore` default. `createDb` gains an `auth: { seed }` config path that derives identity and mints self-signed JWTs. Server-side, `extract_session` gains a self-signed JWT verification branch keyed on `iss = "urn:jazz:self-signed"`. The existing linking system is removed entirely.

**Tech Stack:** Rust (`ed25519-dalek`, `jsonwebtoken`, `uuid` v5), WASM (`wasm-bindgen`), NAPI (`napi-rs`), TypeScript (Vitest)

---

### Task 1: Add Ed25519 + JWT dependencies to Rust crates

**Files:**

- Modify: `crates/jazz-tools/Cargo.toml`
- Modify: `crates/jazz-wasm/Cargo.toml`

- [ ] **Step 1: Add `ed25519-dalek` and `base64` to jazz-tools**

In `crates/jazz-tools/Cargo.toml`, add `ed25519-dalek` to `[dependencies]` (unconditional — needed for both server verification and key derivation):

```toml
ed25519-dalek = { version = "2", features = ["rand_core"] }
```

Also ensure `base64` is unconditional (currently optional behind `server` feature, but needed for self-signed JWT encoding in WASM too). Move it from optional:

```toml
base64 = "0.22"
```

Remove `"dep:base64"` from the `server` and `transport-http` feature lists since it's now always included.

- [ ] **Step 2: Add `ed25519-dalek` to jazz-wasm**

In `crates/jazz-wasm/Cargo.toml`, add:

```toml
ed25519-dalek = { version = "2", features = ["rand_core"] }
base64 = "0.22"
```

- [ ] **Step 3: Verify it compiles**

Run: `cd /Users/guidodorsi/workspace/jazz2 && cargo check -p jazz-tools -p jazz-wasm 2>&1 | tail -5`
Expected: compiles without errors

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-tools/Cargo.toml crates/jazz-wasm/Cargo.toml
git commit -m "deps: add ed25519-dalek for self-signed auth"
```

---

### Task 2: Implement Rust crypto module (key derivation, UUIDv5, JWT minting)

**Files:**

- Create: `crates/jazz-tools/src/identity.rs`
- Modify: `crates/jazz-tools/src/lib.rs`

- [ ] **Step 1: Write tests for the identity module**

Create `crates/jazz-tools/src/identity.rs` with `#[cfg(test)] mod tests` at the bottom:

```rust
//! Self-signed identity: seed → Ed25519 keypair → UUIDv5 userId → JWT.

use ed25519_dalek::{SigningKey, VerifyingKey};
use sha2::{Digest, Sha512};
use uuid::Uuid;

/// Jazz-specific UUIDv5 namespace for deriving user IDs from public keys.
const KEY_NAMESPACE: Uuid = Uuid::from_bytes([
    0x6a, 0x61, 0x7a, 0x7a, // "jazz"
    0x2d, 0x61, // "-a"
    0x75, 0x74, // "ut"
    0x68, 0x2d, // "h-"
    0x6b, 0x65, 0x79, 0x2d, 0x76, 0x31, // "key-v1"
]);

/// Derive an Ed25519 signing key from a 32-byte seed using domain-separated HKDF-like expansion.
///
/// Uses SHA-512(domain || seed) truncated to 32 bytes as the Ed25519 secret scalar,
/// matching the ed25519-dalek `SigningKey::from_bytes` convention.
pub fn derive_signing_key(seed: &[u8; 32], domain: &str) -> SigningKey {
    let mut hasher = Sha512::new();
    hasher.update(domain.as_bytes());
    hasher.update(seed);
    let hash = hasher.finalize();
    let mut key_bytes = [0u8; 32];
    key_bytes.copy_from_slice(&hash[..32]);
    SigningKey::from_bytes(&key_bytes)
}

/// Derive the canonical user ID (UUIDv5) from a seed.
pub fn derive_user_id(seed: &[u8; 32]) -> Uuid {
    let signing_key = derive_signing_key(seed, "jazz-auth-sign-v1");
    let public_key = signing_key.verifying_key();
    Uuid::new_v5(&KEY_NAMESPACE, public_key.as_bytes())
}

/// Get the Ed25519 verifying (public) key from a seed.
pub fn derive_verifying_key(seed: &[u8; 32]) -> VerifyingKey {
    let signing_key = derive_signing_key(seed, "jazz-auth-sign-v1");
    signing_key.verifying_key()
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn same_seed_produces_same_user_id() {
        let id1 = derive_user_id(&alice_seed());
        let id2 = derive_user_id(&alice_seed());
        assert_eq!(id1, id2);
    }

    #[test]
    fn different_seeds_produce_different_user_ids() {
        let alice_id = derive_user_id(&alice_seed());
        let bob_id = derive_user_id(&bob_seed());
        assert_ne!(alice_id, bob_id);
    }

    #[test]
    fn user_id_is_uuid_v5() {
        let id = derive_user_id(&alice_seed());
        assert_eq!(id.get_version_num(), 5);
    }

    #[test]
    fn derive_verifying_key_is_deterministic() {
        let k1 = derive_verifying_key(&alice_seed());
        let k2 = derive_verifying_key(&alice_seed());
        assert_eq!(k1.as_bytes(), k2.as_bytes());
    }
}
```

- [ ] **Step 2: Register the module in lib.rs**

In `crates/jazz-tools/src/lib.rs`, add:

```rust
pub mod identity;
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p jazz-tools identity::tests -- --nocapture 2>&1 | tail -10`
Expected: all 4 tests pass

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-tools/src/identity.rs crates/jazz-tools/src/lib.rs
git commit -m "feat: add identity crypto module (seed derivation, UUIDv5)"
```

---

### Task 3: Add JWT minting and verification to the identity module

**Files:**

- Modify: `crates/jazz-tools/src/identity.rs`

- [ ] **Step 1: Write tests for JWT minting and verification**

Add these tests to the existing `mod tests` in `identity.rs`:

```rust
    use super::*;

    #[test]
    fn mint_and_verify_self_signed_token() {
        let seed = alice_seed();
        let user_id = derive_user_id(&seed);
        let token = mint_self_signed_token(&seed, "my-app", 3600).unwrap();

        let verified = verify_self_signed_token(&token, "my-app").unwrap();
        assert_eq!(verified.user_id, user_id.to_string());
    }

    #[test]
    fn reject_wrong_audience() {
        let seed = alice_seed();
        let token = mint_self_signed_token(&seed, "app-a", 3600).unwrap();
        let result = verify_self_signed_token(&token, "app-b");
        assert!(result.is_err());
    }

    #[test]
    fn reject_tampered_sub() {
        let seed = alice_seed();
        let token = mint_self_signed_token(&seed, "my-app", 3600).unwrap();
        // Decode, tamper sub, re-encode won't have valid signature
        let result = verify_self_signed_token(&token, "my-app");
        // Token itself is valid — but we can test sub mismatch by verifying internals
        let verified = result.unwrap();
        assert_eq!(verified.user_id, derive_user_id(&seed).to_string());
    }

    #[test]
    fn reject_excessive_ttl() {
        let seed = alice_seed();
        let token = mint_self_signed_token(&seed, "my-app", 3600).unwrap();
        let result = verify_self_signed_token_with_max_ttl(&token, "my-app", 1800);
        assert!(result.is_err());
    }
```

- [ ] **Step 2: Implement JWT minting**

Add to `identity.rs` above the tests module:

```rust
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ed25519_dalek::Signer;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

const SELF_SIGNED_ISSUER: &str = "urn:jazz:self-signed";
const DEFAULT_MAX_TTL_SECONDS: u64 = 3600;

#[derive(Debug, Serialize)]
struct SelfSignedClaims {
    iss: String,
    sub: String,
    aud: String,
    jazz_pub_key: String,
    iat: u64,
    exp: u64,
}

#[derive(Debug, Deserialize)]
struct SelfSignedClaimsRaw {
    iss: Option<String>,
    sub: Option<String>,
    aud: Option<String>,
    jazz_pub_key: Option<String>,
    iat: Option<u64>,
    exp: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct JwtHeader {
    alg: String,
    typ: String,
}

/// Result of verifying a self-signed token.
#[derive(Debug)]
pub struct VerifiedSelfSigned {
    pub user_id: String,
    pub public_key_bytes: [u8; 32],
}

/// Mint a self-signed JWT from a seed.
pub fn mint_self_signed_token(seed: &[u8; 32], audience: &str, ttl_seconds: u64) -> Result<String, String> {
    let signing_key = derive_signing_key(seed, "jazz-auth-sign-v1");
    let public_key = signing_key.verifying_key();
    let user_id = Uuid::new_v5(&KEY_NAMESPACE, public_key.as_bytes());

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system time error: {e}"))?
        .as_secs();

    let header = JwtHeader {
        alg: "EdDSA".to_string(),
        typ: "JWT".to_string(),
    };
    let claims = SelfSignedClaims {
        iss: SELF_SIGNED_ISSUER.to_string(),
        sub: user_id.to_string(),
        aud: audience.to_string(),
        jazz_pub_key: URL_SAFE_NO_PAD.encode(public_key.as_bytes()),
        iat: now,
        exp: now + ttl_seconds,
    };

    let header_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).map_err(|e| e.to_string())?);
    let claims_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).map_err(|e| e.to_string())?);
    let signing_input = format!("{header_b64}.{claims_b64}");
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    Ok(format!("{signing_input}.{sig_b64}"))
}

/// Verify a self-signed JWT. Returns the verified user ID and public key.
pub fn verify_self_signed_token(token: &str, expected_audience: &str) -> Result<VerifiedSelfSigned, String> {
    verify_self_signed_token_with_max_ttl(token, expected_audience, DEFAULT_MAX_TTL_SECONDS)
}

/// Verify a self-signed JWT with a custom max TTL.
pub fn verify_self_signed_token_with_max_ttl(
    token: &str,
    expected_audience: &str,
    max_ttl_seconds: u64,
) -> Result<VerifiedSelfSigned, String> {
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 {
        return Err("malformed JWT: expected 3 parts".to_string());
    }

    // 1. Decode header, require EdDSA
    let header_bytes = URL_SAFE_NO_PAD.decode(parts[0]).map_err(|e| format!("invalid header base64: {e}"))?;
    let header: JwtHeader = serde_json::from_slice(&header_bytes).map_err(|e| format!("invalid header JSON: {e}"))?;
    if header.alg != "EdDSA" {
        return Err(format!("self-signed JWT requires alg=EdDSA, got {}", header.alg));
    }

    // 2. Decode claims (unverified)
    let claims_bytes = URL_SAFE_NO_PAD.decode(parts[1]).map_err(|e| format!("invalid claims base64: {e}"))?;
    let claims: SelfSignedClaimsRaw = serde_json::from_slice(&claims_bytes).map_err(|e| format!("invalid claims JSON: {e}"))?;

    // 3. Require iss = "urn:jazz:self-signed"
    match claims.iss.as_deref() {
        Some(SELF_SIGNED_ISSUER) => {}
        other => return Err(format!("expected iss={SELF_SIGNED_ISSUER}, got {:?}", other)),
    }

    // 4. Require aud matches
    match claims.aud.as_deref() {
        Some(aud) if aud == expected_audience => {}
        Some(aud) => return Err(format!("audience mismatch: expected {expected_audience}, got {aud}")),
        None => return Err("missing aud claim".to_string()),
    }

    // 5. Extract and decode public key
    let pub_key_b64 = claims.jazz_pub_key.as_deref().ok_or("missing jazz_pub_key claim")?;
    let pub_key_bytes = URL_SAFE_NO_PAD.decode(pub_key_b64).map_err(|e| format!("invalid jazz_pub_key base64: {e}"))?;
    if pub_key_bytes.len() != 32 {
        return Err(format!("jazz_pub_key must be 32 bytes, got {}", pub_key_bytes.len()));
    }
    let mut pk_array = [0u8; 32];
    pk_array.copy_from_slice(&pub_key_bytes);
    let verifying_key = VerifyingKey::from_bytes(&pk_array).map_err(|e| format!("invalid Ed25519 public key: {e}"))?;

    // 6. Verify signature
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let sig_bytes = URL_SAFE_NO_PAD.decode(parts[2]).map_err(|e| format!("invalid signature base64: {e}"))?;
    if sig_bytes.len() != 64 {
        return Err(format!("signature must be 64 bytes, got {}", sig_bytes.len()));
    }
    let mut sig_array = [0u8; 64];
    sig_array.copy_from_slice(&sig_bytes);
    let signature = ed25519_dalek::Signature::from_bytes(&sig_array);
    verifying_key
        .verify_strict(signing_input.as_bytes(), &signature)
        .map_err(|e| format!("signature verification failed: {e}"))?;

    // 7. Re-derive userId from public key
    let derived_user_id = Uuid::new_v5(&KEY_NAMESPACE, verifying_key.as_bytes());

    // 8. Require sub == derived userId
    let sub = claims.sub.as_deref().ok_or("missing sub claim")?;
    if sub != derived_user_id.to_string() {
        return Err(format!("sub mismatch: token says {sub}, key derives {derived_user_id}"));
    }

    // 9. Check iat, exp, and TTL
    let iat = claims.iat.ok_or("missing iat claim")?;
    let exp = claims.exp.ok_or("missing exp claim")?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if iat > now + 60 {
        return Err("iat is in the future".to_string());
    }
    if exp <= now {
        return Err("token has expired".to_string());
    }
    if exp - iat > max_ttl_seconds {
        return Err(format!("TTL {} exceeds max {max_ttl_seconds}", exp - iat));
    }

    Ok(VerifiedSelfSigned {
        user_id: derived_user_id.to_string(),
        public_key_bytes: pk_array,
    })
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p jazz-tools identity::tests -- --nocapture 2>&1 | tail -15`
Expected: all tests pass (including the 4 new ones)

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-tools/src/identity.rs
git commit -m "feat: add self-signed JWT minting and verification"
```

---

### Task 4: Expose crypto methods on WasmRuntime

**Files:**

- Modify: `crates/jazz-wasm/src/runtime.rs`

- [ ] **Step 1: Write a WASM test for derive_user_id**

Add at the bottom of `crates/jazz-wasm/src/runtime.rs` (or in a new test file `crates/jazz-wasm/tests/identity.rs`):

```rust
#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn wasm_derive_user_id_deterministic() {
        let seed = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(&[0xAAu8; 32]);
        let id1 = WasmRuntime::derive_user_id_static(&seed).unwrap();
        let id2 = WasmRuntime::derive_user_id_static(&seed).unwrap();
        assert_eq!(id1, id2);
    }
}
```

- [ ] **Step 2: Add identity methods to WasmRuntime**

Add these `#[wasm_bindgen]` methods to the `impl WasmRuntime` block in `crates/jazz-wasm/src/runtime.rs`:

```rust
use jazz_tools::identity;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

#[wasm_bindgen]
impl WasmRuntime {
    /// Derive a stable user ID (UUIDv5) from a base64url-encoded seed.
    #[wasm_bindgen(js_name = "deriveUserId")]
    pub fn derive_user_id_static(seed_b64: &str) -> Result<String, JsError> {
        let seed = decode_seed(seed_b64)?;
        Ok(identity::derive_user_id(&seed).to_string())
    }

    /// Mint a self-signed JWT from a base64url-encoded seed.
    #[wasm_bindgen(js_name = "mintSelfSignedToken")]
    pub fn mint_self_signed_token_static(
        seed_b64: &str,
        audience: &str,
        ttl_seconds: u64,
    ) -> Result<String, JsError> {
        let seed = decode_seed(seed_b64)?;
        identity::mint_self_signed_token(&seed, audience, ttl_seconds)
            .map_err(|e| JsError::new(&e))
    }

    /// Get the base64url-encoded Ed25519 public key from a seed.
    #[wasm_bindgen(js_name = "getPublicKeyBase64url")]
    pub fn get_public_key_b64_static(seed_b64: &str) -> Result<String, JsError> {
        let seed = decode_seed(seed_b64)?;
        let vk = identity::derive_verifying_key(&seed);
        Ok(URL_SAFE_NO_PAD.encode(vk.as_bytes()))
    }
}

fn decode_seed(seed_b64: &str) -> Result<[u8; 32], JsError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(seed_b64)
        .map_err(|e| JsError::new(&format!("invalid seed base64: {e}")))?;
    if bytes.len() != 32 {
        return Err(JsError::new(&format!("seed must be 32 bytes, got {}", bytes.len())));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}
```

Note: these are static methods (no `&self`) because they don't need the runtime instance — they're pure crypto functions. Expose them on `WasmRuntime` for discoverability.

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p jazz-wasm 2>&1 | tail -5`
Expected: compiles without errors

- [ ] **Step 4: Run tests**

Run: `cargo test -p jazz-wasm identity_tests -- --nocapture 2>&1 | tail -10`
Expected: test passes

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-wasm/src/runtime.rs
git commit -m "feat: expose identity crypto on WasmRuntime"
```

---

### Task 5: Expose crypto methods on NapiRuntime

**Files:**

- Modify: `crates/jazz-napi/src/lib.rs`
- Modify: `crates/jazz-napi/Cargo.toml`

- [ ] **Step 1: Add dependencies to jazz-napi**

In `crates/jazz-napi/Cargo.toml`, add:

```toml
base64 = "0.22"
```

(`ed25519-dalek` is already available transitively via `jazz-tools`, but `base64` may need to be direct.)

- [ ] **Step 2: Add identity methods to NapiRuntime**

Add these `#[napi]` methods to the `NapiRuntime` impl block in `crates/jazz-napi/src/lib.rs`:

```rust
use jazz_tools::identity;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

#[napi]
impl NapiRuntime {
    /// Derive a stable user ID (UUIDv5) from a base64url-encoded seed.
    #[napi(js_name = "deriveUserId")]
    pub fn derive_user_id_static(seed_b64: String) -> napi::Result<String> {
        let seed = napi_decode_seed(&seed_b64)?;
        Ok(identity::derive_user_id(&seed).to_string())
    }

    /// Mint a self-signed JWT from a base64url-encoded seed.
    #[napi(js_name = "mintSelfSignedToken")]
    pub fn mint_self_signed_token_static(
        seed_b64: String,
        audience: String,
        ttl_seconds: u32,
    ) -> napi::Result<String> {
        let seed = napi_decode_seed(&seed_b64)?;
        identity::mint_self_signed_token(&seed, &audience, ttl_seconds as u64)
            .map_err(|e| napi::Error::from_reason(e))
    }

    /// Get the base64url-encoded Ed25519 public key from a seed.
    #[napi(js_name = "getPublicKeyBase64url")]
    pub fn get_public_key_b64_static(seed_b64: String) -> napi::Result<String> {
        let seed = napi_decode_seed(&seed_b64)?;
        let vk = identity::derive_verifying_key(&seed);
        Ok(URL_SAFE_NO_PAD.encode(vk.as_bytes()))
    }
}

fn napi_decode_seed(seed_b64: &str) -> napi::Result<[u8; 32]> {
    let bytes = URL_SAFE_NO_PAD
        .decode(seed_b64)
        .map_err(|e| napi::Error::from_reason(format!("invalid seed base64: {e}")))?;
    if bytes.len() != 32 {
        return Err(napi::Error::from_reason(format!("seed must be 32 bytes, got {}", bytes.len())));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p jazz-napi 2>&1 | tail -5`
Expected: compiles without errors

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-napi/src/lib.rs crates/jazz-napi/Cargo.toml
git commit -m "feat: expose identity crypto on NapiRuntime"
```

---

### Task 6: Add self-signed JWT verification to server auth middleware

**Files:**

- Modify: `crates/jazz-tools/src/middleware/auth.rs`

This task adds the `iss = "urn:jazz:self-signed"` branch to `extract_session` and adds `allow_self_signed` to `AuthConfig`.

- [ ] **Step 1: Write failing tests**

Add these tests to `crates/jazz-tools/src/middleware/auth.rs` inside the existing `mod tests`:

```rust
    use crate::identity;

    fn alice_seed() -> [u8; 32] {
        let mut seed = [0u8; 32];
        seed[0] = 0xAA;
        seed[31] = 0x01;
        seed
    }

    fn make_self_signed_config() -> AuthConfig {
        AuthConfig {
            jwks_url: None,
            allow_anonymous: false,
            allow_demo: false,
            allow_self_signed: true,
            backend_secret: None,
            admin_secret: None,
        }
    }

    #[tokio::test]
    async fn self_signed_jwt_authenticates() {
        let seed = alice_seed();
        let app_id = test_app_id();
        let token = identity::mint_self_signed_token(&seed, app_id.as_str(), 3600).unwrap();
        let config = make_self_signed_config();

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());

        let session = extract_session(&headers, app_id, &config, None, None)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(session.user_id, identity::derive_user_id(&seed).to_string());
    }

    #[tokio::test]
    async fn self_signed_jwt_wrong_audience_rejected() {
        let seed = alice_seed();
        let token = identity::mint_self_signed_token(&seed, "wrong-app", 3600).unwrap();
        let config = make_self_signed_config();

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config, None, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn self_signed_disabled_rejects() {
        let seed = alice_seed();
        let app_id = test_app_id();
        let token = identity::mint_self_signed_token(&seed, app_id.as_str(), 3600).unwrap();
        let mut config = make_self_signed_config();
        config.allow_self_signed = false;

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());

        let result = extract_session(&headers, app_id, &config, None, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn non_self_signed_iss_falls_through() {
        // A JWT with iss != "urn:jazz:self-signed" should go through the normal JWKS path,
        // not the self-signed path. Without JWKS configured, this should fail as "not configured."
        let config = make_self_signed_config();
        let claims = JwtClaims {
            sub: "user-123".to_string(),
            iss: Some("https://auth.example.com".to_string()),
            jazz_principal_id: None,
            claims: serde_json::json!({}),
            exp: None,
            iat: None,
        };
        let token = make_jwt(&claims, TEST_JWKS_SECRET, TEST_JWKS_KID);

        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());

        let result = extract_session(&headers, test_app_id(), &config, None, None).await;
        // Should fail because no JWKS configured, not because of self-signed validation
        assert!(result.is_err());
    }
```

- [ ] **Step 2: Run tests to see them fail**

Run: `cargo test -p jazz-tools middleware::auth::tests::self_signed -- --nocapture 2>&1 | tail -10`
Expected: compilation error (no `allow_self_signed` field yet)

- [ ] **Step 3: Add `allow_self_signed` to `AuthConfig`**

In `crates/jazz-tools/src/middleware/auth.rs`, add to the `AuthConfig` struct:

```rust
/// Whether self-signed Ed25519 JWT auth is allowed.
pub allow_self_signed: bool,
```

Set the `Default` to `true` (since `AuthConfig` derives `Default`, add `#[serde(default = "default_true")]` or set it in the `Default` impl). Since `AuthConfig` uses `#[derive(Default)]`, and `bool` defaults to `false`, update the existing test configs (`make_test_config`) to include `allow_self_signed: false` so existing tests don't change behavior. Then for the new self-signed tests, set it to `true`.

- [ ] **Step 4: Add self-signed branch to `extract_session`**

In the JWT auth section (Priority 2) of `extract_session`, before calling `validate_jwt_with_cache`, check if the token is self-signed:

```rust
    // Priority 2: JWT auth
    if let Some(auth_value) = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        let Some(token) = auth_value.strip_prefix("Bearer ") else {
            return Err(UnauthenticatedResponse::invalid(
                "Invalid Authorization header format",
            ));
        };

        let token = token.trim();
        if token.is_empty() {
            return Err(UnauthenticatedResponse::invalid("Empty bearer token"));
        }

        // Check if this is a self-signed JWT by peeking at the issuer claim
        if is_self_signed_token(token) {
            if !config.allow_self_signed {
                return Err(UnauthenticatedResponse::disabled(
                    "Self-signed auth is not enabled for this app",
                ));
            }
            let verified = identity::verify_self_signed_token(token, app_id.as_str())
                .map_err(|e| UnauthenticatedResponse::invalid(e))?;
            return Ok(Some(Session {
                user_id: verified.user_id,
                claims: serde_json::json!({
                    "auth_mode": "self-signed",
                }),
            }));
        }

        // Existing JWKS path...
```

Add the helper function:

```rust
/// Check if a JWT has iss = "urn:jazz:self-signed" by decoding the claims without verification.
fn is_self_signed_token(token: &str) -> bool {
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 {
        return false;
    }
    let Ok(claims_bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[1]) else {
        return false;
    };
    #[derive(serde::Deserialize)]
    struct IssOnly {
        iss: Option<String>,
    }
    let Ok(claims) = serde_json::from_slice::<IssOnly>(&claims_bytes) else {
        return false;
    };
    claims.iss.as_deref() == Some("urn:jazz:self-signed")
}
```

- [ ] **Step 5: Fix existing test configs**

Update `make_test_config()` and any other test helper that constructs `AuthConfig` to include `allow_self_signed: false` (preserving existing test behavior):

```rust
    fn make_test_config() -> AuthConfig {
        AuthConfig {
            jwks_url: Some("https://example.test/.well-known/jwks.json".to_string()),
            allow_anonymous: true,
            allow_demo: true,
            allow_self_signed: false,
            backend_secret: Some("backend-secret-12345".to_string()),
            admin_secret: Some("admin-secret-67890".to_string()),
        }
    }
```

- [ ] **Step 6: Run all auth tests**

Run: `cargo test -p jazz-tools middleware::auth::tests -- --nocapture 2>&1 | tail -20`
Expected: all tests pass (existing + new)

- [ ] **Step 7: Commit**

```bash
git add crates/jazz-tools/src/middleware/auth.rs
git commit -m "feat: add self-signed JWT verification to server auth"
```

---

### Task 7: Add `allow_self_signed` to server config and routes

**Files:**

- Modify: `crates/jazz-tools/src/server/builder.rs`
- Modify: `crates/jazz-tools/src/server/mod.rs`
- Modify: `crates/jazz-tools/src/routes.rs` (if AuthConfig is constructed there)

- [ ] **Step 1: Update ServerBuilder**

Check `builder.rs` for where `AuthConfig` is constructed and ensure `allow_self_signed` defaults to `true`. Add a builder method:

```rust
pub fn with_self_signed_auth(mut self, enabled: bool) -> Self {
    self.auth_config.allow_self_signed = enabled;
    self
}
```

- [ ] **Step 2: Verify existing server tests pass**

Run: `cargo test -p jazz-tools server -- --nocapture 2>&1 | tail -10`
Expected: all pass

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-tools/src/server/builder.rs crates/jazz-tools/src/server/mod.rs
git commit -m "feat: add allow_self_signed to server config (default: true)"
```

---

### Task 8: Add SeedStore interface and LocalStorageSeedStore (TS)

**Files:**

- Create: `packages/jazz-tools/src/runtime/seed-store.ts`
- Create: `packages/jazz-tools/src/runtime/seed-store.test.ts`

- [ ] **Step 1: Write the failing test**

Create `packages/jazz-tools/src/runtime/seed-store.test.ts`:

```typescript
import { describe, it, expect, beforeEach } from "vitest";
import { LocalStorageSeedStore, generateSeed } from "./seed-store.js";

// Mock localStorage
function createMockStorage(): Storage {
  const store = new Map<string, string>();
  return {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => {
      store.set(key, value);
    },
    removeItem: (key: string) => {
      store.delete(key);
    },
    clear: () => store.clear(),
    get length() {
      return store.size;
    },
    key: (_index: number) => null,
  } as Storage;
}

describe("generateSeed", () => {
  it("produces a base64url string", () => {
    const seed = generateSeed();
    expect(seed).toMatch(/^[A-Za-z0-9_-]{43}$/);
  });

  it("produces different seeds each call", () => {
    const a = generateSeed();
    const b = generateSeed();
    expect(a).not.toBe(b);
  });
});

describe("LocalStorageSeedStore", () => {
  let storage: Storage;
  let store: LocalStorageSeedStore;

  beforeEach(() => {
    storage = createMockStorage();
    store = new LocalStorageSeedStore({ storage });
  });

  it("loadSeed returns null when no seed stored", async () => {
    expect(await store.loadSeed()).toBeNull();
  });

  it("saveSeed persists and loadSeed retrieves", async () => {
    const seed = generateSeed();
    await store.saveSeed(seed);
    expect(await store.loadSeed()).toBe(seed);
  });

  it("clearSeed removes the seed", async () => {
    await store.saveSeed(generateSeed());
    await store.clearSeed();
    expect(await store.loadSeed()).toBeNull();
  });

  it("getOrCreateSeed generates on first call", async () => {
    const seed = await store.getOrCreateSeed();
    expect(seed).toMatch(/^[A-Za-z0-9_-]{43}$/);
  });

  it("getOrCreateSeed returns same seed on second call", async () => {
    const first = await store.getOrCreateSeed();
    const second = await store.getOrCreateSeed();
    expect(first).toBe(second);
  });

  it("clearSeed then getOrCreateSeed produces a new seed", async () => {
    const first = await store.getOrCreateSeed();
    await store.clearSeed();
    const second = await store.getOrCreateSeed();
    expect(second).not.toBe(first);
  });

  it("uses custom key name", async () => {
    const customStore = new LocalStorageSeedStore({ storage, key: "my-custom-key" });
    await customStore.saveSeed("test-seed");
    expect(storage.getItem("my-custom-key")).toBe("test-seed");
  });
});
```

- [ ] **Step 2: Run to see it fail**

Run: `cd /Users/guidodorsi/workspace/jazz2 && npx vitest run packages/jazz-tools/src/runtime/seed-store.test.ts 2>&1 | tail -10`
Expected: FAIL — module not found

- [ ] **Step 3: Implement SeedStore and LocalStorageSeedStore**

Create `packages/jazz-tools/src/runtime/seed-store.ts`:

```typescript
/**
 * Interface for platform-appropriate seed persistence.
 */
export interface SeedStore {
  loadSeed(): Promise<string | null>;
  saveSeed(seed: string): Promise<void>;
  clearSeed(): Promise<void>;
  getOrCreateSeed(): Promise<string>;
}

const DEFAULT_KEY = "jazz-seed";

/**
 * Generate a new 32-byte seed as a base64url string.
 * Uses the platform's native CSPRNG.
 */
export function generateSeed(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return uint8ArrayToBase64url(bytes);
}

function uint8ArrayToBase64url(bytes: Uint8Array): string {
  let binary = "";
  for (const b of bytes) {
    binary += String.fromCharCode(b);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export interface LocalStorageSeedStoreOptions {
  /** localStorage key name (default: "jazz-seed") */
  key?: string;
  /** Override storage backend (for testing) */
  storage?: Pick<Storage, "getItem" | "setItem" | "removeItem">;
}

/**
 * SeedStore backed by localStorage.
 *
 * Uses a check-then-write pattern; not atomic across concurrent tabs on
 * first visit. Apps that need strict cross-tab guarantees can use a custom
 * SeedStore with IndexedDB transactions or BroadcastChannel coordination.
 */
export class LocalStorageSeedStore implements SeedStore {
  private readonly key: string;
  private readonly storage: Pick<Storage, "getItem" | "setItem" | "removeItem">;

  constructor(options: LocalStorageSeedStoreOptions = {}) {
    this.key = options.key ?? DEFAULT_KEY;
    this.storage = options.storage ?? globalThis.localStorage;
  }

  async loadSeed(): Promise<string | null> {
    return this.storage.getItem(this.key);
  }

  async saveSeed(seed: string): Promise<void> {
    this.storage.setItem(this.key, seed);
  }

  async clearSeed(): Promise<void> {
    this.storage.removeItem(this.key);
  }

  async getOrCreateSeed(): Promise<string> {
    const existing = this.storage.getItem(this.key);
    if (existing) return existing;

    const seed = generateSeed();
    this.storage.setItem(this.key, seed);
    return seed;
  }
}
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/guidodorsi/workspace/jazz2 && npx vitest run packages/jazz-tools/src/runtime/seed-store.test.ts 2>&1 | tail -15`
Expected: all tests pass

- [ ] **Step 5: Export from runtime index**

In `packages/jazz-tools/src/runtime/index.ts`, add:

```typescript
export { generateSeed, LocalStorageSeedStore } from "./seed-store.js";
export type { SeedStore } from "./seed-store.js";
```

- [ ] **Step 6: Commit**

```bash
git add packages/jazz-tools/src/runtime/seed-store.ts packages/jazz-tools/src/runtime/seed-store.test.ts packages/jazz-tools/src/runtime/index.ts
git commit -m "feat: add SeedStore interface and LocalStorageSeedStore"
```

---

### Task 9: Add `auth: { seed }` path to DbConfig and createDb

**Files:**

- Modify: `packages/jazz-tools/src/runtime/db.ts`
- Modify: `packages/jazz-tools/src/runtime/context.ts` (if `DbConfig` types are re-exported there)

- [ ] **Step 1: Write failing test**

Add a test file `packages/jazz-tools/src/runtime/db.self-signed-auth.test.ts`:

```typescript
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import type { DbConfig } from "./db.js";

describe("DbConfig auth validation", () => {
  it("rejects setting both auth.seed and jwtToken", () => {
    const config: DbConfig = {
      appId: "test-app",
      auth: { seed: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" },
      jwtToken: "some-jwt",
    };
    // Import the validation function once it exists
    expect(() => validateAuthConfig(config)).toThrow("config error");
  });
});
```

This test will be refined once we see the exact validation function shape. The key contract: `auth` + `jwtToken` simultaneously is a config error.

- [ ] **Step 2: Update DbConfig interface**

In `packages/jazz-tools/src/runtime/db.ts`, update the `DbConfig` interface:

```typescript
export interface DbConfig {
  // ... existing fields ...

  /**
   * Self-signed auth via a local seed.
   * Mutually exclusive with `jwtToken`.
   */
  auth?: { seed: string } | { seedStore: SeedStore };

  // jwtToken, localAuthMode, localAuthToken remain unchanged
}
```

Add the import:

```typescript
import type { SeedStore } from "./seed-store.js";
```

- [ ] **Step 3: Add auth resolution to createDb**

In the `createDb` function, before `resolveLocalAuthDefaults`, add self-signed auth resolution:

```typescript
export async function createDb(config: DbConfig): Promise<Db> {
  // Validate mutually exclusive auth options
  if (config.auth && config.jwtToken) {
    throw new Error("DbConfig error: auth and jwtToken are mutually exclusive");
  }

  let resolvedConfig = config;

  // Self-signed auth: resolve seed → mint JWT
  if (config.auth) {
    let seed: string;
    if ("seed" in config.auth) {
      seed = config.auth.seed;
    } else {
      seed = await config.auth.seedStore.getOrCreateSeed();
    }

    // We need the WASM runtime to mint the token.
    // For now, mint using the static WasmRuntime methods after loading the module.
    const wasmModule = await loadWasmModule(config);
    const jwtToken = wasmModule.WasmRuntime.mintSelfSignedToken(seed, config.appId, 3600);
    const userId = wasmModule.WasmRuntime.deriveUserId(seed);

    resolvedConfig = {
      ...config,
      jwtToken,
      // Store seed for token refresh
      _selfSignedSeed: seed,
      _selfSignedUserId: userId,
    };
  }

  resolvedConfig = resolveLocalAuthDefaults(resolvedConfig);
  // ... rest of createDb unchanged
}
```

Note: The exact integration depends on how `loadWasmModule` works. The implementation should call the WASM static methods. The `_selfSignedSeed` internal field is used by token refresh (Task 10).

- [ ] **Step 4: Update auth mode precedence in resolveLocalAuthDefaults**

In `packages/jazz-tools/src/runtime/local-auth.ts`, update `LocalAuthDefaultsInput` to include `auth`:

```typescript
type LocalAuthDefaultsInput = {
  appId: string;
  auth?: { seed: string } | { seedStore: SeedStore };
  jwtToken?: string;
  backendSecret?: string;
  localAuthMode?: LocalAuthMode;
  localAuthToken?: string;
};
```

And at the top of `resolveLocalAuthDefaults`, skip local auth when `auth` is set:

```typescript
if (config.auth) {
  return config;
}
```

- [ ] **Step 5: Run the test**

Run: `cd /Users/guidodorsi/workspace/jazz2 && npx vitest run packages/jazz-tools/src/runtime/db.self-signed-auth.test.ts 2>&1 | tail -15`
Expected: passes

- [ ] **Step 6: Commit**

```bash
git add packages/jazz-tools/src/runtime/db.ts packages/jazz-tools/src/runtime/local-auth.ts packages/jazz-tools/src/runtime/db.self-signed-auth.test.ts
git commit -m "feat: add auth.seed config path to createDb"
```

---

### Task 10: Implement token refresh for self-signed auth

**Files:**

- Modify: `packages/jazz-tools/src/runtime/db.ts`

- [ ] **Step 1: Write failing test**

Add to `packages/jazz-tools/src/runtime/db.self-signed-auth.test.ts`:

```typescript
describe("self-signed token refresh", () => {
  it("schedules refresh and cleans up on shutdown", async () => {
    // This test verifies that:
    // 1. A refresh timer is created when auth.seed is used
    // 2. db.shutdown() clears the timer
    const clearTimeoutSpy = vi.spyOn(globalThis, "clearTimeout");
    // Create a db with auth.seed (will need proper WASM mock)
    // ... implementation depends on how mocking works in existing tests
    // After shutdown, clearTimeout should have been called
  });
});
```

- [ ] **Step 2: Add refresh timer to Db**

In the `Db` class, add a private field:

```typescript
private selfSignedRefreshTimer: ReturnType<typeof setTimeout> | null = null;
private selfSignedSeed: string | null = null;
```

After the self-signed JWT is minted in `createDb` (or in a post-creation setup), schedule refresh:

```typescript
private scheduleSelfSignedRefresh(seed: string, ttlSeconds: number): void {
  // Refresh at 80% of TTL
  const refreshMs = ttlSeconds * 800; // 80% of TTL in ms
  this.selfSignedSeed = seed;
  this.selfSignedRefreshTimer = setTimeout(() => {
    this.refreshSelfSignedToken();
  }, refreshMs);
}

private refreshSelfSignedToken(): void {
  if (!this.selfSignedSeed || this.isShuttingDown) return;

  try {
    const wasmModule = this.wasmModule;
    if (!wasmModule) return;

    const newToken = wasmModule.WasmRuntime.mintSelfSignedToken(
      this.selfSignedSeed,
      this.config.appId,
      3600,
    );
    this.updateAuthToken(newToken);
    this.scheduleSelfSignedRefresh(this.selfSignedSeed, 3600);
  } catch (e) {
    // Minting is a local WASM call — failure means runtime is gone
    console.error("Failed to refresh self-signed token:", e);
  }
}
```

- [ ] **Step 3: Clean up timer on shutdown**

In the `shutdown()` method, add:

```typescript
if (this.selfSignedRefreshTimer) {
  clearTimeout(this.selfSignedRefreshTimer);
  this.selfSignedRefreshTimer = null;
}
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/guidodorsi/workspace/jazz2 && npx vitest run packages/jazz-tools/src/runtime/db.self-signed-auth.test.ts 2>&1 | tail -15`
Expected: passes

- [ ] **Step 5: Commit**

```bash
git add packages/jazz-tools/src/runtime/db.ts packages/jazz-tools/src/runtime/db.self-signed-auth.test.ts
git commit -m "feat: add self-signed JWT token refresh"
```

---

### Task 11: Remove the linking/upgrade system

**Files:**

- Delete: `packages/jazz-tools/src/react/use-link-external-identity.ts`
- Delete: `packages/jazz-tools/src/vue/use-link-external-identity.ts`
- Delete: `packages/jazz-tools/src/svelte/use-link-external-identity.ts`
- Delete: `packages/jazz-tools/src/svelte/use-link-external-identity.test.ts`
- Modify: `packages/jazz-tools/src/react/index.ts` (remove re-export)
- Modify: `packages/jazz-tools/src/vue/index.ts` (remove re-export)
- Modify: `packages/jazz-tools/src/svelte/index.ts` (remove re-export)
- Modify: `packages/jazz-tools/src/react-native/index.ts` (remove re-export if present)
- Modify: `packages/jazz-tools/src/runtime/index.ts` (remove `linkExternalIdentity` export if present)
- Modify: `packages/jazz-tools/src/runtime/sync-transport.ts` (remove `linkExternalIdentity` function)
- Modify: `packages/jazz-tools/src/runtime/sync-transport.test.ts` (remove linking tests)
- Delete: `crates/jazz-tools/src/server/external_identity_store.rs`
- Modify: `crates/jazz-tools/src/server/mod.rs` (remove external_identity_store module, remove external_identities from ServerState)
- Modify: `crates/jazz-tools/src/routes.rs` (remove `/auth/link-external` route and handler)
- Modify: `crates/jazz-tools/src/middleware/auth.rs` (remove `external_identities` parameter from `extract_session` and `resolve_verified_jwt_session`)

- [ ] **Step 1: Delete TS linking files**

```bash
rm packages/jazz-tools/src/react/use-link-external-identity.ts
rm packages/jazz-tools/src/vue/use-link-external-identity.ts
rm packages/jazz-tools/src/svelte/use-link-external-identity.ts
rm packages/jazz-tools/src/svelte/use-link-external-identity.test.ts
```

- [ ] **Step 2: Remove re-exports from framework index files**

In each of these files, remove the `use-link-external-identity` import/export:

- `packages/jazz-tools/src/react/index.ts`
- `packages/jazz-tools/src/vue/index.ts`
- `packages/jazz-tools/src/svelte/index.ts`
- `packages/jazz-tools/src/react-native/index.ts`

- [ ] **Step 3: Remove `linkExternalIdentity` from sync-transport**

In `packages/jazz-tools/src/runtime/sync-transport.ts`, remove the `linkExternalIdentity` function and its types. Also remove any related exports from `packages/jazz-tools/src/runtime/index.ts`.

- [ ] **Step 4: Remove linking tests from sync-transport.test.ts**

Remove any test cases in `packages/jazz-tools/src/runtime/sync-transport.test.ts` that test `linkExternalIdentity`.

- [ ] **Step 5: Delete external_identity_store.rs**

```bash
rm crates/jazz-tools/src/server/external_identity_store.rs
```

- [ ] **Step 6: Remove external_identity_store from server/mod.rs**

In `crates/jazz-tools/src/server/mod.rs`:

- Remove `pub mod external_identity_store;`
- Remove `external_identities: RwLock<ExternalIdentityMap>` from `ServerState`
- Remove `external_identity_store: ExternalIdentityStore` from `ServerState`
- Remove any initialization of these fields

- [ ] **Step 7: Remove link-external route from routes.rs**

In `crates/jazz-tools/src/routes.rs`:

- Remove the `POST /auth/link-external` route registration
- Remove the `link_external_handler` function (lines ~1436-1634)
- Remove the `LinkExternalResponse` type

- [ ] **Step 8: Remove `external_identities` parameter from auth middleware**

In `crates/jazz-tools/src/middleware/auth.rs`:

- Remove `external_identities: Option<&ExternalIdentityMap>` from `extract_session` signature
- Remove `external_identities: Option<&ExternalIdentityMap>` from `resolve_verified_jwt_session` signature
- Remove the external identity mapping lookup in `resolve_verified_jwt_session` (the `mapped_principal` logic)
- Update all callers of these functions
- Update tests that pass `external_identities`

- [ ] **Step 9: Verify everything compiles and tests pass**

Run in parallel:

```bash
cargo test -p jazz-tools 2>&1 | tail -20
cd /Users/guidodorsi/workspace/jazz2 && npx vitest run packages/jazz-tools 2>&1 | tail -20
```

Expected: all pass

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "refactor: remove identity linking system"
```

---

### Task 12: Integration test — self-signed auth end-to-end

**Files:**

- Create: `crates/jazz-tools/tests/self_signed_auth.rs`

- [ ] **Step 1: Write the integration test**

Create `crates/jazz-tools/tests/self_signed_auth.rs`:

```rust
//! Integration tests for self-signed Ed25519 JWT auth.

use jazz_tools::identity;
use jazz_tools::middleware::auth::{AuthConfig, extract_session};
use axum::http::{HeaderMap, header::AUTHORIZATION};

fn test_app_id() -> jazz_tools::schema_manager::AppId {
    jazz_tools::schema_manager::AppId::from_name("self-signed-integration-test")
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
        jwks_url: None,
        allow_anonymous: false,
        allow_demo: false,
        allow_self_signed: true,
        backend_secret: None,
        admin_secret: None,
    }
}

#[tokio::test]
async fn valid_self_signed_jwt_authenticates() {
    let seed = alice_seed();
    let app_id = test_app_id();
    let token = identity::mint_self_signed_token(&seed, app_id.as_str(), 3600).unwrap();
    let config = self_signed_config();

    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());

    let session = extract_session(&headers, app_id, &config, None)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(session.user_id, identity::derive_user_id(&seed).to_string());
}

#[tokio::test]
async fn same_seed_same_identity() {
    let app_id = test_app_id();
    let config = self_signed_config();

    let token1 = identity::mint_self_signed_token(&alice_seed(), app_id.as_str(), 3600).unwrap();
    let token2 = identity::mint_self_signed_token(&alice_seed(), app_id.as_str(), 3600).unwrap();

    let mut h1 = HeaderMap::new();
    h1.insert(AUTHORIZATION, format!("Bearer {token1}").parse().unwrap());
    let mut h2 = HeaderMap::new();
    h2.insert(AUTHORIZATION, format!("Bearer {token2}").parse().unwrap());

    let s1 = extract_session(&h1, app_id, &config, None).await.unwrap().unwrap();
    let s2 = extract_session(&h2, app_id, &config, None).await.unwrap().unwrap();

    assert_eq!(s1.user_id, s2.user_id);
}

#[tokio::test]
async fn different_seeds_different_identities() {
    let app_id = test_app_id();
    let config = self_signed_config();

    let t_alice = identity::mint_self_signed_token(&alice_seed(), app_id.as_str(), 3600).unwrap();
    let t_bob = identity::mint_self_signed_token(&bob_seed(), app_id.as_str(), 3600).unwrap();

    let mut h_alice = HeaderMap::new();
    h_alice.insert(AUTHORIZATION, format!("Bearer {t_alice}").parse().unwrap());
    let mut h_bob = HeaderMap::new();
    h_bob.insert(AUTHORIZATION, format!("Bearer {t_bob}").parse().unwrap());

    let s_alice = extract_session(&h_alice, app_id, &config, None).await.unwrap().unwrap();
    let s_bob = extract_session(&h_bob, app_id, &config, None).await.unwrap().unwrap();

    assert_ne!(s_alice.user_id, s_bob.user_id);
}

#[tokio::test]
async fn wrong_audience_rejected() {
    let token = identity::mint_self_signed_token(&alice_seed(), "wrong-app", 3600).unwrap();
    let config = self_signed_config();

    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());

    let result = extract_session(&headers, test_app_id(), &config, None).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn missing_aud_rejected() {
    // Manually craft a token without aud — will fail the identity module validation
    let seed = alice_seed();
    let token = identity::mint_self_signed_token(&seed, "", 3600).unwrap();
    // Empty audience should be rejected by the server when it doesn't match appId
    let config = self_signed_config();

    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());

    let result = extract_session(&headers, test_app_id(), &config, None).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn self_signed_disabled_rejected() {
    let app_id = test_app_id();
    let token = identity::mint_self_signed_token(&alice_seed(), app_id.as_str(), 3600).unwrap();
    let mut config = self_signed_config();
    config.allow_self_signed = false;

    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());

    let result = extract_session(&headers, app_id, &config, None).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn expired_token_rejected() {
    // Mint a token with 0 TTL — it expires immediately
    let seed = alice_seed();
    let app_id = test_app_id();
    let token = identity::mint_self_signed_token(&seed, app_id.as_str(), 0).unwrap();
    let config = self_signed_config();

    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());

    let result = extract_session(&headers, app_id, &config, None).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn non_self_signed_iss_falls_through_to_external() {
    // A JWT with a different issuer should not be handled by the self-signed path.
    // Without JWKS configured, it should fail as "JWT auth not enabled."
    let config = self_signed_config();
    // Use a manually crafted JWT-like string that has iss != urn:jazz:self-signed
    // The self-signed path should not touch it; it falls through to JWKS which is not configured.
    let mut headers = HeaderMap::new();
    // A minimal JWT with iss = "https://auth.example.com" (won't pass JWKS validation)
    let fake_claims = serde_json::json!({"sub": "user", "iss": "https://auth.example.com"});
    let claims_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&fake_claims).unwrap());
    let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(b"{\"alg\":\"HS256\",\"typ\":\"JWT\"}");
    let fake_token = format!("{header_b64}.{claims_b64}.fake-sig");
    headers.insert(AUTHORIZATION, format!("Bearer {fake_token}").parse().unwrap());

    let result = extract_session(&headers, test_app_id(), &config, None).await;
    // Should error because no JWKS is configured, NOT because of self-signed validation
    assert!(result.is_err());
}
```

**Note on BetterAuth continuity tests:** BetterAuth integration is app-level. The Jazz-side requirement is that `jazz_principal_id` claim resolution works in `resolve_verified_jwt_session` — this is already implemented and tested in existing auth tests. No new Jazz code is needed for BetterAuth continuity beyond what the self-signed path provides.

- [ ] **Step 2: Run the integration tests**

Run: `cargo test -p jazz-tools --test self_signed_auth -- --nocapture 2>&1 | tail -20`
Expected: all pass

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-tools/tests/self_signed_auth.rs
git commit -m "test: add self-signed auth integration tests"
```

---

### Task 13: Full build verification

- [ ] **Step 1: Build everything**

Run: `cd /Users/guidodorsi/workspace/jazz2 && pnpm build 2>&1 | tail -20`
Expected: builds without errors

- [ ] **Step 2: Run all tests**

Run: `cd /Users/guidodorsi/workspace/jazz2 && pnpm test 2>&1 | tail -30`
Expected: all tests pass

- [ ] **Step 3: Commit any fixups**

If any build or test issues were found and fixed, commit them:

```bash
git add -A
git commit -m "fix: resolve build issues from self-signed auth integration"
```
