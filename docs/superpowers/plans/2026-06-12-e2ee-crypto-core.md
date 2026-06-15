# E2EE Crypto Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Rust crypto primitives for Jazz2 E2EE: X25519 identity derivation from the LocalFirst Auth seed, space-key generation, HPKE sealing/unsealing of space keys, and XChaCha20-Poly1305 value encryption with a context-binding envelope format.

**Architecture:** A new self-contained `e2ee` module in `crates/jazz-tools` exposing pure functions over byte slices — no runtime, schema, or storage coupling. Later plans (schema layer, runtime integration, TS bindings) consume this module. Spec: `docs/superpowers/specs/2026-06-12-e2ee-shared-keys-design.md` (§1, §8).

**Tech Stack:** Rust; `chacha20poly1305` (XChaCha20-Poly1305), `hpke` (RFC 9180, DHKEM(X25519) + HKDF-SHA256 + ChaCha20-Poly1305), existing `sha2` + `base64` + `rand` + `uuid` deps.

**Plan series:** This is plan 1 of 4 for the E2EE feature.

1. **Crypto core (this plan)**
2. Schema layer (`.encryption_space()` / `encrypted_column` in Rust, `.encryptionSpace()` / `.encrypted()` in TS DSL, `$keys` companion tables + policies)
3. Runtime integration (key cache, transparent encrypt/decrypt, `Value::Locked`, `share_key`/`unshare_key`/`key_holders`)
4. TS bindings + typed-app layer (`db.e2ee.publicKey()`, typed space-table methods, `Locked` sentinel, E2E black-box tests)

**Format reference (from the spec, normative for this plan):**

- Ciphertext envelope: `[alg_id: 1 byte][key_id: 16 bytes][nonce: 24 bytes][ciphertext+tag]` — header is 41 bytes; `alg_id = 1` means XChaCha20-Poly1305.
- Sealed key blob: `[alg_id: 1 byte][encapped_key: 32 bytes][ciphertext: 48 bytes]` — 81 bytes total; `alg_id = 1` means HPKE Base mode, DHKEM(X25519)+HKDF-SHA256+ChaCha20-Poly1305.
- Derivation domain: `jazz-e2ee-seal-v1`, mirroring `jazz-auth-sign-v1` in `crates/jazz-tools/src/identity.rs:285` (`SHA-512(domain || seed)`, first 32 bytes as key material).
- AAD binds `(table, column, row_id, key_id)` with length-prefixed fields (no delimiter ambiguity).

**Conventions:**

- Black-box tests through the crate's public API in `crates/jazz-tools/tests/` (matches `local_first_auth.rs` there).
- Commit messages: plain conventional commits, no AI attribution of any kind.
- All commands run from the repo root `/Users/guidodorsi/workspace/jazz2`.

---

### Task 0: Branch and dependencies

**Files:**

- Modify: `crates/jazz-tools/Cargo.toml` (the `[dependencies]` section, after the `sha2 = "0.10"` line)

- [ ] **Step 0.1: Create the feature branch**

```bash
git checkout -b guido/e2ee-crypto-core
```

- [ ] **Step 0.2: Add crypto dependencies**

In `crates/jazz-tools/Cargo.toml`, directly below the line `sha2 = "0.10"`, add:

```toml
chacha20poly1305 = "0.10"
hpke = { version = "0.12", default-features = false, features = ["alloc", "x25519"] }
```

- [ ] **Step 0.3: Verify the dependency tree builds**

Run: `cargo check -p jazz-tools`
Expected: PASS (compiles; new crates resolve).
If cargo rejects the `hpke` feature list, fall back to `hpke = "0.12"` (its defaults include the X25519 KEM) and re-run; record the final line in the commit.

- [ ] **Step 0.4: Commit**

```bash
git add crates/jazz-tools/Cargo.toml Cargo.lock
git commit -m "chore(jazz-tools): add chacha20poly1305 and hpke dependencies"
```

---

### Task 1: E2EE identity derivation

**Files:**

- Create: `crates/jazz-tools/src/e2ee.rs`
- Modify: `crates/jazz-tools/src/lib.rs` (module list at the top; add alongside `pub mod identity;`)
- Test: `crates/jazz-tools/tests/e2ee.rs`

- [ ] **Step 1.1: Write the failing tests**

Create `crates/jazz-tools/tests/e2ee.rs`:

```rust
//! Black-box tests for the E2EE crypto core (public API only).

use jazz_tools::e2ee::{derive_e2ee_keypair, E2eePublicKey};
use jazz_tools::identity::derive_verifying_key;

#[test]
fn e2ee_keypair_is_deterministic_per_seed() {
    let seed = [7u8; 32];
    let a = derive_e2ee_keypair(&seed);
    let b = derive_e2ee_keypair(&seed);
    assert_eq!(a.public.as_bytes(), b.public.as_bytes());

    let other = derive_e2ee_keypair(&[8u8; 32]);
    assert_ne!(a.public.as_bytes(), other.public.as_bytes());
}

#[test]
fn e2ee_key_is_domain_separated_from_signing_key() {
    let seed = [7u8; 32];
    let e2ee = derive_e2ee_keypair(&seed);
    let signing = derive_verifying_key(&seed);
    assert_ne!(e2ee.public.as_bytes(), signing.as_bytes());
}

#[test]
fn public_key_base64url_round_trips() {
    let seed = [9u8; 32];
    let pk = derive_e2ee_keypair(&seed).public;
    let encoded = pk.to_base64url();
    // 32 bytes -> 43 base64url chars, no padding
    assert_eq!(encoded.len(), 43);
    assert!(!encoded.contains('='));
    let decoded = E2eePublicKey::from_base64url(&encoded).unwrap();
    assert_eq!(decoded.as_bytes(), pk.as_bytes());
}

#[test]
fn public_key_rejects_malformed_input() {
    assert!(E2eePublicKey::from_base64url("not base64!!!").is_err());
    assert!(E2eePublicKey::from_base64url("AAAA").is_err()); // wrong length
}
```

- [ ] **Step 1.2: Run tests to verify they fail**

Run: `cargo test -p jazz-tools --test e2ee`
Expected: FAIL to compile with `could not find e2ee in jazz_tools`.

- [ ] **Step 1.3: Implement derivation**

Create `crates/jazz-tools/src/e2ee.rs`:

```rust
//! E2EE crypto core: identity derivation, space-key sealing, value encryption.
//!
//! Pure functions over byte slices; no runtime or storage coupling. Formats are
//! specified in docs/superpowers/specs/2026-06-12-e2ee-shared-keys-design.md.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hpke::kem::X25519HkdfSha256;
use hpke::{Deserializable, Kem as KemTrait, Serializable};
use sha2::{Digest, Sha512};

/// Domain string for deriving the E2EE encryption identity from the auth seed.
/// Sibling of `jazz-auth-sign-v1` in `identity.rs`.
pub const E2EE_SEAL_DOMAIN: &str = "jazz-e2ee-seal-v1";

type Kem = X25519HkdfSha256;

#[derive(Debug, PartialEq, Eq)]
pub enum E2eeError {
    MalformedPublicKey,
    MalformedSealedKey,
    MalformedEnvelope,
    UnsupportedAlgorithm(u8),
    Seal(String),
    Unseal(String),
    Encrypt(String),
    Decrypt(String),
}

impl std::fmt::Display for E2eeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            E2eeError::MalformedPublicKey => write!(f, "malformed E2EE public key"),
            E2eeError::MalformedSealedKey => write!(f, "malformed sealed key blob"),
            E2eeError::MalformedEnvelope => write!(f, "malformed ciphertext envelope"),
            E2eeError::UnsupportedAlgorithm(id) => write!(f, "unsupported algorithm id {id}"),
            E2eeError::Seal(e) => write!(f, "seal failed: {e}"),
            E2eeError::Unseal(e) => write!(f, "unseal failed: {e}"),
            E2eeError::Encrypt(e) => write!(f, "encrypt failed: {e}"),
            E2eeError::Decrypt(e) => write!(f, "decrypt failed: {e}"),
        }
    }
}

impl std::error::Error for E2eeError {}

/// X25519 public key of an E2EE identity (what apps publish to their directory).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct E2eePublicKey([u8; 32]);

impl E2eePublicKey {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn to_base64url(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0)
    }

    pub fn from_base64url(encoded: &str) -> Result<Self, E2eeError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| E2eeError::MalformedPublicKey)?;
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| E2eeError::MalformedPublicKey)?;
        Ok(Self(bytes))
    }
}

/// An E2EE identity keypair derived from the 32-byte LocalFirst Auth seed.
pub struct E2eeKeypair {
    secret: <Kem as KemTrait>::PrivateKey,
    pub public: E2eePublicKey,
}

impl E2eeKeypair {
    fn secret(&self) -> &<Kem as KemTrait>::PrivateKey {
        &self.secret
    }
}

/// Derive the E2EE keypair from a 32-byte seed.
/// Key material = SHA-512(domain || seed)[..32], mirroring `identity::derive_signing_key`,
/// fed as IKM into the HPKE KEM's deterministic keypair derivation.
pub fn derive_e2ee_keypair(seed: &[u8; 32]) -> E2eeKeypair {
    let mut hasher = Sha512::new();
    hasher.update(E2EE_SEAL_DOMAIN.as_bytes());
    hasher.update(seed);
    let hash = hasher.finalize();
    let ikm: [u8; 32] = hash[..32].try_into().expect("SHA-512 output is 64 bytes");
    let (secret, public) = Kem::derive_keypair(&ikm);
    let public_bytes: [u8; 32] = public
        .to_bytes()
        .as_slice()
        .try_into()
        .expect("X25519 public key is 32 bytes");
    E2eeKeypair {
        secret,
        public: E2eePublicKey(public_bytes),
    }
}
```

In `crates/jazz-tools/src/lib.rs`, after the line `pub mod digest;`, add:

```rust
pub mod e2ee;
```

- [ ] **Step 1.4: Run tests to verify they pass**

Run: `cargo test -p jazz-tools --test e2ee`
Expected: PASS, 4 tests.

- [ ] **Step 1.5: Commit**

```bash
git add crates/jazz-tools/src/e2ee.rs crates/jazz-tools/src/lib.rs crates/jazz-tools/tests/e2ee.rs
git commit -m "feat(jazz-tools): derive E2EE identity keypair from auth seed"
```

---

### Task 2: Space keys and HPKE sealing

**Files:**

- Modify: `crates/jazz-tools/src/e2ee.rs` (append)
- Test: `crates/jazz-tools/tests/e2ee.rs` (append)

- [ ] **Step 2.1: Write the failing tests**

Append to `crates/jazz-tools/tests/e2ee.rs` (extend the existing `use` line for the new items):

```rust
use jazz_tools::e2ee::{seal_space_key, unseal_space_key, E2eeError, SpaceKey, SEALED_KEY_LEN};

#[test]
fn seal_unseal_round_trips() {
    let recipient = derive_e2ee_keypair(&[1u8; 32]);
    let space_key = SpaceKey::generate();
    let sealed = seal_space_key(&recipient.public, &space_key).unwrap();
    assert_eq!(sealed.len(), SEALED_KEY_LEN); // 81 bytes: alg(1) + encapped(32) + ct(48)
    assert_eq!(sealed[0], 1); // alg_id 1 = HPKE X25519
    let unsealed = unseal_space_key(&recipient, &sealed).unwrap();
    assert_eq!(unsealed.as_bytes(), space_key.as_bytes());
}

#[test]
fn unseal_with_wrong_recipient_fails() {
    let recipient = derive_e2ee_keypair(&[1u8; 32]);
    let attacker = derive_e2ee_keypair(&[2u8; 32]);
    let sealed = seal_space_key(&recipient.public, &SpaceKey::generate()).unwrap();
    assert!(matches!(
        unseal_space_key(&attacker, &sealed),
        Err(E2eeError::Unseal(_))
    ));
}

#[test]
fn unseal_rejects_tampered_blob() {
    let recipient = derive_e2ee_keypair(&[1u8; 32]);
    let mut sealed = seal_space_key(&recipient.public, &SpaceKey::generate()).unwrap();
    let last = sealed.len() - 1;
    sealed[last] ^= 0xff;
    assert!(unseal_space_key(&recipient, &sealed).is_err());
}

#[test]
fn unseal_rejects_malformed_blobs() {
    let recipient = derive_e2ee_keypair(&[1u8; 32]);
    assert_eq!(
        unseal_space_key(&recipient, &[]),
        Err(E2eeError::MalformedSealedKey)
    );
    assert_eq!(
        unseal_space_key(&recipient, &[1u8; 10]),
        Err(E2eeError::MalformedSealedKey)
    );
    // unknown algorithm id
    let mut sealed = seal_space_key(&recipient.public, &SpaceKey::generate()).unwrap();
    sealed[0] = 99;
    assert_eq!(
        unseal_space_key(&recipient, &sealed),
        Err(E2eeError::UnsupportedAlgorithm(99))
    );
}

#[test]
fn space_keys_are_random() {
    assert_ne!(SpaceKey::generate().as_bytes(), SpaceKey::generate().as_bytes());
}
```

- [ ] **Step 2.2: Run tests to verify they fail**

Run: `cargo test -p jazz-tools --test e2ee`
Expected: FAIL to compile with unresolved imports (`seal_space_key`, `SpaceKey`, ...).

- [ ] **Step 2.3: Implement sealing**

Append to `crates/jazz-tools/src/e2ee.rs`:

```rust
use hpke::aead::ChaCha20Poly1305;
use hpke::kdf::HkdfSha256;
use hpke::{single_shot_open, single_shot_seal, OpModeR, OpModeS};
use rand::rngs::OsRng;
use rand::RngCore;

/// Algorithm id for HPKE Base mode, DHKEM(X25519) + HKDF-SHA256 + ChaCha20-Poly1305.
pub const SEALED_ALG_HPKE_X25519: u8 = 1;
/// Sealed blob layout: [alg_id: 1][encapped_key: 32][ciphertext: 32 key + 16 tag].
pub const SEALED_KEY_LEN: usize = 1 + 32 + 48;

/// 32-byte symmetric key owned by an encryption-space row.
#[derive(Clone, PartialEq, Eq)]
pub struct SpaceKey([u8; 32]);

// Redacting Debug: tests compare Result<SpaceKey, _> with assert_eq!, but key
// bytes must never end up in logs or panic messages.
impl std::fmt::Debug for SpaceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpaceKey(..)")
    }
}

impl SpaceKey {
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// Seal a space key to a recipient's public key. Output: 81-byte blob.
pub fn seal_space_key(
    recipient: &E2eePublicKey,
    space_key: &SpaceKey,
) -> Result<Vec<u8>, E2eeError> {
    let recipient_pk = <Kem as KemTrait>::PublicKey::from_bytes(recipient.as_bytes())
        .map_err(|e| E2eeError::Seal(e.to_string()))?;
    let (encapped, ciphertext) = single_shot_seal::<ChaCha20Poly1305, HkdfSha256, Kem, _>(
        &OpModeS::Base,
        &recipient_pk,
        E2EE_SEAL_DOMAIN.as_bytes(),
        space_key.as_bytes(),
        &[],
        &mut OsRng,
    )
    .map_err(|e| E2eeError::Seal(e.to_string()))?;

    let mut blob = Vec::with_capacity(SEALED_KEY_LEN);
    blob.push(SEALED_ALG_HPKE_X25519);
    blob.extend_from_slice(encapped.to_bytes().as_slice());
    blob.extend_from_slice(&ciphertext);
    debug_assert_eq!(blob.len(), SEALED_KEY_LEN);
    Ok(blob)
}

/// Unseal a space key with the recipient's keypair.
pub fn unseal_space_key(keypair: &E2eeKeypair, sealed: &[u8]) -> Result<SpaceKey, E2eeError> {
    if sealed.is_empty() {
        return Err(E2eeError::MalformedSealedKey);
    }
    let alg = sealed[0];
    if alg != SEALED_ALG_HPKE_X25519 {
        // Length check only applies to known layouts; unknown alg wins when long
        // enough to plausibly carry one, otherwise report malformed.
        if sealed.len() == SEALED_KEY_LEN {
            return Err(E2eeError::UnsupportedAlgorithm(alg));
        }
        return Err(E2eeError::MalformedSealedKey);
    }
    if sealed.len() != SEALED_KEY_LEN {
        return Err(E2eeError::MalformedSealedKey);
    }
    let encapped = <Kem as KemTrait>::EncappedKey::from_bytes(&sealed[1..33])
        .map_err(|_| E2eeError::MalformedSealedKey)?;
    let plaintext = single_shot_open::<ChaCha20Poly1305, HkdfSha256, Kem>(
        &OpModeR::Base,
        keypair.secret(),
        &encapped,
        E2EE_SEAL_DOMAIN.as_bytes(),
        &sealed[33..],
        &[],
    )
    .map_err(|e| E2eeError::Unseal(e.to_string()))?;
    let bytes: [u8; 32] = plaintext
        .try_into()
        .map_err(|_| E2eeError::Unseal("unexpected plaintext length".to_string()))?;
    Ok(SpaceKey::from_bytes(bytes))
}
```

- [ ] **Step 2.4: Run tests to verify they pass**

Run: `cargo test -p jazz-tools --test e2ee`
Expected: PASS, 9 tests.

- [ ] **Step 2.5: Commit**

```bash
git add crates/jazz-tools/src/e2ee.rs crates/jazz-tools/tests/e2ee.rs
git commit -m "feat(jazz-tools): space key generation and HPKE sealing"
```

---

### Task 3: Value encryption with context-binding envelope

**Files:**

- Modify: `crates/jazz-tools/src/e2ee.rs` (append)
- Test: `crates/jazz-tools/tests/e2ee.rs` (append)

- [ ] **Step 3.1: Write the failing tests**

Append to `crates/jazz-tools/tests/e2ee.rs`:

```rust
use jazz_tools::e2ee::{
    decrypt_value, encrypt_value, envelope_key_id, EncryptionContext, ENVELOPE_HEADER_LEN,
};
use uuid::Uuid;

fn ctx<'a>(table: &'a str, column: &'a str, row_id: &'a [u8]) -> EncryptionContext<'a> {
    EncryptionContext {
        table,
        column,
        row_id,
    }
}

#[test]
fn value_encryption_round_trips() {
    let key = SpaceKey::generate();
    let key_id = Uuid::new_v4();
    let context = ctx("todos", "title", b"row-1");
    let envelope = encrypt_value(&key, &key_id, &context, b"secret title").unwrap();
    assert_eq!(envelope[0], 1); // alg_id 1 = XChaCha20-Poly1305
    assert_eq!(envelope_key_id(&envelope).unwrap(), key_id);
    let plaintext = decrypt_value(&key, &context, &envelope).unwrap();
    assert_eq!(plaintext, b"secret title");
}

#[test]
fn nonces_are_unique_per_encryption() {
    let key = SpaceKey::generate();
    let key_id = Uuid::new_v4();
    let context = ctx("todos", "title", b"row-1");
    let a = encrypt_value(&key, &key_id, &context, b"same").unwrap();
    let b = encrypt_value(&key, &key_id, &context, b"same").unwrap();
    // nonce lives at bytes [17..41]
    assert_ne!(a[17..41], b[17..41]);
    assert_ne!(a, b);
}

#[test]
fn decrypt_fails_when_any_context_field_differs() {
    let key = SpaceKey::generate();
    let key_id = Uuid::new_v4();
    let envelope = encrypt_value(&key, &key_id, &ctx("todos", "title", b"row-1"), b"x").unwrap();

    for wrong in [
        ctx("notes", "title", b"row-1"),  // different table
        ctx("todos", "body", b"row-1"),   // different column
        ctx("todos", "title", b"row-2"),  // different row
    ] {
        assert!(matches!(
            decrypt_value(&key, &wrong, &envelope),
            Err(E2eeError::Decrypt(_))
        ));
    }
}

#[test]
fn context_encoding_is_unambiguous_across_field_boundaries() {
    // ("ab", "c") and ("a", "bc") must not produce the same AAD.
    let key = SpaceKey::generate();
    let key_id = Uuid::new_v4();
    let envelope = encrypt_value(&key, &key_id, &ctx("ab", "c", b"r"), b"x").unwrap();
    assert!(decrypt_value(&key, &ctx("a", "bc", b"r"), &envelope).is_err());
}

#[test]
fn decrypt_fails_with_wrong_key() {
    let context = ctx("todos", "title", b"row-1");
    let envelope =
        encrypt_value(&SpaceKey::generate(), &Uuid::new_v4(), &context, b"x").unwrap();
    assert!(decrypt_value(&SpaceKey::generate(), &context, &envelope).is_err());
}

#[test]
fn decrypt_binds_key_id_via_aad() {
    // Same key, same context, but envelope key_id swapped to a different uuid:
    // authentication must fail because key_id is part of the AAD.
    let key = SpaceKey::generate();
    let context = ctx("todos", "title", b"row-1");
    let mut envelope = encrypt_value(&key, &Uuid::new_v4(), &context, b"x").unwrap();
    let other = Uuid::new_v4();
    envelope[1..17].copy_from_slice(other.as_bytes());
    assert!(decrypt_value(&key, &context, &envelope).is_err());
}

#[test]
fn decrypt_rejects_malformed_envelopes() {
    let key = SpaceKey::generate();
    let context = ctx("todos", "title", b"row-1");
    assert_eq!(
        decrypt_value(&key, &context, &[]),
        Err(E2eeError::MalformedEnvelope)
    );
    assert_eq!(
        decrypt_value(&key, &context, &[1u8; ENVELOPE_HEADER_LEN - 1]),
        Err(E2eeError::MalformedEnvelope)
    );
    let mut envelope = encrypt_value(&key, &Uuid::new_v4(), &context, b"x").unwrap();
    envelope[0] = 99;
    assert_eq!(
        decrypt_value(&key, &context, &envelope),
        Err(E2eeError::UnsupportedAlgorithm(99))
    );
}

#[test]
fn empty_plaintext_round_trips() {
    let key = SpaceKey::generate();
    let context = ctx("todos", "title", b"row-1");
    let envelope = encrypt_value(&key, &Uuid::new_v4(), &context, b"").unwrap();
    assert_eq!(decrypt_value(&key, &context, &envelope).unwrap(), b"");
}
```

- [ ] **Step 3.2: Run tests to verify they fail**

Run: `cargo test -p jazz-tools --test e2ee`
Expected: FAIL to compile with unresolved imports (`encrypt_value`, ...).

- [ ] **Step 3.3: Implement value encryption**

Append to `crates/jazz-tools/src/e2ee.rs`:

```rust
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use uuid::Uuid;

/// Algorithm id for XChaCha20-Poly1305 value encryption.
pub const ENVELOPE_ALG_XCHACHA20POLY1305: u8 = 1;
/// Envelope layout: [alg_id: 1][key_id: 16][nonce: 24][ciphertext+tag].
pub const ENVELOPE_HEADER_LEN: usize = 1 + 16 + 24;

const AAD_DOMAIN: &str = "jazz-e2ee-aad-v1";

/// Identifies where a ciphertext lives; bound into the AAD so ciphertext cannot
/// be grafted between rows or columns.
#[derive(Debug, Clone, Copy)]
pub struct EncryptionContext<'a> {
    pub table: &'a str,
    pub column: &'a str,
    pub row_id: &'a [u8],
}

/// Length-prefixed field encoding: no two distinct (table, column, row_id, key_id)
/// tuples can produce the same AAD bytes.
fn build_aad(context: &EncryptionContext<'_>, key_id: &Uuid) -> Vec<u8> {
    let mut aad = Vec::with_capacity(
        AAD_DOMAIN.len()
            + context.table.len()
            + context.column.len()
            + context.row_id.len()
            + 16
            + 5 * 8,
    );
    for field in [
        AAD_DOMAIN.as_bytes(),
        context.table.as_bytes(),
        context.column.as_bytes(),
        context.row_id,
        key_id.as_bytes().as_slice(),
    ] {
        aad.extend_from_slice(&(field.len() as u64).to_be_bytes());
        aad.extend_from_slice(field);
    }
    aad
}

/// Encrypt a serialized column value under a space key. Output: envelope bytes.
pub fn encrypt_value(
    key: &SpaceKey,
    key_id: &Uuid,
    context: &EncryptionContext<'_>,
    plaintext: &[u8],
) -> Result<Vec<u8>, E2eeError> {
    let cipher = XChaCha20Poly1305::new(key.as_bytes().into());
    let mut nonce = [0u8; 24];
    OsRng.fill_bytes(&mut nonce);
    let aad = build_aad(context, key_id);
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|e| E2eeError::Encrypt(e.to_string()))?;

    let mut envelope = Vec::with_capacity(ENVELOPE_HEADER_LEN + ciphertext.len());
    envelope.push(ENVELOPE_ALG_XCHACHA20POLY1305);
    envelope.extend_from_slice(key_id.as_bytes());
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(&ciphertext);
    Ok(envelope)
}

/// Read the key id from an envelope without decrypting (the runtime uses this to
/// pick the right space key from its cache).
pub fn envelope_key_id(envelope: &[u8]) -> Result<Uuid, E2eeError> {
    if envelope.len() < ENVELOPE_HEADER_LEN {
        return Err(E2eeError::MalformedEnvelope);
    }
    let alg = envelope[0];
    if alg != ENVELOPE_ALG_XCHACHA20POLY1305 {
        return Err(E2eeError::UnsupportedAlgorithm(alg));
    }
    let bytes: [u8; 16] = envelope[1..17].try_into().expect("slice length checked");
    Ok(Uuid::from_bytes(bytes))
}

/// Decrypt an envelope under a space key. The key id inside the envelope is part
/// of the AAD, so a swapped key id fails authentication.
pub fn decrypt_value(
    key: &SpaceKey,
    context: &EncryptionContext<'_>,
    envelope: &[u8],
) -> Result<Vec<u8>, E2eeError> {
    if envelope.len() < ENVELOPE_HEADER_LEN {
        return Err(E2eeError::MalformedEnvelope);
    }
    let key_id = envelope_key_id(envelope)?;
    let nonce = &envelope[17..41];
    let ciphertext = &envelope[41..];
    let aad = build_aad(context, &key_id);
    let cipher = XChaCha20Poly1305::new(key.as_bytes().into());
    cipher
        .decrypt(
            XNonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|e| E2eeError::Decrypt(e.to_string()))
}
```

- [ ] **Step 3.4: Run tests to verify they pass**

Run: `cargo test -p jazz-tools --test e2ee`
Expected: PASS, 17 tests.

- [ ] **Step 3.5: Commit**

```bash
git add crates/jazz-tools/src/e2ee.rs crates/jazz-tools/tests/e2ee.rs
git commit -m "feat(jazz-tools): value encryption with context-binding envelope"
```

---

### Task 4: Crate-wide verification

**Files:** none new.

- [ ] **Step 4.1: Run the whole crate's tests**

Run: `cargo test -p jazz-tools`
Expected: PASS — the new module must not break any existing test. If anything unrelated fails, check it fails on `main` too before touching it; do not fix unrelated tests in this branch.

- [ ] **Step 4.2: Lint**

Run: `cargo clippy -p jazz-tools -- -D warnings`
Expected: clean. Fix any warnings introduced by the new module only.

- [ ] **Step 4.3: Commit (only if lint required changes)**

```bash
git add crates/jazz-tools/src/e2ee.rs
git commit -m "chore(jazz-tools): clippy fixes in e2ee module"
```

---

## Out of scope for this plan (handled by plans 2–4)

- Schema: `.encryption_space()`, `encrypted_column`, `$keys` companion tables, policies (plan 2)
- Runtime: key cache, transparent encrypt/decrypt in mutation/query paths, `Value::Locked`, `share_key`/`unshare_key`/`key_holders`, `E2EEKeyUnavailable` (plan 3)
- TS: DSL `.encryptionSpace()` / `.encrypted()`, `db.e2ee.publicKey()`, `Locked` sentinel, WASM/NAPI plumbing, E2E tests (plan 4)
- Benchmarks for the cost-summary validation (spec §9) — belongs with plan 3 where the hot path exists
