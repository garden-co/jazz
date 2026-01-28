# Crypto Stable Stringify Migration - Design

## Overview

This design addresses the migration from JS `stableStringify`-based canonicalization to Rust-owned canonical encodings for cryptographic operations. The goal is to eliminate the dependency on JavaScript-specific JSON canonicalization in the crypto layer while maintaining backward compatibility and performance.

The approach involves:

1. **Using Rust handlers for data structures** that require canonical serialization (e.g., `CoValueHeaderBuilder`)
2. **Moving canonical encoding to Rust** (cojson-core) with deterministic serialization via `BTreeMap`
3. **Exposing handlers via WASM/RN/NAPI bindings** following the existing `SessionLog` pattern
4. **Constraining crypto APIs** to accept bytes/strings rather than arbitrary JSON
5. **Replacing `JsonValue` with typed enums** where the actual value space is constrained

## Architecture

### Current Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          JS/TS Layer                            │
├─────────────────────────────────────────────────────────────────┤
│  stableStringify(value)  ──────────────────────────────────────►│
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │  shortHash   │    │  sign/verify │    │ encrypt/seal │      │
│  │  (CoValue ID)│    │  (Ed25519)   │    │ (XSalsa20)   │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│         │                   │                   │               │
│         └───────────────────┴───────────────────┘               │
│                             │                                   │
│                    textEncoder.encode()                         │
│                             ▼                                   │
├─────────────────────────────────────────────────────────────────┤
│                      Rust Core (via bindings)                   │
│   blake3HashOnce() │ sign() / verify() │ encrypt() / seal()    │
└─────────────────────────────────────────────────────────────────┘
```

### Target Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          JS/TS Layer                            │
├─────────────────────────────────────────────────────────────────┤
│  Request/Auth (jazz-tools)                                      │
│    - Constructs payloads                                        │
│    - Serializes to JSON string                                  │
│    - Passes string/bytes to crypto                              │
├─────────────────────────────────────────────────────────────────┤
│  cojson CryptoProvider                                          │
│    - Creates Rust handlers: new CoValueHeaderBuilder()          │
│    - Manipulates handlers: builder.setType("comap")             │
│    - Computes results: builder.computeId() → RawCoID            │
│    - sign(secret, message: string | bytes)                      │
│    - encrypt/seal with typed nonce material handlers            │
│                             │                                   │
│                             ▼                                   │
├─────────────────────────────────────────────────────────────────┤
│                      Rust Core (cojson-core)                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Rust Handlers (opaque structs exposed to JS)            │   │
│  │    - CoValueHeaderBuilder (builds header, computes ID)   │   │
│  │    - NonceMaterialBuilder (builds nonce material)        │   │
│  │    - Uses BTreeMap for sorted key serialization          │   │
│  │    - Transaction serialization (serde-based)             │   │
│  └──────────────────────────────────────────────────────────┘   │
│                             │                                   │
│                             ▼                                   │
│   blake3HashOnce() │ sign() / verify() │ encrypt() / seal()    │
└─────────────────────────────────────────────────────────────────┘
```

### Handler Pattern (Existing Precedent)

This approach follows the existing `SessionLog` pattern in the codebase:

```typescript
// JS creates and manipulates Rust-owned handle
const sessionLog = new SessionLog(coID, sessionID, signerID);
sessionLog.tryAdd(transactions, signature, skipVerify);
sessionLog.addNewPrivateTransaction(changes, signerSecret, ...);
```

The Rust struct is exposed via `#[wasm_bindgen]` / `#[napi]` as an opaque handle:

```rust
#[wasm_bindgen]
pub struct SessionLog {
    internal: SessionLogInternal,
}

#[wasm_bindgen]
impl SessionLog {
    #[wasm_bindgen(constructor)]
    pub fn new(co_id: String, session_id: String, signer_id: Option<String>) -> SessionLog { ... }
    
    #[wasm_bindgen(js_name = tryAdd)]
    pub fn try_add(&mut self, transactions: Vec<String>, signature: String, skip_verify: bool) -> Result<(), JsError> { ... }
}
```

Memory management is handled automatically:
- **WASM**: `FinalizationRegistry` cleans up when JS object is garbage collected
- **NAPI**: Automatic via napi-rs
- **React Native**: UniFFI generates pointer-based handles with cleanup

## Components

### 1. CoValueHeaderBuilder (Rust Handler)

New handler in `crates/cojson-core/src/core/header.rs`:

```rust
use std::collections::BTreeMap;

/// Builder pattern for CoValueHeader - exposed as opaque handle to JS
/// JS manipulates this handle; Rust owns the data and serialization
#[wasm_bindgen]
pub struct CoValueHeaderBuilder {
    covalue_type: Option<CoValueType>,
    ruleset: Option<Ruleset>,
    meta: Option<BTreeMap<String, serde_json::Value>>,
    uniqueness: Option<Uniqueness>,
    created_at: Option<CreatedAt>,
}

#[wasm_bindgen]
impl CoValueHeaderBuilder {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            covalue_type: None,
            ruleset: None,
            meta: None,
            uniqueness: None,
            created_at: None,
        }
    }

    /// Set the CoValue type
    #[wasm_bindgen(js_name = setType)]
    pub fn set_type(&mut self, covalue_type: &str) -> Result<(), JsError> {
        self.covalue_type = Some(match covalue_type {
            "comap" => CoValueType::Comap,
            "colist" => CoValueType::Colist,
            "coplaintext" => CoValueType::Coplaintext,
            "costream" => CoValueType::Costream,
            "BinaryCoStream" => CoValueType::BinaryCoStream,
            _ => return Err(JsError::new(&format!("Invalid CoValue type: {}", covalue_type))),
        });
        Ok(())
    }

    /// Set ruleset to group with initial admin
    #[wasm_bindgen(js_name = setRulesetGroup)]
    pub fn set_ruleset_group(&mut self, initial_admin: &str) {
        self.ruleset = Some(Ruleset::Group {
            initial_admin: initial_admin.to_string(),
        });
    }

    /// Set ruleset to owned by group
    #[wasm_bindgen(js_name = setRulesetOwnedByGroup)]
    pub fn set_ruleset_owned_by_group(&mut self, group_id: &str) {
        self.ruleset = Some(Ruleset::OwnedByGroup {
            group: CoID(group_id.to_string()),
        });
    }

    /// Set ruleset to unsafe allow all
    #[wasm_bindgen(js_name = setRulesetUnsafeAllowAll)]
    pub fn set_ruleset_unsafe_allow_all(&mut self) {
        self.ruleset = Some(Ruleset::UnsafeAllowAll);
    }

    /// Set meta from JSON string (parsed into BTreeMap for sorted serialization)
    #[wasm_bindgen(js_name = setMeta)]
    pub fn set_meta(&mut self, meta_json: Option<String>) -> Result<(), JsError> {
        self.meta = match meta_json {
            Some(json) => {
                let value: serde_json::Value = serde_json::from_str(&json)?;
                match value {
                    serde_json::Value::Object(map) => {
                        // Convert to BTreeMap for sorted key serialization
                        Some(map.into_iter().collect())
                    }
                    serde_json::Value::Null => None,
                    _ => return Err(JsError::new("meta must be an object or null")),
                }
            }
            None => None,
        };
        Ok(())
    }

    /// Set uniqueness to null
    #[wasm_bindgen(js_name = setUniquenessNull)]
    pub fn set_uniqueness_null(&mut self) {
        self.uniqueness = Some(Uniqueness::Null);
    }

    /// Set uniqueness to a boolean value
    #[wasm_bindgen(js_name = setUniquenessBool)]
    pub fn set_uniqueness_bool(&mut self, value: bool) {
        self.uniqueness = Some(Uniqueness::Bool(value));
    }

    /// Set uniqueness to a string value (most common case)
    #[wasm_bindgen(js_name = setUniquenessString)]
    pub fn set_uniqueness_string(&mut self, value: &str) {
        self.uniqueness = Some(Uniqueness::String(value.to_string()));
    }

    /// Set uniqueness to a flat object with string values
    /// Accepts a JSON string representing an object with string keys and string values
    /// Example: {"key1": "value1", "key2": "value2"}
    #[wasm_bindgen(js_name = setUniquenessObject)]
    pub fn set_uniqueness_object(&mut self, json: &str) -> Result<(), JsError> {
        let value: serde_json::Value = serde_json::from_str(json)?;
        match value {
            serde_json::Value::Object(map) => {
                let mut btree: BTreeMap<String, String> = BTreeMap::new();
                for (k, v) in map {
                    match v {
                        serde_json::Value::String(s) => {
                            btree.insert(k, s);
                        }
                        _ => return Err(JsError::new("uniqueness object values must be strings")),
                    }
                }
                self.uniqueness = Some(Uniqueness::Object(btree));
                Ok(())
            }
            _ => Err(JsError::new("uniqueness must be an object with string values")),
        }
    }

    /// Set createdAt to null
    #[wasm_bindgen(js_name = setCreatedAtNull)]
    pub fn set_created_at_null(&mut self) {
        self.created_at = Some(CreatedAt::Null);
    }

    /// Set createdAt to a timestamp string
    #[wasm_bindgen(js_name = setCreatedAtTimestamp)]
    pub fn set_created_at_timestamp(&mut self, timestamp: &str) {
        self.created_at = Some(CreatedAt::Timestamp(timestamp.to_string()));
    }

    /// Compute the CoValue ID from the header
    /// Returns the ID in format "co_z${hash}"
    #[wasm_bindgen(js_name = computeId)]
    pub fn compute_id(&self) -> Result<String, JsError> {
        let bytes = self.canonical_bytes()?;
        let hash = blake3::hash(&bytes);
        // Take first 19 bytes for short hash
        Ok(format!("co_z{}", bs58::encode(&hash.as_bytes()[..19]).into_string()))
    }

    /// Get the canonical bytes (for testing/debugging)
    #[wasm_bindgen(js_name = canonicalBytes)]
    pub fn canonical_bytes_js(&self) -> Result<Box<[u8]>, JsError> {
        Ok(self.canonical_bytes()?.into_boxed_slice())
    }

    /// Internal: produce canonical JSON bytes matching stableStringify output
    fn canonical_bytes(&self) -> Result<Vec<u8>, JsError> {
        // Build a BTreeMap to ensure sorted key order
        let mut map: BTreeMap<&str, serde_json::Value> = BTreeMap::new();

        // Add fields in any order - BTreeMap sorts them alphabetically
        if let Some(ref created_at) = self.created_at {
            map.insert("createdAt", serde_json::to_value(created_at)?);
        }

        map.insert("meta", match &self.meta {
            Some(m) => serde_json::to_value(m)?,
            None => serde_json::Value::Null,
        });

        map.insert("ruleset", serde_json::to_value(
            self.ruleset.as_ref().ok_or_else(|| JsError::new("ruleset not set"))?
        )?);

        map.insert("type", serde_json::to_value(
            self.covalue_type.as_ref().ok_or_else(|| JsError::new("type not set"))?
        )?);

        map.insert("uniqueness", serde_json::to_value(
            self.uniqueness.as_ref().ok_or_else(|| JsError::new("uniqueness not set"))?
        )?);

        Ok(serde_json::to_vec(&map)?)
    }
}
```

### 1b. Canonical Encoders (Rust - cojson-core)

Additional encoding functions in `crates/cojson-core/src/core/canonical.rs`:

```rust
/// Canonical encoding for seal/encrypt nonce material
pub fn encode_nonce_material(material: &SealNonceMaterial) -> Vec<u8>;

/// Canonical encoding for key-wrapping nonce material
pub fn encode_key_nonce_material(encrypted_id: &KeyID, encrypting_id: &KeyID) -> Vec<u8>;
```

These encoders use `BTreeMap` internally to produce JSON bytes with sorted keys (matching `stableStringify` behavior) for backward compatibility.

### 2. Typed Data Structures (Rust)

```rust
use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};

/// Uniqueness value - constrained to actual TypeScript type from verifiedState.ts
/// 
/// The TypeScript type is:
/// ```typescript
/// type Uniqueness =
///   | string
///   | boolean
///   | null
///   | undefined
///   | { [key: string]: string };  // Flat object with string values only
/// ```
/// 
/// Common usages:
/// - null (accounts, some tests)
/// - z${string} (random 12 bytes base58 from uniquenessForHeader())
/// - "" (empty string for branches)
/// - User-provided strings (findUnique, upsertUnique, loadUnique)
/// - Flat objects with string values (user-provided via API)
/// 
/// Note: `undefined` in TypeScript maps to `None` in Rust (the field is omitted).
/// Objects use BTreeMap to ensure deterministic key ordering for canonical encoding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Uniqueness {
    Null,
    Bool(bool),
    String(String),
    Object(BTreeMap<String, String>),  // Flat object: string keys, string values only
}

/// CoValue type enumeration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CoValueType {
    Comap,
    Colist,
    Coplaintext,
    Costream,
    BinaryCoStream,
}

/// Ruleset enumeration with serde for canonical JSON output
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Ruleset {
    Group { 
        #[serde(rename = "initialAdmin")]
        initial_admin: String 
    },
    OwnedByGroup { group: CoID },
    UnsafeAllowAll,
}

/// CreatedAt value - constrained to actual usage patterns
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CreatedAt {
    Null,
    Timestamp(String),  // ISO 8601 format starting with "2"
}

/// Internal CoValueHeader structure
/// Uses BTreeMap for meta to ensure sorted key serialization
pub struct CoValueHeaderInternal {
    pub covalue_type: CoValueType,
    pub ruleset: Ruleset,
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
    pub uniqueness: Uniqueness,
    pub created_at: Option<CreatedAt>,
}

/// Nonce material for seal/unseal operations
pub struct SealNonceMaterial {
    pub in_id: CoID,
    pub tx: TransactionID,
}

/// Nonce material for key wrapping
pub struct KeyNonceMaterial {
    pub encrypted_id: KeyID,
    pub encrypting_id: KeyID,
}
```

#### Uniqueness Type Rationale

The `uniqueness` field has a **constrained type** defined in `verifiedState.ts`:

```typescript
export type Uniqueness =
  | string
  | boolean
  | null
  | undefined
  | {
      [key: string]: string;  // Flat object with string values only
    };
```

This constrained type simplifies the Rust implementation significantly:

| Value | Usage | Location |
|-------|-------|----------|
| `null` | Account headers | `account.ts` |
| `z${string}` | Random uniqueness | `crypto.ts` `uniquenessForHeader()` |
| `""` | Branch headers | `branching.ts` |
| `boolean` | Rare, but supported | Public API |
| User string | Unique lookups | `coMap.ts`, `coList.ts` |
| Flat object | Custom uniqueness via API | `CoMap.create({ unique: { key: "value" } })` |

**Key constraints that simplify the implementation:**

1. **No nested objects**: The object variant only allows `{ [key: string]: string }`, not nested structures. This eliminates the need for recursive canonical encoding.

2. **No arrays**: Arrays are not supported in uniqueness values.

3. **No numbers/floats**: Only strings, booleans, and null are supported as primitive values. This avoids float serialization determinism issues between JS and Rust.

4. **BTreeMap for objects**: The flat `{ [key: string]: string }` object uses `BTreeMap<String, String>` in Rust, which naturally produces sorted keys matching `stableStringify` behavior.

The `undefined` case in TypeScript maps to `Option<Uniqueness>` being `None` in Rust - the field is simply omitted from serialization.

### 3. Updated CryptoProvider Interface (TypeScript)

```typescript
abstract class CryptoProvider {
  // Header hashing - uses Rust canonical encoding
  abstract shortHashHeader(header: CoValueHeader): ShortHash;
  
  // Byte-oriented signing (string → UTF-8 bytes)
  abstract signBytes(secret: SignerSecret, message: Uint8Array): Signature;
  abstract signString(secret: SignerSecret, message: string): Signature;
  
  abstract verifyBytes(sig: Signature, message: Uint8Array, id: SignerID): boolean;
  abstract verifyString(sig: Signature, message: string, id: SignerID): boolean;
  
  // Hash operations
  abstract secureHashBytes(data: Uint8Array): Hash;
  abstract secureHashString(data: string): Hash;
  
  // Seal with typed nonce material
  abstract seal<T extends JsonValue>(params: {
    message: T;
    from: SealerSecret;
    to: SealerID;
    nOnceMaterial: SealNonceMaterial;
  }): Sealed<T>;
  
  // Encrypt with typed nonce material
  abstract encrypt<T extends JsonValue>(
    value: T,
    keySecret: KeySecret,
    nOnceMaterial: KeyNonceMaterial | SealNonceMaterial,
  ): Encrypted<T>;
  
  // Legacy compatibility (deprecated, to be removed)
  /** @deprecated Use signString or signBytes instead */
  sign(secret: SignerSecret, message: JsonValue): Signature;
}
```

### 4. WASM/NAPI Binding Updates

New exports from `cojson-core-wasm` and `cojson-core-napi`:

```typescript
// CoValueHeaderBuilder - Rust handler exposed to JS
export class CoValueHeaderBuilder {
  constructor();
  free(): void;
  
  // Type setters
  setType(covalue_type: string): void;
  
  // Ruleset setters
  setRulesetGroup(initial_admin: string): void;
  setRulesetOwnedByGroup(group_id: string): void;
  setRulesetUnsafeAllowAll(): void;
  
  // Meta setter (accepts JSON string or null)
  setMeta(meta_json: string | null): void;
  
  // Uniqueness setters
  setUniquenessNull(): void;
  setUniquenessBool(value: boolean): void;
  setUniquenessString(value: string): void;
  setUniquenessObject(json: string): void;  // For flat objects: { [key: string]: string }
  
  // CreatedAt setters
  setCreatedAtNull(): void;
  setCreatedAtTimestamp(timestamp: string): void;
  
  // Compute the CoValue ID without returning serialized bytes
  computeId(): string;
  
  // Get canonical bytes (for testing/debugging)
  canonicalBytes(): Uint8Array;
}

// NonceMaterialBuilder - Rust handler for nonce material
export class NonceMaterialBuilder {
  constructor();
  free(): void;
  
  // For seal nonce material
  setInId(co_id: string): void;
  setTxSessionId(session_id: string): void;
  setTxIndex(tx_index: number): void;
  
  // For key nonce material
  setEncryptedId(key_id: string): void;
  setEncryptingId(key_id: string): void;
  
  // Get canonical bytes
  canonicalBytes(): Uint8Array;
}
```

**Usage Example:**

```typescript
// Creating a CoValue ID using the Rust handler
const builder = new CoValueHeaderBuilder();
builder.setType("comap");
builder.setRulesetOwnedByGroup(groupId);
builder.setMeta(null);
builder.setUniquenessString(crypto.uniquenessForHeader());
builder.setCreatedAtTimestamp(new Date().toISOString());

const coValueId = builder.computeId();  // "co_z..."
builder.free();  // Or let FinalizationRegistry handle it
```

### 5. Request/Auth Layer (jazz-tools)

The request/auth code in `packages/jazz-tools/src/tools/coValues/request.ts` remains responsible for:

- Constructing request payloads
- Serializing to JSON using `stableStringify`
- Passing the serialized string to crypto for hashing/signing

```typescript
// Request envelope signing (jazz-tools)
const payload = stableStringify({
  contentPieces,
  id: envelope.$jazz.id,
  createdAt,
  signerID,
});

const signPayload = crypto.secureHashString(payload);
const authToken = crypto.signString(signerSecret, signPayload);
```

This keeps request-specific logic outside the crypto layer.

## Data Models

### CoValueHeader Canonical Form

The `CoValueHeader` has these possible shapes:

```typescript
type CoValueHeader = {
  type: "comap" | "colist" | "coplaintext" | "costream" | "BinaryCoStream";
  ruleset: 
    | { type: "group"; initialAdmin: RawAccountID | AgentID }
    | { type: "ownedByGroup"; group: RawCoID }
    | { type: "unsafeAllowAll" };
  meta: JsonObject | null;
  uniqueness: Uniqueness;  // Constrained type (see below)
  createdAt?: `2${string}` | null;
};

// From verifiedState.ts
type Uniqueness =
  | string
  | boolean
  | null
  | undefined
  | { [key: string]: string };  // Flat object with string values only
```

**Key Constraint**: The `uniqueness` field is **not** a full `JsonValue`. It is constrained to:
- Primitive values: `string`, `boolean`, `null`, `undefined`
- Flat objects with string values only: `{ [key: string]: string }`

This constraint simplifies the Rust implementation:

```rust
/// Uniqueness value - constrained to flat structures (no nesting)
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum Uniqueness {
    Null,
    Bool(bool),
    String(String),
    Object(BTreeMap<String, String>),  // Flat: string keys, string values only
}
```

The canonical encoding must:

1. Sort object keys alphabetically (matching `stableStringify`) - achieved via `BTreeMap`
2. Produce identical bytes to legacy `stableStringify(header)` for all existing header shapes
3. Handle the `createdAt` field correctly (omit if undefined, include if null or string)
4. Reject nested objects or arrays in uniqueness values (runtime validation)

### Nonce Material Canonical Forms

**Seal nonce material:**

```typescript
type SealNonceMaterial = {
  in: RawCoID;
  tx: TransactionID;
};

type TransactionID = {
  sessionID: SessionID;
  txIndex: number;
};
```

**Key-wrapping nonce material:**

```typescript
type KeyNonceMaterial = {
  encryptedID: KeyID;
  encryptingID: KeyID;
};
```

### Transaction Canonical Form

Transactions are already serialized by the Rust core using serde. The key insight is:

- `TrustingTransaction.changes` and `TrustingTransaction.meta` are **already strings** (pre-serialized JSON)
- `PrivateTransaction.encryptedChanges` and `PrivateTransaction.meta` are encrypted blobs

The Rust core's serde serialization naturally produces deterministic output for these structures.

## Migration Strategy

### Phase 1: Implement Rust Handlers

1. Implement `CoValueHeaderBuilder` handler in `crates/cojson-core/src/core/header.rs`
2. Implement `NonceMaterialBuilder` handler for seal/encrypt nonce material
3. Use `BTreeMap` internally for sorted key serialization
4. Add `Uniqueness` enum (constrained to flat structures) and `CreatedAt` enum with proper serde attributes
5. Add comprehensive parity tests comparing Rust output to JS `stableStringify` output

### Phase 2: Expose Handlers via Bindings

1. Export `CoValueHeaderBuilder` via WASM (`#[wasm_bindgen]`)
2. Export `CoValueHeaderBuilder` via NAPI (`#[napi]`)
3. Export via React Native bindings (UniFFI)
4. Ensure memory management is correct (FinalizationRegistry for WASM)

### Phase 3: Update CryptoProvider

1. Add new byte/string-oriented methods (`signBytes`, `signString`, etc.)
2. Update `idforHeader` to use `CoValueHeaderBuilder.computeId()`
3. Update seal/encrypt to use `NonceMaterialBuilder` for nonce encoding
4. Keep legacy methods as deprecated wrappers

### Phase 4: Update Callers

1. Update all `idforHeader` call sites to use the builder pattern
2. Update seal/encrypt call sites to use typed nonce material builders
3. Update request/auth code in jazz-tools to use string-based signing
4. Verify `stableStringify` usage in session log adapter is already handled by Rust

### Phase 5: Remove Legacy Code

1. Remove deprecated `sign(message: JsonValue)` methods
2. Remove `stableStringify` from crypto layer
3. Keep `stableStringify` only where needed outside crypto (e.g., debugging, tests)
4. TypeScript types already use constrained `Uniqueness` type (defined in `verifiedState.ts`)

## Backward Compatibility

### CoValue IDs

CoValue IDs are derived from `shortHash(header)`. To maintain compatibility:

- The Rust `encode_header` must produce **identical bytes** to `stableStringify(header)`
- This is verified by extensive fixtures comparing Rust and JS outputs

### Signatures

Existing signatures were created by signing `stableStringify(message)`:

- Most callers already sign strings (e.g., hash outputs like `"hash_z..."`)
- The new `signString` method signs `textEncoder.encode(message)`
- For `JsonValue` inputs that are strings, `sign(message)` and `signString(message)` produce identical results

### Sealed/Encrypted Data

Legacy sealed/encrypted payloads used `stableStringify` for both message and nonce material:

- The Rust canonical encoders match `stableStringify` output for the supported nonce material shapes
- For unseal/decrypt, both legacy and new nonce encodings are tried (fallback mechanism)

## Error Handling

### Type Errors

The new APIs are more strictly typed. If a caller attempts to hash/sign arbitrary JSON:

```typescript
// Old (accepts any JSON)
crypto.shortHash(arbitraryValue);

// New (requires typed header)
crypto.shortHashHeader(header); // TypeScript enforces CoValueHeader type
```

TypeScript catches misuse at compile time.

### Runtime Errors

For edge cases where types don't catch issues:

```rust
pub fn encode_header(header: &CoValueHeader) -> Result<Vec<u8>, CanonicalEncodingError> {
    // Validates header structure
    // Returns clear error if encoding fails
}
```

### Decryption Fallback

For legacy data, unseal/decrypt operations try:

1. New canonical nonce encoding
2. If decryption fails, retry with legacy `stableStringify`-equivalent encoding

This is implemented in the Rust core:

```rust
pub fn unseal_with_fallback(
    sealed: &str,
    sealer: &SealerSecret,
    from: &SealerID,
    nonce_material: &SealNonceMaterial,
) -> Result<Vec<u8>, UnsealError> {
    // Try new encoding first
    if let Ok(result) = unseal(sealed, sealer, from, encode_nonce_material(nonce_material)) {
        return Ok(result);
    }
    // Fallback to legacy encoding
    unseal(sealed, sealer, from, legacy_encode_nonce_material(nonce_material))
}
```

## Testing Strategy

### Unit Tests

1. **Canonical encoding parity tests**: Compare Rust encoder output to JS `stableStringify` for all header/nonce shapes
2. **Signature verification tests**: Verify signatures created with new API can be verified with old, and vice versa
3. **Encryption/seal round-trip tests**: Ensure data encrypted with new API can be decrypted, including legacy fallback

### Fixture-Based Tests

Create shared test fixtures (JSON files) that can be validated by both JS and Rust:

```json
{
  "headers": [
    {
      "input": { "type": "comap", "ruleset": { "type": "group", "initialAdmin": "co_z..." }, ... },
      "expectedBytes": "base64...",
      "expectedHash": "shortHash_z..."
    }
  ],
  "nonceMaterials": [
    {
      "input": { "in": "co_z...", "tx": { "sessionID": "...", "txIndex": 0 } },
      "expectedBytes": "base64..."
    }
  ]
}
```

### Integration Tests

1. **Cross-platform ID derivation**: Create CoValues in JS, verify IDs match when derived in Rust
2. **Cross-platform seal/unseal**: Seal in JS, unseal in Rust (and vice versa)
3. **Legacy data loading**: Load existing stored data to verify backward compatibility

### Performance Tests

Benchmark against current `stableStringify`-based implementation:

1. Header hashing throughput
2. Signing/verification throughput
3. Large request payload handling (contentPieces)

## Benefits of Handler Approach

### 1. Avoids Serialization Overhead

Instead of serializing data in JS, passing it across the boundary, and deserializing in Rust, JS directly manipulates a Rust-owned handle:

```typescript
// Old approach: serialize → transfer → deserialize → serialize again
const bytes = textEncoder.encode(stableStringify(header));
const id = computeIdFromBytes(bytes);

// New approach: direct manipulation, no intermediate serialization
const builder = new CoValueHeaderBuilder();
builder.setType("comap");
builder.setRulesetGroup(adminId);
// ... Rust computes canonical bytes internally
const id = builder.computeId();
```

### 2. Type Safety at Compile Time

The handler API enforces correct structure:

```typescript
// Old: accepts any JsonValue, errors at runtime
crypto.shortHash({ invalid: "structure" });

// New: TypeScript enforces valid method calls
builder.setType("comap");           // ✓ Valid
builder.setType("invalid");         // ✗ Runtime error with clear message
builder.setUniquenessString("z.."); // ✓ Explicitly typed
```

### 3. BTreeMap Guarantees Sorted Keys

Rust's `BTreeMap` naturally maintains sorted key order, matching `stableStringify` behavior:

```rust
// Keys automatically sorted alphabetically
let mut map: BTreeMap<&str, Value> = BTreeMap::new();
map.insert("type", ...);      // Will appear after "ruleset"
map.insert("meta", ...);      // Will appear after "createdAt"
map.insert("createdAt", ...); // Will appear first

// Serializes as: {"createdAt":...,"meta":...,"ruleset":...,"type":...,"uniqueness":...}
```

### 4. Follows Existing Patterns

The approach mirrors the existing `SessionLog` implementation, reducing learning curve and ensuring consistency across the codebase.

### 5. Constrained Uniqueness Type

The `Uniqueness` type is constrained to flat structures (no nesting), which provides several benefits:

- **Simpler implementation**: No recursive canonical encoding needed - objects only contain string values
- **No float determinism issues**: Only strings, booleans, and null are supported as primitives
- **Type safety**: `BTreeMap<String, String>` naturally enforces the flat object constraint
- **Convenience methods**: `setUniquenessNull()`, `setUniquenessBool()`, `setUniquenessString()`, `setUniquenessObject()`
- **Runtime validation**: `setUniquenessObject()` validates that all values are strings, rejecting nested structures

## Open Questions Resolution

### Q1: Byte-for-byte compatibility vs. narrowing header shapes

**Decision**: Maintain byte-for-byte compatibility.

The Rust `CoValueHeaderBuilder.canonical_bytes()` produces output identical to `stableStringify` for all header shapes. This is achievable because:

- `stableStringify` behavior is well-defined (sorted keys, specific string handling)
- `BTreeMap` naturally produces sorted keys in Rust
- The `uniqueness` field is constrained to flat structures (no nesting), simplifying canonical encoding
- Only strings, booleans, and null are supported as primitive uniqueness values (no floats)
- We can add comprehensive parity tests for all supported uniqueness shapes

### Q2: Sealed message shapes - structured vs opaque

**Decision**: Treat seal message as opaque bytes at the crypto boundary.

The seal operation accepts:

```typescript
seal({
  message: T,                    // Caller provides JSON value
  from: SealerSecret,
  to: SealerID,
  nOnceMaterial: NonceMaterialBuilder, // Rust handler for nonce
}): Sealed<T>
```

The `message` is serialized using `JSON.stringify` (not `stableStringify`) before sealing because:

- The sealed content is not used for ID derivation
- Determinism is only needed for nonce material
- This simplifies the API and allows any JSON value

The nonce material uses the Rust `NonceMaterialBuilder` handler because it must be deterministic for encryption/decryption to work correctly.

### Q3: Handler vs function-based API

**Decision**: Use handler (builder) pattern.

Reasons:
- Headers are often built incrementally in application code
- Builder pattern allows validation at each step
- Matches existing `SessionLog` pattern in codebase
- Enables future extensions (e.g., adding new fields) without breaking API
- Memory management is well-understood (FinalizationRegistry for WASM)

