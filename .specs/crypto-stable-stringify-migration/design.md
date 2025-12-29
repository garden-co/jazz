# Crypto Stable Stringify Migration - Design

## Overview

This design addresses the migration from JS `stableStringify`-based canonicalization to Rust-owned canonical encodings for cryptographic operations. The goal is to eliminate the dependency on JavaScript-specific JSON canonicalization in the crypto layer while maintaining backward compatibility and performance.

The approach involves:

1. **Defining explicit data shapes** for all crypto-relevant inputs currently flowing through `stableStringify`
2. **Moving canonical encoding to Rust** (cojson-core) and exposing it via WASM/RN/NAPI bindings
3. **Constraining crypto APIs** to accept bytes/strings rather than arbitrary JSON
4. **Keeping request/auth serialization in jazz-tools** outside the crypto layer

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
│    - shortHash(header: CoValueHeader) → calls Rust encoder      │
│    - sign(secret, message: string | bytes)                      │
│    - verify(sig, message: string | bytes, id)                   │
│    - encrypt/seal with typed nonce material                     │
│                             │                                   │
│                             ▼                                   │
├─────────────────────────────────────────────────────────────────┤
│                      Rust Core (cojson-core)                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Canonical Encoders (Rust-owned)                         │   │
│  │    - encodeCoValueHeader(header) → bytes                 │   │
│  │    - encodeNonceMaterial(material) → bytes               │   │
│  │    - Transaction serialization (serde-based)             │   │
│  └──────────────────────────────────────────────────────────┘   │
│                             │                                   │
│                             ▼                                   │
│   blake3HashOnce() │ sign() / verify() │ encrypt() / seal()    │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Canonical Encoders (Rust - cojson-core)

New module in `crates/cojson-core/src/core/canonical.rs`:

```rust
/// Canonical encoding for CoValueHeader
/// Produces bytes that match legacy stableStringify output for backward compatibility
pub fn encode_header(header: &CoValueHeader) -> Vec<u8>;

/// Canonical encoding for seal/encrypt nonce material
pub fn encode_nonce_material(material: &NonceMaterial) -> Vec<u8>;

/// Canonical encoding for key-wrapping nonce material
pub fn encode_key_nonce_material(encrypted_id: &KeyID, encrypting_id: &KeyID) -> Vec<u8>;
```

These encoders produce JSON bytes with sorted keys (matching `stableStringify` behavior) for backward compatibility.

### 2. Typed Data Structures (Rust)

```rust
/// CoValueHeader shapes for ID derivation
pub struct CoValueHeader {
    pub covalue_type: CoValueType,
    pub ruleset: Ruleset,
    pub meta: Option<JsonObject>,
    pub uniqueness: JsonValue,
    pub created_at: Option<String>,
}

pub enum Ruleset {
    Group { initial_admin: String },
    OwnedByGroup { group: CoID },
    UnsafeAllowAll,
}

pub enum CoValueType {
    CoMap,
    CoList,
    CoPlainText,
    CoStream,
    BinaryCoStream,
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
// Canonical encoding functions
export function encodeCoValueHeader(header: CoValueHeaderJs): Uint8Array;
export function encodeNonceMaterial(material: SealNonceMaterialJs): Uint8Array;
export function encodeKeyNonceMaterial(encryptedId: string, encryptingId: string): Uint8Array;

// Updated shortHash that accepts typed header
export function shortHashHeader(header: CoValueHeaderJs): string;
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
  meta: JsonObject | null; // TODO: We should convert JsonObject into the meta values used in cojson
  uniqueness: JsonValue;  // typically `z${string}` but can be any JSON string (TODO: verifiy if it's true)
  createdAt?: `2${string}` | null;
};
```

The canonical encoding must:

1. Sort object keys alphabetically (matching `stableStringify`)
2. Produce identical bytes to legacy `stableStringify(header)` for all existing header shapes
3. Handle the `createdAt` field correctly (omit if undefined, include if null or string)

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

### Phase 1: Add Rust Canonical Encoders

1. Implement `encode_header`, `encode_nonce_material`, `encode_key_nonce_material` in Rust
2. Add comprehensive tests comparing Rust output to JS `stableStringify` output
3. Expose via WASM/NAPI bindings

### Phase 2: Update CryptoProvider

1. Add new byte/string-oriented methods (`signBytes`, `signString`, etc.)
2. Add `shortHashHeader` that uses Rust canonical encoding
3. Update seal/encrypt to use typed nonce material with Rust encoding
4. Keep legacy methods as deprecated wrappers

### Phase 3: Update Callers

1. Update `idforHeader` to use `shortHashHeader`
2. Update seal/encrypt call sites to use typed nonce material
3. Update request/auth code in jazz-tools to use string-based signing
4. Remove `stableStringify` usage from session log adapter (already handled by Rust)

### Phase 4: Remove Legacy Code

1. Remove deprecated `sign(message: JsonValue)` methods
2. Remove `stableStringify` from crypto layer
3. Keep `stableStringify` only where needed outside crypto (e.g., debugging, tests)

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

## Open Questions Resolution

### Q1: Byte-for-byte compatibility vs. narrowing header shapes

**Decision**: Maintain byte-for-byte compatibility.

The Rust `encode_header` function produces output identical to `stableStringify` for all currently used header shapes. This is achievable because:

- `stableStringify` behavior is well-defined (sorted keys, specific number/string handling)
- The header shapes are finite and known
- We can add comprehensive parity tests

### Q2: Sealed message shapes - structured vs opaque

**Decision**: Treat seal message as opaque bytes at the crypto boundary.

The seal operation accepts:

```typescript
seal({
  message: T,                    // Caller provides JSON value
  from: SealerSecret,
  to: SealerID,
  nOnceMaterial: SealNonceMaterial, // Typed, Rust-encoded
}): Sealed<T>
```

The `message` is serialized using `JSON.stringify` (not `stableStringify`) before sealing because:

- The sealed content is not used for ID derivation
- Determinism is only needed for nonce material
- This simplifies the API and allows any JSON value

The nonce material uses the Rust canonical encoder because it must be deterministic for encryption/decryption to work correctly.

