## Overview

This feature removes TypeScript-side canonicalization (`stableStringify`) from the crypto pipeline by moving **deterministic serialization of predefined data structures** into Rust, and then routing all providers (`WasmCrypto`, `NapiCrypto`, `RNCrypto`) through those Rust implementations.

Key goals:

- Preserve the current TypeScript API surface and string formats (`signerSecret_z…`, `signature_z…`, `encrypted_U…`, `sealed_U…`, `hash_z…`, `shortHash_z…`).
- Preserve backwards compatibility for signatures, sealed payloads, encrypted payloads, and hashes by matching legacy `stableStringify` semantics **via Rust-owned structure serialization**.
- Do **not** change random number generation behavior (US7).
- Keep high performance across an **incremental** JS↔Rust migration by minimizing boundary conversions and enabling Rust-backed reuse of parsed structures (US8, US9).

Non-goals:

- Changing the randomness sources, RNG libraries, or key generation formats.
- Introducing a new externally-visible cryptographic scheme or changing existing prefixes/encodings.

## Architecture / components

### 1) Rust-owned data models for all crypto inputs

Instead of implementing a “`stableStringify`-equivalent” function that takes arbitrary JSON text, we treat determinism as a **data modeling** problem:

- Every value that currently flows into `stableStringify(...)` is represented in Rust as a concrete `struct`/`enum`.
- Deterministic encoding is achieved by `serde` serialization of those Rust types (plus deterministic map types where needed), not by a “canonicalize JSON string” utility.

This matches your constraint: **Rust builds/owns the structures**, and serialization happens from those structures.

### 2) Structured crypto entrypoints in `crates/cojson-core` (no generic “JSON value”)

Add wrapper entrypoints that accept **specific Rust domain types**, serialize them deterministically, then call the existing byte-level primitives.

Concrete entrypoints (based on current production call sites):

- **Header hashing**
  - `short_hash_header(header: CoValueHeader) -> shortHash_z…`
  - (optional) `secure_hash_header(header: CoValueHeader) -> hash_z…` if needed beyond `shortHash`
- **Key secret revelation (seal/unseal)**
  - `seal_key_secret(secret: KeySecret, from: SealerSecret, to: SealerID, nonce: SealNonceMaterial) -> sealed bytes`
  - `unseal_key_secret(sealed bytes, sealer: SealerSecret, from: SealerID, nonce: SealNonceMaterial) -> KeySecret`
- **Key wrapping (encrypt/decrypt key secrets)**
  - `encrypt_key_secret(secret: KeySecret, sealing_key: KeySecret, nonce: KeyWrapNonceMaterial) -> encrypted bytes`
  - `decrypt_key_secret(encrypted bytes, sealing_key: KeySecret, nonce: KeyWrapNonceMaterial) -> KeySecret`
- **Signing (if still needed)**
  - Signing/verification must accept only explicitly-modeled message types (e.g. `Hash`), not arbitrary JSON objects.

Where:

- `SealNonceMaterial` and other nonce materials are *typed* records/structs.
Determinism is obtained by `serde` serialization of these concrete Rust types (including deterministic map types where relevant).

#### Example (realistic): seal a read key using typed nonce material (WASM/N-API shape)

Rust (exported by `cojson-core-wasm` / `cojson-core-napi`):

```rust
// Minimal handle objects (stored in Rust; getters/setters exposed by binding layer).
pub struct KeySecretHandle {
    pub value: String, // "keySecret_z..."
}

pub struct SealNonceHandle {
    pub in_co_id: String,  // "co_z..."
    pub session_id: String, // "..._session_z..."
    pub tx_index: u32,
}

// Exported function that consumes handles and calls existing crypto::seal::seal
pub fn seal_key_secret_from_handles(
    secret: &KeySecretHandle,
    from_sealer_secret: &str, // "sealerSecret_z..."
    to_sealer_id: &str,       // "sealer_z..."
    nonce: &SealNonceHandle,
) -> String {
    // Typed nonce -> deterministic bytes (serde)
    let nonce_json = serde_json::json!({
        "in": nonce.in_co_id,
        "tx": { "sessionID": nonce.session_id, "txIndex": nonce.tx_index }
    });
    let nonce_bytes = serde_json::to_vec(&nonce_json).unwrap();

    let sealed = crate::crypto::seal::seal(secret.value.as_bytes(), from_sealer_secret, to_sealer_id, &nonce_bytes).unwrap();
    format!("sealed_U{}", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&*sealed))
}
```

TypeScript consumer (short-hand getters/setters + passing handles back into Rust):

```ts
import {
  KeySecretHandle,
  SealNonceHandle,
  sealKeySecretFromHandles,
} from "cojson-core-wasm"; // or "cojson-core-napi"

const readKey = new KeySecretHandle("keySecret_z...");
const nonce = new SealNonceHandle("co_zGROUP...", "co_zGROUP..._session_z...", 0);

// shorthand mutation via setters (Rust owns the fields)
nonce.inCoId = "co_zGROUP...";
nonce.txIndex = 1;

// pass handles back into Rust crypto
const sealed = sealKeySecretFromHandles(
  readKey,
  "sealerSecret_zSENDER...",
  "sealer_zRECIPIENT...",
  nonce,
);

// store `sealed_U...` exactly like current `cojson` does
group.set(`${readKeyId}_for_${agentId}`, sealed, "trusting");
```

### 3) Bindings: WASM / N-API / RN expose Rust-backed data structures (getters/setters)

Bindings should expose the Rust data models as **first-class objects** to JavaScript/TypeScript, so that:

- Parsing/conversion happens once at the boundary.
- After that, JS interacts with the **Rust version of the data structure** via methods and **getters/setters** implemented in Rust.
- Updates to the structure are performed through Rust APIs, keeping both behavior and serialization deterministic and centralized.

Implementation model:

- JS holds an opaque handle to a Rust object (WASM class / N-API class / UniFFI object).
- The Rust object internally stores the typed model (e.g. `CoValueHeader`, `SealNonceMaterial`, `KeyWrapNonceMaterial`, etc.).
- Rust provides:
  - **getters** for reading fields
  - **setters** for mutation (where mutation is allowed by the domain model)
  - operation methods (sign/hash/seal/encrypt/…)

#### Example (realistic): handle reuse across multiple Rust calls

```ts
import { CoValueHeaderHandle, shortHashHeader } from "cojson-core-wasm";

// Create once (Rust owns structure)
const header = new CoValueHeaderHandle();
header.type = "group";
header.ruleset = { /* ... */ };
header.uniqueness = "zABC...";

// Reuse without re-parsing:
const h1 = shortHashHeader(header);   // pass handle into Rust
header.type = "account";             // mutate in Rust via setter
const h2 = shortHashHeader(header);   // pass same handle again
```

Platform notes:

- **WASM**: expose `#[wasm_bindgen]` classes wrapping Rust structs. JS creates/receives instances and calls methods/getters/setters on them.
- **N-API**: expose `#[napi]` classes wrapping Rust structs. JS receives instances and calls methods/getters/setters on them.
- **React Native (UniFFI)**: expose UniFFI `object`s/`record`s as appropriate. Prefer UniFFI `object`s for mutable structures so we can offer getters/setters directly.

Performance note (optional optimization):

- We may keep a parallel JS representation for hot-path convenience (dual JS+Rust mirrors), but the **source of truth** must be the Rust structure and all updates must be applied in Rust (the JS mirror, if present, is a cache).

### Incremental migration strategy (performance-oriented)

To keep performance high while migrating step-by-step:

- Prefer **Rust handles** for repeated operations (e.g. header hashing, nonce reuse) to avoid repeated JS→Rust conversions.
- Avoid intermediate JSON strings and avoid JS re-serialization in hot paths; conversions occur once at the boundary.

### 4) TypeScript providers call structured Rust entrypoints (no `stableStringify`)

Update `packages/cojson/src/crypto/{WasmCrypto,NapiCrypto,RNCrypto}.ts` to:

- Replace all uses of `stableStringify(...)` in crypto operations with calls to the new structured binding functions.
- Keep the `CryptoProvider` method signatures unchanged (`sign(secret, message: JsonValue)` etc.), but pass the JS object directly to the binding layer for Rust-side deserialization.

The `CryptoProvider` base class remains responsible for:

- Type-level string formats and helpers (agent ID derivation, etc.).
- Randomness via `crypto.getRandomValues` for `randomBytes()` (unchanged).

## Data models

### Typed models used by crypto (replacing `stableStringify` inputs)

For well-known shapes, define explicit Rust structs/records:

- **TransactionID** (for seal nonce material `tx`):
  - Fields (alphabetical): `branch?: RawCoID`, `sessionID: SessionID`, `txIndex: number`
- **Seal nonce material**:
  - `{ in: RawCoID, tx: TransactionID }`
- **Encrypt nonce material** (key wrapping):
  - `{ encryptedID: KeyID, encryptingID: KeyID }`

Generic “canonical JSON value” types are explicitly **out of scope**: crypto entrypoints must always receive a **specific modeled structure**.

Compatibility requirement:

- For each modeled shape, `serde_json` serialization output must match the previous TS `stableStringify(...)` output byte-for-byte (validated by vectors).

## Error handling / testing strategy

### Error handling

- The structured Rust entrypoints return existing crypto errors for:
  - Invalid prefixes (`signerSecret_z`, `sealerSecret_z`, `keySecret_z`, etc.)
  - Base58 decoding failures
  - Auth failures / decryption failures
- Additionally, they return deserialization errors when converting foreign values into Rust domain types.
- Binding layers map these to:
  - WASM: thrown `JsValue` errors with clear messages
  - N-API: `napi::Error` with `GenericFailure`
  - RN: UniFFI errors surfaced as JS exceptions

### Testing strategy

1) **Golden serialization vectors**

- Add test vectors that cover the Rust models’ deterministic serialization, asserting equality with legacy TS `stableStringify` for the same logical values.

2) **Crypto compatibility vectors**

- For each operation (sign/verify, seal/unseal, encrypt/decrypt, secureHash/shortHash), add vectors that:
  - Use fixed keys and fixed nonce materials
  - Assert outputs match the legacy implementation byte-for-byte

3) **Cross-provider parity**

- Add TS-level tests that run the same vectors against `WasmCrypto` and `NapiCrypto` (and `RNCrypto` where available in CI) and assert identical results.


