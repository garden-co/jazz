## Overview

This design removes `stableStringify` from the crypto boundary by making crypto operations strictly **byte-oriented**, while **preserving backward compatibility for all existing IDs and cryptographic artifacts**.

Today, `packages/cojson/src/crypto/*Crypto.ts` and `CryptoProvider` compute bytes by calling `stableStringify(...)` in JS and then hashing/signing/encrypting those bytes. This design changes the boundary so crypto consumes **bytes**; higher layers decide how to encode inputs (typed Rust encoders for supported shapes, or explicit compatibility shims where legacy artifacts require it).

Key constraints from requirements:

- Legacy IDs/signatures/sealed/encrypted/session logs must keep working.
- The long-term API direction is to prefer **explicit string/bytes** and **typed inputs** at the crypto boundary.
- `shortHash`/CoValue IDs must remain **byte-for-byte compatible** with current `stableStringify` behavior for supported `CoValueHeader` shapes.

## Architecture / components

### 1) Cojson-core API surface (bytes first)

Extend `cojson-core` (and thus `cojson-core-wasm`, `cojson-core-napi`, `cojson-core-rn`) with functions that accept either:

- **bytes/string inputs** (preferred), or
- **structured inputs** for the finite set of shapes we care about, which are encoded in Rust.

Concretely, add Rust entrypoints such as:

- `hash_bytes(data: &[u8]) -> Hash`
- `short_hash_bytes(data: &[u8]) -> ShortHash`
- `sign_bytes(secret, data: &[u8]) -> Signature`
- `verify_bytes(signature, data: &[u8], signer_id) -> bool`
- `encrypt_bytes(plaintext: &[u8], key_secret, nonce_material_bytes: &[u8]) -> Encrypted`
- `seal_bytes(plaintext: &[u8], from, to, nonce_material_bytes: &[u8]) -> Sealed`

and, where it’s valuable, higher-level helpers like:

- `encode_nonce_material_keywrap({ encryptedID, encryptingID }) -> Vec<u8>`
- `encode_nonce_material_seal({ in, tx }) -> Vec<u8>`

Design note:

- We explicitly do **not** implement a Rust clone of JS `stableStringify`.
- Backward compatibility for existing artifacts is achieved by ensuring callers can reproduce the legacy bytes they were created with (which may mean continuing to use JS `stableStringify` in a compatibility shim outside crypto for legacy cases).

### 2) JS/TS wrapper changes (WASM/NAPI/RN)

Update `packages/cojson/src/crypto/WasmCrypto.ts`, `NapiCrypto.ts`, `RNCrypto.ts` and `CryptoProvider` so crypto primitives operate on **bytes**, not arbitrary JSON.

Wrappers/higher layers may still perform encoding, but it becomes explicit:

- Prefer Rust encoders for supported typed shapes (e.g. nonce material).
- For legacy compatibility where required (notably existing CoValue IDs derived from legacy header hashing), the wrapper/higher layer may use JS `stableStringify` as a compatibility shim to reproduce legacy bytes.

`CryptoProvider.secureHash` / `shortHash` should be refactored to accept bytes (or string) inputs so `stableStringify` is not “hidden” in crypto APIs.

### 3) SessionLog / transactions

Today `SessionLogAdapter` passes `stableStringify(tx)` into `cojson-core` session log APIs.

Design change:

- Prefer Rust-owned serde serialization for `Transaction`/`TrustingTransaction`/`PrivateTransaction` shapes inside `cojson-core`.
- Expose a binding-level API that accepts structured transactions (or a canonical “Transaction DTO”) and serializes them in Rust.

If binding constraints force keeping string inputs, provide a Rust-exposed `serialize_transaction(tx)` helper so JS can pass strings without using `stableStringify`.

### 4) “Typed-first” encoders (only for explicitly supported shapes)

For the long term, define typed/structured inputs for the finite shapes we actually sign/hash:

- `CoValueHeader`
- nonce material shapes

Encoding remains Rust-owned and must remain backward compatible. Where byte-for-byte compatibility with the legacy stringified form is required, typed encoders must be proven equivalent to the legacy encoder for that shape.

## Data models

### Typed models (recommended)

- `CoValueHeader` (exact variants derived from current header shapes; to be enumerated during implementation)
- `NonceMaterialKeyWrap = { encryptedID: KeyID, encryptingID: KeyID }`
- `NonceMaterialSeal = { in: RawCoID, tx: TransactionID }`

## Error handling

- Encoding errors (unsupported shapes, cycles) should surface as explicit errors in JS (not silent coercions), matching current behavior where applicable.
- `decrypt`/`unseal` should continue to return `undefined` on failure at the high-level API, while logging (consistent with current behavior).
- For typed encoders, validation failures should include enough context to debug (which field was invalid), but must not leak secrets.

## Testing strategy

### Cross-language golden vectors

- Create a set of golden fixtures that include:
  - representative `CoValueHeader` instances
  - nonce materials
  - edge cases from `stableStringify` (non-finite numbers, `undefined`, arrays, key ordering, special `encrypted_U`/`binary_U` strings)
- Assert that:
  - `shortHash` and derived `co_z...` IDs match current outputs
  - sign/verify/encrypt/seal outputs match current outputs for the same inputs

### Platform parity

- Run the same fixture suite against WASM/NAPI/RN bindings (at least smoke-level parity) to ensure no platform-specific divergence.

### Session log invariants

- Add fixtures for transaction chains to ensure signature-chain verification is unchanged when moving transaction serialization into Rust.

## Open questions / follow-ups

- Enumerate the exact set of `CoValueHeader` variants currently produced in the wild; confirm we can encode them in Rust in a way that exactly matches the legacy `stableStringify` bytes.
- Confirm the request/auth signing scheme remains owned by `jazz-tools` (outside crypto), and that crypto only exposes byte-oriented primitives needed by that scheme.
