## Introduction

We currently rely on `stableStringify` to turn dynamic JSON values into deterministic bytes for hashing and signing in the crypto layer. This makes the codebase hard to migrate into Rust because:

- `stableStringify` is a JavaScript-specific canonicalization (including quirks) that is non-trivial to replicate byte-for-byte in Rust.
- The crypto boundary accepts “any JSON”, which hides the fact that only a finite set of data shapes actually flow into hashing/signing today.

This spec defines a migration path from **dynamic JSON inputs** to a small set of **explicit, static data structures** whose canonical encoding is defined and implemented in Rust (and called from JS/WASM/RN/NAPI bindings).

The key goal is to simplify future migrations into Rust while maintaining or improving performance.

## User stories

- As a maintainer, I want crypto operations to accept **explicit, typed inputs** (or already-encoded strings/bytes), so that canonicalization is not a hidden cross-language footgun.
- As a maintainer, I want canonical encoding to be **Rust-owned** and shared by all bindings (WASM/RN/NAPI), so that we don’t re-implement tricky determinism logic in JS.
- As a user with existing data, I want all **previously generated IDs, signatures, tokens, and sessions** to continue to verify/load.
- As a performance-conscious user, I want hashing/signing to be at least as fast as today (and ideally faster)

## Requirements (acceptance criteria, EARS style)

### R1 — Canonical encoding ownership and portability

- The system shall define a finite set of canonical encodings for the crypto-relevant data shapes currently flowing into hashing/signing/sealing.
- The system shall implement these canonical encodings in Rust and expose them consistently via WASM/RN/NAPI bindings.
- When the JS/TS layer needs bytes for crypto, the system shall obtain those bytes from the Rust-owned canonical encoding (or from explicit, already-encoded inputs), not from JS `stableStringify`.

### R2 — `shortHash` and CoValue ID derivation (always backward-compatible)

- The system shall define a canonical encoding for the full set of `CoValueHeader` shapes used by `idforHeader(header, crypto)`.
- When deriving a CoValue ID from a header, the system shall produce an ID that is stable and deterministic across platforms.
- The system shall preserve backward compatibility for CoValue IDs by ensuring that header hashing yields the same `co_z...` IDs as the current `stableStringify`-based implementation for all supported header shapes.

### R3 — Request/auth signing stays outside crypto (crypto is byte-oriented)

- The system shall keep request/auth payload construction and serialization outside the crypto layer (e.g. in `jazz-tools`), so crypto does not need request-specific typed payload encoders.
- The system shall expose byte-oriented primitives (hash/sign/verify) that allow request/auth code to sign exactly the bytes it defines (e.g. a JSON body string encoded as UTF-8), without passing arbitrary JSON through crypto.
- The system shall preserve backward compatibility for existing request/auth signatures by ensuring the request/auth layer continues to produce/verifiy signatures according to its current scheme.

### R4 — `sign`/`verify` input constraints (prefer bytes/strings)

- The system shall constrain signing/verifying such that `sign` and `verify` operate on explicit bytes or strings, not arbitrary JSON values.
- When signing/verifying a string, the system shall define the byte representation (UTF-8) that is signed/verified.
- When a caller previously signed `stableStringify(message)`, the system shall provide an equivalent explicit input mode that preserves verification behavior.

### R5 — Encryption and sealing inputs and nonce material determinism

- The system shall allow encryption and sealing operations to accept an explicit plaintext representation (bytes or string) so the plaintext does not require deterministic JSON canonicalization.
- The system shall define a canonical encoding for `nOnceMaterial` for all call sites that require determinism (e.g. key wrapping `{ encryptedID, encryptingID }`, sealing nonce material such as `{ in: RawCoID, tx: TransactionID }`).
- When encrypting/decrypting or sealing/unsealing, the system shall use the Rust-owned canonical encoding for nonce material bytes.
- When decrypting/unsealing legacy payloads, the system shall continue to accept payloads produced using legacy `stableStringify`-based nonce material encoding.

### R6 — Session log / transactions (remove JS `stableStringify`)

- When adding transactions to the Rust core (e.g. `tryAdd(transactions)`), the system shall avoid `stableStringify(tx)` in JS and instead rely on the Rust core’s canonical transaction serialization.
- When constructing transactions that include `changes` and `meta`, the system shall ensure these fields are represented in the stable transaction shape in a way that does not require JS canonicalization (e.g. as pre-serialized strings), consistent with the Rust core’s existing serde-based encoding.
- The system shall preserve the signature-chain verification semantics for existing logs.

### R7 — Tooling and tests updated to new canonical rules

- When tests or tooling construct hashed/signed/sealed/encrypted inputs, the system shall do so using the new canonical encodings (or explicit string/byte inputs) rather than `stableStringify`.
- The system shall continue to validate determinism across platforms for the supported shapes (e.g. fixtures that can be checked in both JS and Rust).

### R8 — Performance and safety

- The system shall not regress performance relative to current `stableStringify`-based hashing/signing for representative payload sizes, with special attention to large request `contentPieces`.
- The system shall fail fast with clear error messages when a caller attempts to hash/sign arbitrary JSON outside the supported shapes (after migration constraints are enforced).

## Scope inventory (current `stableStringify` usage)

The current JS/TS codebase uses `stableStringify` for deterministic bytes across multiple crypto operations, not just hashing.

### 1) Hashing (BLAKE3) used for IDs and request authentication

- `shortHash(value: JsonValue)`
  - Used to derive CoValue IDs from headers:
    - `idforHeader(header, crypto)` computes `co_z...` from `crypto.shortHash(header)`.
  - Input is effectively `CoValueHeader` (a structured object), but it is still serialized as dynamic JSON today.
  - This is the hardest case because changing bytes changes IDs, and IDs are persisted and referenced.

- `secureHash(value: JsonValue)`
  - Used for request/auth payload hashing in `jazz-tools`:
    - Request envelope signing: `secureHash({ contentPieces, id, createdAt, signerID })`
    - Auth token signing: `secureHash({ id, createdAt })`

### 2) Signing / verification (Ed25519)

Signing/verifying currently signs the bytes of `stableStringify(message)`:

- `sign(secret, message: JsonValue)`
- `verify(signature, message: JsonValue, id)`

In practice, many call sites already sign a string (e.g. the output of `secureHash(...)` is a `hash_z...` string), and request/auth payload construction remains outside crypto; the crypto API should not need to accept arbitrary JSON long-term.

### 3) Encryption / sealing and their nonce material encoding

`stableStringify` also determines bytes for:

- `encrypt(value, keySecret, nOnceMaterial)` / `decryptRaw(...)`
  - Both `value` and `nOnceMaterial` are stable-stringified before encryption/decryption.
  - `nOnceMaterial` appears in several places (e.g. key wrapping uses `{ encryptedID, encryptingID }`).
  - Determinism is required for `nOnceMaterial`, not for the encryption payload itself (it is not hashed for identity).

- `seal({ message, from, to, nOnceMaterial })` / `unseal(...)`
  - Both `message` and `nOnceMaterial` are stable-stringified before sealing/unsealing.
  - Example `nOnceMaterial`: `{ in: RawCoID, tx: TransactionID }`.

### 4) Session log / transactions (signature chaining)

The session log adapter passes stable-stringified JSON strings into the Rust core:

- `tryAdd(transactions)` currently maps `transactions.map(tx => stableStringify(tx))`
- `addNewTrustingTransaction(changes, meta)` stable-stringifies `changes` and `meta`
- `addNewPrivateTransaction(changes, meta)` stable-stringifies `changes` and `meta`

In the Rust core, the signature chain signs the JSON-stringified form of `"hash_z..."` (note the quotes), which is effectively the same as stable-stringifying a string.

### 5) Tooling/tests

There are additional `stableStringify` usages in:

- `packages/jazz-tools/src/tools/testing.ts` (constructing sealed/encrypted strings for tests)
- `packages/cojson/src/tests/*` (asserting deterministic hashing/signatures, and building test data)

These are in scope only insofar as they must be updated to follow the new canonical encoding rules.

## Goals

- Replace “hash/sign arbitrary JSON via `stableStringify`” with explicitly defined canonical encodings for a finite set of data shapes.
- Make canonical encoding Rust-owned (implemented in `cojson-core` and used by WASM/RN/NAPI), so JS does not need to replicate tricky canonicalization logic.
- Provide backward compatibility:
  - CoValue IDs derived from legacy header hashing continue to verify/load.
  - Existing signatures/tokens/sessions continue to verify during the transition.
- Maintain or improve performance vs `stableStringify`

## Non-goals

- Changing cryptographic primitives (still BLAKE3 and Ed25519).
- Rewriting the entire network protocol in one step.
- Supporting hashing/signing of fully arbitrary JSON forever (the goal is to enumerate and constrain).

## Open questions

- Do we require byte-for-byte compatibility with current `stableStringify` quirks for `CoValueHeader`, or do we first narrow/normalize the allowed header shapes until a Rust canonical encoding naturally matches the legacy outputs?
- Which sealed message shapes (if any) must remain “structured” (typed) vs treated as opaque bytes/strings at the crypto boundary?


