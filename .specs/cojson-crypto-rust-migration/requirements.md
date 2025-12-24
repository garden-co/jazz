## Introduction

`packages/cojson/src/crypto/crypto.ts` defines the TypeScript `CryptoProvider` abstraction and a set of value formats (e.g. `signerSecret_z…`, `signature_z…`, `encrypted_U…`, `sealed_U…`) used across the `cojson` package. Today, the platform providers (`WasmCrypto`, `NapiCrypto`, `RNCrypto`) already delegate most cryptographic primitives to Rust via `cojson-core-wasm`, `cojson-core-napi`, and `cojson-core-rn`, but the TypeScript layer still performs **canonicalization** via `stableStringify` for:

- Signing / verifying messages
- Encryption / decryption nonce material serialization
- Sealing / unsealing nonce material and message serialization
- Hashing helpers in `CryptoProvider` (`secureHash`, `shortHash`)

This feature migrates the remaining crypto-layer logic (including canonical serialization concerns) fully into Rust, and **eliminates `stableStringify` usage** from the crypto pipeline by using Rust-owned canonical serialization (via `serde`) over **predefined Rust data models** (typed structs/enums with deterministic ordering).

## User stories + acceptance criteria (EARS)

- **US1 — Single-source crypto implementation**
  - As a maintainer of `cojson`, I want all crypto-provider behavior defined by `packages/cojson/src/crypto/crypto.ts` to be implemented in Rust and exposed consistently across WASM, N-API, and React Native, so that platform providers cannot drift.
  - Acceptance criteria:
    - WHEN a consumer uses `WasmCrypto`, `NapiCrypto`, or `RNCrypto` for any supported operation THEN the underlying operation is executed by Rust code (no JS crypto primitives, and no JS canonicalization as part of the crypto operation).
    - WHEN a new crypto-related feature or fix is made THEN it is made once in Rust and reflected across all supported platforms via the bindings.

- **US8 — Rust-backed data structures (getters/setters)**
  - As a maintainer, I want crypto-related data structures to have a Rust-owned representation that JS can interact with via getters/setters, so parsing happens once and subsequent operations/mutations use Rust code paths.
  - Acceptance criteria:
    - WHEN a crypto-relevant structure is created/received in JS THEN there is a corresponding Rust-backed object (handle) that can be used for operations and updates.
    - WHEN reading fields THEN JS uses Rust-exposed getters (no re-parsing / re-serializing in JS for those reads).
    - WHEN updating fields THEN JS uses Rust-exposed setters/mutators, and serialization determinism remains Rust-controlled.
    - WHEN optimizing performance THEN a dual JS+Rust mirror is allowed, but the Rust structure is the source of truth and updates are applied through Rust.

- **US2 — Remove `stableStringify` from crypto**
  - As a developer, I want crypto operations to be deterministic without relying on `stableStringify` in TypeScript, so that signatures/seals/encryption behave identically across runtimes.
  - Acceptance criteria:
    - WHEN building `cojson` THEN there is no runtime dependency on `stableStringify` for signing/verifying/encrypting/decrypting/sealing/unsealing/hashing paths.
    - WHEN invoking sign/verify/encrypt/decrypt/seal/unseal via any provider THEN canonical serialization is performed in Rust using `serde` with a deterministic strategy (typed structs/enums with deterministic ordering, e.g. sorted-map representation for objects).
    - WHEN invoking crypto operations THEN there is no API surface whose contract is “accept arbitrary JSON text and stable-stringify it”; determinism is achieved by Rust-owned data modeling plus serialization.

- **US3 — Backwards compatibility for persisted data**
  - As a user of Jazz/`cojson`, I want existing signatures, sealed payloads, encrypted payloads, and hashes to remain valid, so upgrades do not break existing datasets.
  - Acceptance criteria:
    - WHEN verifying a signature produced by a previous version THEN verification succeeds under the new implementation (for the same logical message).
    - WHEN unsealing or decrypting payloads produced by a previous version THEN the plaintext result matches the previous behavior.
    - WHEN computing `secureHash` / `shortHash` on values that have been historically hashed THEN the resulting hash strings are unchanged (or a migration strategy is explicitly provided and documented in the design if unchanged hashes are not feasible).

- **US4 — Preserve the public TypeScript surface**
  - As a library consumer, I want the TypeScript API and value formats to stay stable, so I do not need to change application code.
  - Acceptance criteria:
    - WHEN upgrading THEN TypeScript types and exported symbols remain source-compatible (or any breaking change is explicitly called out in the design and constrained to the minimum necessary scope).
    - WHEN generating or parsing `SignerSecret`, `SignerID`, `SealerSecret`, `SealerID`, `Signature`, `Encrypted<…>`, and `Sealed<…>` THEN the string prefix formats remain identical.

- **US5 — Deterministic typed serialization for known message shapes**
  - As a maintainer, I want nonce-material and other known message shapes used by crypto operations to be modeled as explicit Rust types, so serialization order is guaranteed by construction.
  - Acceptance criteria:
    - WHEN a crypto operation uses structured inputs (e.g. nonce material such as `{ encryptedID, encryptingID }` and seal nonce material `{ in, tx }`) THEN Rust uses predefined `serde`-serializable structs to serialize those inputs deterministically.
    - WHEN a crypto operation is invoked THEN it always receives a **specific modeled data structure** (no “generic JSON value” / `CanonicalValue`-like type, and no “accept arbitrary JSON text” API).
    - WHEN a new crypto message shape is needed THEN a new Rust type is introduced for it and covered by test vectors.

- **US6 — Testable parity across platforms**
  - As a maintainer, I want test coverage that proves parity across WASM, N-API, and React Native, so regressions are caught early.
  - Acceptance criteria:
    - WHEN running the test suite THEN there are tests that validate identical outputs (or interoperable outputs where appropriate) for sign/verify, seal/unseal, encrypt/decrypt, and hash functions across all providers.
    - WHEN a serialization/canonicalization change is proposed THEN it is guarded by stable test vectors committed to the repo.

- **US7 — Don't touch random number generation**
  - As a maintainer, I want to avoid touching the random number generation code, this should be done in a next feature request.
  - Acceptance criteria:
    - WHEN generating random numbers THEN the random number generation code is not touched.
