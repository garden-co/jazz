# Local-First Auth Design

## Overview

Add self-signed Ed25519 JWT auth as the primary local-first identity path, alongside the existing `demoAuth`/`localMode` path (which remains unchanged and will be removed in a future phase).

For the local-first path there is exactly one canonical user identity:

```text
userId = UUIDv5(KEY_NAMESPACE, raw_ed25519_public_key_bytes)
```

Jazz stores this value as `principal_id`. If the app later creates a BetterAuth user for that same person, BetterAuth must persist that exact same value as `user.id`. There is no linking flow, no translation table, and no second canonical identifier to merge later.

Self-signed tokens use a Jazz-specific issuer URI, `iss = "urn:jazz:self-signed"`, and they require an `aud` claim so a token from one app cannot be replayed against another unrelated Jazz deployment.

WorkOS remains provider-first only and is out of scope for self-signed keys.

## Goals

- Preserve zero-friction local-first onboarding with no external auth provider required
- Support a single client secret seed now, with room to derive encryption keys later
- Keep the existing `demoAuth` / `localMode` / `X-Jazz-Local-*` path working unchanged (removal is a separate phase; see `todo/issues/deprecate-demo-local-auth.md`)
- Remove the existing linking / upgrade system entirely
- Keep backend impersonation unchanged
- Keep external JWT verification unchanged
- All cryptographic derivation and signing (key derivation, JWT minting/verification) live in Rust, exposed to TS via WASM/NAPI bindings for maximum portability. Seed generation (32 random bytes) is the exception — it uses the platform's native CSPRNG (`crypto.getRandomValues` on web, `expo-crypto` on RN, etc.) and does not round-trip through Rust

## Hard Constraints

- Existing external JWT auth does not change: JWKS verification, claim-based principal resolution, and provider integration patterns stay as they are
- The existing `demoAuth` / `localMode` path continues to work unchanged; self-signed auth is an additive path, not a replacement (yet)
- The self-signed path must not introduce any Jazz-side identity linking, account merge, or translation layer
- Self-signed JWTs must include `aud`, and the server must validate it
- If an app wants continuity between self-signed auth and BetterAuth auth, the BetterAuth user must be created with the existing self-signed `userId`
- WorkOS is out of scope for self-signed keys
- All crypto operations (Ed25519 key derivation, JWT signing, UUIDv5 derivation) are implemented in Rust and consumed by TS through the WASM/NAPI surface — TS never performs raw crypto directly

## Non-Goals

- Multi-device seed sync
- Key hierarchy, delegated auth keys, or other non-KISS auth trees
- Full E2EE design in this spec
- Backwards compatibility with pre-launch anonymous principals
- Changing how external provider JWTs are verified or how they resolve to principals
- Retrofitting a newly generated self-signed key onto an existing provider-first user with a different ID
- Removing or deprecating `demoAuth` / `localMode` / synthetic users (separate phase)

---

## Core Model

### Canonical user ID

Self-signed auth derives one stable ID from the signing public key:

```text
userId = UUIDv5(KEY_NAMESPACE, raw_ed25519_public_key_bytes)
```

- self-signed JWT `sub` = `userId`
- Jazz persists it as `principal_id`
- BetterAuth persists it as `user.id`

It is stable across sessions and devices if the seed is restored.

### Same ID everywhere

Jazz recognizes only one canonical ID for a local-first user.

- the self-signed JWT authenticates `userId`
- BetterAuth-created sessions for that user also authenticate `userId`
- Jazz never translates from a local ID to a different BetterAuth ID
- Jazz never merges two different IDs into one user

This is not "linked identities." It is one identity represented consistently across auth methods.

### Removed linking system

Delete the existing linking / upgrade model entirely:

- no linked-auth-method records
- no identity mapping table
- no post-hoc account merge flow
- no Jazz-side translation from self-signed IDs to BetterAuth IDs

If a BetterAuth user is created with a different `user.id`, that is a different user.

---

## Seed And Key Model

### Root seed

When the client wants local self-signed auth, it stores one 32-byte root seed.

```rust
// Rust-side types exposed through WASM/NAPI
pub struct SecretSeed([u8; 32]);

pub struct StoredIdentitySeed {
    pub version: u8, // always 1
    pub seed: String, // base64url-encoded
}
```

TS receives the seed as an opaque base64url string. All derivation happens in Rust.

This is the only long-lived client secret we persist/export.

### Domain-separated derivation (Rust)

Today, the seed derives one signing keypair:

```rust
fn derive_signing_keypair(seed: &SecretSeed, domain: &str) -> Ed25519Keypair;
// domain = "jazz-auth-sign-v1"
```

Future encryption can derive separate key material from the same seed:

```rust
fn derive_encryption_keypair(seed: &SecretSeed, domain: &str) -> X25519Keypair;
// domain = "jazz-e2ee-x25519-v1"
```

This keeps the auth model simple now while preserving a clean path to encryption later.

### Rust crypto surface for TS

Crypto operations are methods on the runtime, not standalone WASM exports. The runtime already owns the WASM module; adding identity crypto to it avoids a separate initialization path:

```rust
// Methods on the existing Runtime (exposed via WASM/NAPI)
impl Runtime {
    fn derive_user_id(&self, seed: &str) -> String;       // UUIDv5
    fn mint_self_signed_token(
        &self,
        seed: &str,
        audience: &str,
        ttl_seconds: u64,
    ) -> String;                                           // signed JWT
    fn get_public_key_base64url(&self, seed: &str) -> String;
}
```

Seed generation is not a runtime concern — a seed is just 32 random bytes, easily produced by any platform's crypto API.

TS calls these through the existing `JazzClient` / runtime binding surface. Seeds are plain base64url strings at the FFI boundary; the `StoredIdentitySeed` struct is Rust-internal.

### Optional local seed

A client does not need a local seed to use Jazz.

Supported modes:

- local-first (self-signed): client creates/loads a seed and authenticates with self-signed JWTs via the Rust crypto surface
- local-first (legacy): client uses `demoAuth` / `localMode` with opaque tokens (existing path, unchanged)
- provider-first: client authenticates only with external JWTs and does not need a local seed

Provider-first mode remains unchanged. This spec only defines continuity for flows that start from a self-signed `userId`.

---

## Authentication Flows

### JWT path selection

Jazz distinguishes the two JWT-backed auth methods by exact issuer:

- `iss = "urn:jazz:self-signed"` selects the self-signed verification path
- all other bearer JWTs go through the existing external JWKS verification path
- a token that claims `iss = "urn:jazz:self-signed"` but is missing required self-signed fields is rejected as an invalid self-signed token

Using a Jazz-specific issuer URI makes accidental collisions far less likely than a bare `iss = "self"` value and makes logs/errors clearer.

### Self-signed key auth

Client token:

```json
{
  "iss": "urn:jazz:self-signed",
  "sub": "<userId>",
  "aud": "<jazzAppId>",
  "jazz_pub_key": "<base64url(raw Ed25519 public key bytes)>",
  "iat": 1735686000,
  "exp": 1735689600
}
```

Server verification:

1. Decode JWT header and unverified claims
2. Read `alg` only from the untrusted header for early rejection, and require it to be exactly `EdDSA` from an allowlist of exactly `["EdDSA"]`
3. Require `iss = "urn:jazz:self-signed"`
4. Require `aud` and check that it matches the server's `appId`
5. Extract `jazz_pub_key`
6. Verify signature with that public key
7. Re-derive `userId` from the raw public key
8. Require `sub == userId`
9. Require `iat` and check `exp`: reject if expired, if `iat` is in the future, or if `exp - iat` exceeds the server-enforced max TTL (default: 1 hour)
10. Authenticate as `userId`

The verifier must be hardwired to Ed25519 for this path. It must not negotiate algorithms from the JWT header or delegate algorithm selection to a generic JWT library.

The required `aud` claim prevents replay across unrelated Jazz apps that accept self-signed JWTs.

### External JWT auth

Unchanged. External provider JWTs continue to be verified and resolved to principals exactly as they are today.

For local-first continuity, the only new invariant is that any BetterAuth-issued JWT for that user must resolve to the same `userId`.

### Backend impersonation

Unchanged:

- `X-Jazz-Backend-Secret`
- `X-Jazz-Session`

Backend impersonation sets the target session directly.

---

## BetterAuth Integration

### Local-first registration

When a self-signed user later creates a BetterAuth account, the app must create that BetterAuth user with `user.id = currentUserId`.

The important part is the persisted value, not the exact BetterAuth hook name:

- the app already knows `currentUserId` from the self-signed key
- the BetterAuth user-creation path must persist that exact value as `user.id`

In other words, the BetterAuth-generated user ID is not a second ID. It must be the already-derived self-signed `userId`.

### Server-side principal resolution for BetterAuth continuity

The existing external JWT principal resolution (`crates/jazz-tools/src/middleware/auth.rs`) resolves the principal in this order:

1. `jazz_principal_id` claim in the JWT → use directly
2. External identity mapping table → use mapped value
3. Issuer present → `derive_external_principal_id(app_id, iss, sub)` → `"external:<hash>"`
4. No issuer → raw `sub`

A BetterAuth JWT has an issuer (its JWKS URL), so without intervention the server would hit case 3 and derive an `"external:<hash>"` principal — **not** the self-signed `userId`. Continuity would silently break.

To preserve identity continuity, BetterAuth-issued JWTs for users that started as self-signed **must include the `jazz_principal_id` claim set to the self-signed `userId`**. This ensures the server hits case 1 and resolves to the correct principal.

The BetterAuth integration is responsible for including this claim when minting JWTs for users whose `user.id` was originally derived from a self-signed key. This is an app-level integration detail, but the requirement is non-negotiable: without `jazz_principal_id`, the user sees different data after signing up.

### Proof of possession

The BetterAuth user-creation path must not trust a client-supplied `currentUserId` by itself.

To create a BetterAuth account for an existing self-signed user, the request must include proof that the caller currently controls the signing key for that `userId`. The simplest allowed proof is the caller's current self-signed JWT.

Validation flow:

1. The client sends normal BetterAuth registration fields plus a self-signed proof token
2. The BetterAuth integration verifies that proof token with the same self-signed validation rules Jazz uses for normal request auth:
   - require `iss = "urn:jazz:self-signed"`
   - require `alg = "EdDSA"`
   - require valid `aud`
   - verify the Ed25519 signature using `jazz_pub_key`
   - re-derive `provedUserId` from the public key
   - require `sub == provedUserId`
   - require `iat` present and not in the future, `exp` not expired, and `exp - iat` within the allowed max TTL
3. The BetterAuth integration requires any claimed `currentUserId` in the request to equal `provedUserId`
4. The BetterAuth integration creates the BetterAuth user with `user.id = provedUserId`
5. If any check fails, registration is rejected

This means possession of the self-signed private key is what authorizes continuity into BetterAuth. There is no trust in an unsigned client assertion like "please create user `abc`."

How the app wires this into BetterAuth user creation is still an app-level integration detail. That can be a `generateId` hook, adapter logic, or another server-side creation path. The spec requires the same validation result regardless of which hook is used.

### Provider-first mode

If an app does not use self-signed auth, nothing changes. Provider JWTs work exactly as they do today.

This spec does not define taking an existing provider-first user with one ID and later attaching a brand new self-signed key that derives a different ID. Without linking, that retrofit flow does not exist.

### WorkOS

WorkOS is out of scope for self-signed key integration. WorkOS apps remain provider-first only.

---

## Client / SDK Changes

### Existing local auth: unchanged

The following remain intact and functional:

- `resolveLocalAuthDefaults`
- `localAuthMode` / `localAuthToken`
- `X-Jazz-Local-Mode` / `X-Jazz-Local-Token`
- Synthetic users (`SyntheticUserSwitcher`, `SyntheticUserStore`, etc.)

These will be deprecated and removed in a separate phase (see `todo/issues/deprecate-demo-local-auth.md`).

Delete any client-side helpers or state shaped around "linking" a local identity to a different BetterAuth identity (the linking system is removed regardless).

### Config surface

The app passes a seed or a `SeedStore` to `createDb` via the `auth` field:

```ts
interface DbConfig {
  // existing fields...
  auth?: { seed: string } | { seedStore: SeedStore };
}
```

- `auth.seed` — raw base64url seed string. The app manages the full seed lifecycle manually.
- `auth.seedStore` — a `SeedStore` instance. `createDb` calls `getOrCreateSeed()` at startup and proceeds as if a raw seed were passed.

When `auth` is not set, the existing auth resolution (`jwtToken`, `localAuthMode`/`localAuthToken`, `backendSecret`) works exactly as today.

### SeedStore

Jazz provides a `SeedStore` interface for platform-appropriate seed persistence:

```ts
interface SeedStore {
  loadSeed(): Promise<string | null>;
  saveSeed(seed: string): Promise<void>;
  clearSeed(): Promise<void>;
  getOrCreateSeed(): Promise<string>;
}
```

`getOrCreateSeed` loads an existing seed if one exists, otherwise generates and saves a new one. `generateSeed()` remains a standalone export for apps that want manual control.

#### Platform defaults

- **Web** (`@jazz-tools/jazz-browser`): `LocalStorageSeedStore` — stores the base64url seed string under a configurable `localStorage` key (default: `"jazz-seed"`). Uses a check-then-write pattern; not atomic across concurrent tabs on first visit. Apps that need strict cross-tab guarantees on first launch can use a custom `SeedStore` with IndexedDB transactions or `BroadcastChannel` coordination.
- **React Native** (`@jazz-tools/jazz-expo`): `ExpoSecureSeedStore` — wraps `expo-secure-store` for hardware-backed encrypted storage. Stored under a configurable key name (default: `"jazz-seed"`).
- **Custom**: Apps can implement `SeedStore` for any backend (IndexedDB, react-native-keychain, filesystem, etc.).

#### Seed lifecycle operations

- **Logout**: `seedStore.clearSeed()` + `db.shutdown()`
- **New identity**: `seedStore.clearSeed()`, then `seedStore.getOrCreateSeed()` produces a fresh seed, then re-create db
- **Export/backup**: `seedStore.loadSeed()` returns the raw base64url string for the app to display/copy
- **Import/restore**: `seedStore.saveSeed(importedSeed)` then re-create db

### App-level identity lifecycle

With a `SeedStore` (managed mode):

```ts
const seedStore = new LocalStorageSeedStore(); // or ExpoSecureSeedStore, custom, etc.
const db = await createDb({ appId: "my-app", auth: { seedStore } });

// Logout: clear seed, shutdown
await seedStore.clearSeed();
await db.shutdown();

// New identity: clear + re-create
await seedStore.clearSeed();
const db2 = await createDb({ appId: "my-app", auth: { seedStore } });
```

With a raw seed (manual mode):

```ts
const seed = localStorage.getItem("my-seed") ?? generateSeed();
localStorage.setItem("my-seed", seed);

const db = await createDb({ appId: "my-app", auth: { seed } });
```

Framework adapters (React/Vue/Svelte) use the platform-appropriate `SeedStore` by default when the app opts into self-signed auth. The app can override with a custom `SeedStore` instance or pass a raw seed.

### Self-signed auth flow inside createDb

When `auth` resolves to a seed (either directly via `auth.seed` or via `seedStore.getOrCreateSeed()`), `createDb` runs this pipeline using the runtime it already creates:

```text
1. runtime.deriveUserId(seed) → userId
   → set on Session for main-thread consumers (hooks, etc.)

2. runtime.mintSelfSignedToken(seed, appId, ttlSeconds) → jwtToken
   → passed as jwtToken through the existing config path

3. createDb continues with { ...config, jwtToken } — existing path
   → Worker receives jwtToken via init message — existing path
```

Since the crypto surface lives on the runtime, no additional WASM loading or initialization is needed — the runtime is already available at this point in the `createDb` lifecycle.

### Token refresh

Self-signed JWTs expire (server-enforced max: 1 hour). The main thread owns proactive refresh:

- After minting, schedule a refresh at ~80% of TTL (e.g. ~48 minutes for a 1-hour token)
- On refresh: call `runtime.mintSelfSignedToken` again with the seed from the original config
- Push the new token to the worker via the existing `update-auth` message
- Update the main-thread `SyncAuth.jwtToken` for any direct main-thread sync (memory-mode)

The refresh timer is cleaned up on `Db.shutdown()`.

Since minting is a local Rust WASM call (no network), refresh is effectively free and never fails unless the runtime is shutting down.

### Transport

`jwtToken` remains the single transport slot:

- external provider JWTs go there
- self-signed JWTs also go there

No extra auth transport headers are added for ordinary requests. The transport layer does not know or care whether the JWT is self-signed or externally issued.

### BetterAuth account creation

When the user creates a BetterAuth account from a self-signed session, the client passes:

- the normal BetterAuth registration fields
- the current `userId`
- a self-signed proof token for that same identity

The BetterAuth server validates the proof token with the self-signed verifier, derives `provedUserId`, requires `provedUserId == currentUserId`, and then persists `user.id = provedUserId`.

After that, the client can switch to the BetterAuth-issued JWT because it carries the same principal.

### Provider-first mode

If the app does not pass `auth`, the client does not generate a seed and no self-signed auth is used.

### Auth mode precedence

`createDb` resolves auth in this order:

1. `auth.seed` or `auth.seedStore` → self-signed pipeline (seed → Rust → JWT)
2. `jwtToken` → external JWT (existing path)
3. `backendSecret` → backend impersonation (existing path)
4. none of the above → `resolveLocalAuthDefaults` fallback (existing `localAuthMode`/`localAuthToken` path)

Setting both `auth` and `jwtToken` is a config error — `createDb` throws. Setting both `auth.seed` and `auth.seedStore` is also a config error.

---

## Server Config Changes

```rust
pub struct AppConfig {
    // existing fields unchanged ...
    pub allow_anonymous: bool,  // kept (deprecated in future phase)
    pub allow_demo: bool,       // kept (deprecated in future phase)
    // new:
    pub allow_self_signed: bool, // default: true
}
```

Rules:

- `allow_self_signed = false` disables the `urn:jazz:self-signed` path
- self-signed JWTs must have `aud` matching the server's `appId`; mismatches are rejected
- `allow_anonymous` and `allow_demo` continue to work as today; they will be deprecated and removed alongside the client-side local auth path
- external JWT verification and principal resolution remain unchanged
- there is no server-side linking configuration because linking no longer exists

---

## Test Plan

### Self-signed auth

- valid self-signed JWT authenticates as `userId`
- invalid signature is rejected
- expired token is rejected
- token with TTL exceeding the server max is rejected
- token with missing `aud` is rejected
- token with wrong `aud` is rejected
- token with `sub` not matching the re-derived `userId` is rejected
- token with `iss != "urn:jazz:self-signed"` falls through to the external auth path
- token claiming `iss = "urn:jazz:self-signed"` without the required self-signed shape is rejected as invalid self-signed auth
- `allow_self_signed = false` rejects self-signed JWTs
- same seed produces the same `userId` across sessions

### External auth

- existing external provider auth tests continue to pass without modification

### BetterAuth continuity

- user authenticates with self-signed JWT, creates data
- user creates a BetterAuth account with `user.id = currentUserId`
- user authenticates with a BetterAuth-issued JWT that includes `jazz_principal_id = userId`
- BetterAuth JWT resolves to the same principal and the user sees the same data
- BetterAuth JWT **without** `jazz_principal_id` resolves to a different principal (the `"external:<hash>"` path) — continuity does not silently work by accident
- BetterAuth account creation is rejected if the self-signed proof token is invalid
- BetterAuth account creation is rejected if `currentUserId` does not match the `userId` derived from the proof token

### TS adapter auth flow

- `auth: { seed }` with the same seed produces the same `userId` across sessions
- `createDb` with `auth: { seed }` produces a valid self-signed JWT that authenticates against the server
- `createDb` with `auth: { seedStore }` calls `getOrCreateSeed()` and produces a valid self-signed JWT
- token refresh mints a new token before expiry and pushes it to the worker via `update-auth`
- `Db.shutdown()` cancels the refresh timer
- different seeds produce different userIds and separate identities
- `auth: { seed }` does not interfere with `localAuthMode`/`localAuthToken` when both are absent
- setting both `auth` and `jwtToken` is a config error — `createDb` throws

### SeedStore

- `LocalStorageSeedStore.getOrCreateSeed()` generates and persists a seed on first call
- `LocalStorageSeedStore.getOrCreateSeed()` returns the same seed on subsequent calls
- `LocalStorageSeedStore.clearSeed()` removes the seed; next `getOrCreateSeed()` generates a new one
- `LocalStorageSeedStore.loadSeed()` returns `null` when no seed is stored
- `LocalStorageSeedStore` uses the configured key name
- `ExpoSecureSeedStore.getOrCreateSeed()` generates and persists a seed on first call
- `ExpoSecureSeedStore.clearSeed()` removes the seed from secure storage
- custom `SeedStore` implementation works with `createDb`

### No-linking invariants

- creating a BetterAuth user with a different `user.id` produces a different user, not a merge
- there is no server behavior that translates one user ID into another
- provider-first users with unrelated IDs are outside the self-signed continuity flow

---

## Summary

This design adds self-signed Ed25519 JWT auth as the new local-first identity path while keeping both external provider auth and the existing `demoAuth`/`localMode` path unchanged.

- one canonical `userId` is derived from the self-signed key
- Jazz stores that value as `principal_id`
- BetterAuth must store that same value as `user.id`
- the old linking / upgrade system is removed entirely
- self-signed JWTs use `iss = "urn:jazz:self-signed"` and require `aud`
- all crypto (key derivation, JWT minting) lives in Rust for portability
- TS adapters pass a seed via `auth: { seed }` on `DbConfig`; `createDb` handles key derivation, JWT minting, and proactive token refresh internally
- `SeedStore` provides platform-appropriate seed persistence: `LocalStorageSeedStore` for web, `ExpoSecureSeedStore` for React Native, or custom implementations
- `SeedStore.getOrCreateSeed()` is the primary entry point — loads an existing seed or generates + saves a new one
- main thread owns identity and pushes fresh tokens to worker via existing `update-auth`
- `demoAuth` / `localMode` / synthetic users remain functional and will be removed in a separate phase
- WorkOS remains out of scope for self-signed keys

This keeps the local-first path simple, removes identity-mapping complexity, and avoids cross-app replay of self-signed tokens.
