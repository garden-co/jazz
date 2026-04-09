# Local-First Auth Design

## Overview

Replace the anonymous/demo opaque-token auth path with self-signed Ed25519 JWTs derived from a local client seed.

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
- Remove `X-Jazz-Local-Mode` / `X-Jazz-Local-Token`
- Remove the existing linking / upgrade system entirely
- Keep backend impersonation unchanged
- Keep external JWT verification unchanged

## Hard Constraints

- Existing external JWT auth does not change: JWKS verification, claim-based principal resolution, and provider integration patterns stay as they are
- The self-signed path must not introduce any Jazz-side identity linking, account merge, or translation layer
- Self-signed JWTs must include `aud`, and the server must validate it
- If an app wants continuity between self-signed auth and BetterAuth auth, the BetterAuth user must be created with the existing self-signed `userId`
- WorkOS is out of scope for self-signed keys

## Non-Goals

- Multi-device seed sync
- Key hierarchy, delegated auth keys, or other non-KISS auth trees
- Full E2EE design in this spec
- Backwards compatibility with pre-launch anonymous principals
- Changing how external provider JWTs are verified or how they resolve to principals
- Retrofitting a newly generated self-signed key onto an existing provider-first user with a different ID

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

```ts
type SecretSeed = Uint8Array; // 32 bytes

type StoredIdentitySeed = {
  version: 1;
  seed: string; // base64url
};
```

This is the only long-lived client secret we persist/export.

### Domain-separated derivation

Today, the seed derives one signing keypair:

```ts
deriveSigningKeypair(seed, "jazz-auth-sign-v1");
```

Future encryption can derive separate key material from the same seed:

```ts
deriveEncryptionKeypair(seed, "jazz-e2ee-x25519-v1");
```

This keeps the auth model simple now while preserving a clean path to encryption later.

### Optional local seed

A client does not need a local seed to use Jazz.

Supported modes:

- local-first: client creates/loads a seed and authenticates with self-signed JWTs
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
  "exp": 1735689600
}
```

Server verification:

1. Decode JWT header and unverified claims
2. Read `alg` only from the untrusted header for early rejection, and require it to be exactly `EdDSA` from an allowlist of exactly `["EdDSA"]`
3. Require `iss = "urn:jazz:self-signed"`
4. Require `aud` and check that it matches the configured self-signed audience
5. Extract `jazz_pub_key`
6. Verify signature with that public key
7. Re-derive `userId` from the raw public key
8. Require `sub == userId`
9. Check `exp` and reject if expired or if TTL exceeds the server-enforced max (default: 1 hour)
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
- later BetterAuth sessions and JWTs then naturally resolve to the same Jazz principal

In other words, the BetterAuth-generated user ID is not a second ID. It must be the already-derived self-signed `userId`.

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
   - require the token to be unexpired and within the allowed TTL
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

### Replace local opaque auth

Delete:

- `resolveLocalAuthDefaults`
- `localAuthMode`
- `localAuthToken`
- `X-Jazz-Local-Mode`
- `X-Jazz-Local-Token`

Also delete any client-side helpers or state shaped around "linking" a local identity to a different BetterAuth identity.

### Local seed helpers

```ts
async function loadOrCreateIdentitySeed(
  appId: string,
  options?: {
    storage?: LocalAuthStorageLike;
  },
): Promise<StoredIdentitySeed>;

async function deriveIdentitySigningKeypair(
  stored: StoredIdentitySeed,
  options?: {
    crypto?: JazzCryptoProvider;
  },
): Promise<{ privateKey: CryptoKey; publicKey: CryptoKey }>;

async function deriveSelfSignedUserId(
  publicKey: CryptoKey,
  options?: {
    crypto?: JazzCryptoProvider;
  },
): Promise<string>;
```

### Self-signed token minting

```ts
async function mintSelfSignedToken(
  keypair: { privateKey: CryptoKey; publicKey: CryptoKey },
  options: {
    audience: string;
    crypto?: JazzCryptoProvider;
    ttlSeconds?: number;
  },
): Promise<string>;
```

This token is just another `jwtToken` value.

### Transport

`jwtToken` remains the single transport slot:

- external provider JWTs go there
- self-signed JWTs also go there

No extra auth transport headers are added for ordinary requests.

### BetterAuth account creation

When the user creates a BetterAuth account from a self-signed session, the client passes:

- the normal BetterAuth registration fields
- the current `userId`
- a self-signed proof token for that same identity

The BetterAuth server validates the proof token with the self-signed verifier, derives `provedUserId`, requires `provedUserId == currentUserId`, and then persists `user.id = provedUserId`.

After that, the client can switch to the BetterAuth-issued JWT because it carries the same principal.

### Provider-first mode

If the app supplies an external JWT and does not want local self-signed auth, the client does not have to generate a seed at startup.

If the app wants local-first mode, the browser default remains:

- generate/load seed
- derive signing key
- derive `userId`
- mint self-signed JWT with `aud`
- connect

---

## Server Config Changes

```rust
pub struct AppConfig {
    // existing fields ...
    pub allow_self_signed: bool, // default: true
    pub self_signed_audience: Option<String>, // required when allow_self_signed = true
    // removed: allow_anonymous, allow_demo
}
```

Rules:

- `allow_self_signed = false` disables the `urn:jazz:self-signed` path
- `allow_self_signed = true` requires `self_signed_audience` to be configured
- self-signed JWTs whose `aud` does not match are rejected
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
- user authenticates with a BetterAuth-issued JWT
- BetterAuth JWT resolves to the same principal and the user sees the same data
- BetterAuth account creation is rejected if the self-signed proof token is invalid
- BetterAuth account creation is rejected if `currentUserId` does not match the `userId` derived from the proof token

### No-linking invariants

- creating a BetterAuth user with a different `user.id` produces a different user, not a merge
- there is no server behavior that translates one user ID into another
- provider-first users with unrelated IDs are outside the self-signed continuity flow

---

## Summary

This design replaces the local opaque-token path with self-signed Ed25519 JWTs while keeping external provider auth unchanged.

- one canonical `userId` is derived from the self-signed key
- Jazz stores that value as `principal_id`
- BetterAuth must store that same value as `user.id`
- the old linking / upgrade system is removed entirely
- self-signed JWTs use `iss = "urn:jazz:self-signed"` and require `aud`
- WorkOS remains out of scope for self-signed keys

This keeps the local-first path simple, removes identity-mapping complexity, and avoids cross-app replay of self-signed tokens.
