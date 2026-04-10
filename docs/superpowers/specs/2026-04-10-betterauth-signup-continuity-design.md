# Better Auth Signup Continuity Demo Design

## Overview

Add a minimal continuity-preserving sign-up path to `examples/auth-betterauth-chat`.

The demo starts from an existing local-first Jazz session backed by a self-signed identity. When the user signs up with the existing Better Auth email/password form, the created Better Auth user must reuse that existing Jazz `userId` instead of generating a new Better Auth-specific id.

This design intentionally covers only new sign-ups from an existing self-signed Jazz session. It does not add login continuity, account linking, migration for preexisting Better Auth users, or any generic token-export API.

## Goals

- Preserve the existing Jazz identity for new Better Auth sign-ups
- Keep the current email/password UI unchanged
- Make continuity fields invisible to the user
- Require proof of possession of the current self-signed Jazz identity before sign-up succeeds
- Keep the example small and easy to explain

## Hard Constraints

- The continuity source of truth is the current self-signed Jazz identity already loaded in the browser
- Sign-up must fail if the client cannot prove that self-signed identity
- Better Auth must persist the proved Jazz `userId` as `user.id`
- The runtime API for proof minting is explicit: `db.getSelfSignedToken({ ttlSeconds?: number }): Promise<string | null>`
- `db.getSelfSignedToken(...)` returns `null` for non-self-signed sessions
- The visible sign-up form does not change

## Non-Goals

- Supporting continuity for login or session upgrade after sign-up
- Supporting provider-first or already-authenticated external sessions
- Adding a generic `db.getJWT()` API
- Adding request-aware `generateId` plumbing
- Preserving identity for existing Better Auth rows created before this flow

---

## Runtime API

The runtime exposes one continuity-proof API:

```ts
db.getSelfSignedToken({ ttlSeconds?: number }): Promise<string | null>
```

Behavior:

- returns a freshly minted self-signed JWT when the current Jazz session is self-signed
- returns `null` when the current session is not self-signed
- throws for real runtime failures such as initialization problems or signing errors

The token subject is the current Jazz `userId`. The TTL is short-lived and intended for one sign-up attempt, with the example client requesting roughly 60 seconds.

This API is intentionally narrow. It proves possession of the local Jazz identity without exposing a generic "current auth token" abstraction.

## Client Flow

The Better Auth example keeps the current sign-up UI and changes only the submit path.

On sign-up submit:

1. Read the current Jazz identity from the existing client/runtime state.
2. Call `db.getSelfSignedToken({ ttlSeconds: 60 })`.
3. If the result is `null`, fail locally and do not call Better Auth sign-up.
4. Call `authClient.signUp.email(...)` with the existing visible fields plus:
   - `currentUserId`
   - `proofToken`
5. Continue with the existing post-sign-up authenticated flow.

The extra fields are added programmatically and are never shown in the UI.

## Better Auth Server Flow

The Better Auth example keeps the stock sign-up endpoint and adds continuity logic around it.

### Before-hook validation

A Better Auth `hooks.before` handler on `/sign-up/email` reads `currentUserId` and `proofToken` from the request body and validates the proof token server-side.

Validation rules:

- the proof token must parse as a self-signed Jazz JWT
- the proof token must be validly signed
- the proof token must not be expired
- the proof token audience must match the current Jazz app / server configuration
- the proof token must represent the self-signed issuer/auth mode expected by the Jazz verifier
- the proof token subject must equal `currentUserId`

If any check fails, the request is rejected and no Better Auth user is created.

After successful validation, the handler stores the proved Jazz `userId` in request-local state for the rest of the sign-up request.

### User create override

`databaseHooks.user.create.before` reads the proved Jazz `userId` from request-local state and overwrites the pending Better Auth user payload:

```ts
user.id = provedUserId;
```

This is the only persistence mutation introduced by the demo. Better Auth still owns the rest of the sign-up flow.

## Identity Outcome

After sign-up succeeds:

- the Better Auth user row has `user.id = <existing Jazz userId>`
- the Better Auth JWT subject continues to resolve to that same id through the existing example setup
- the authenticated Jazz session therefore resolves to the same principal the browser had before sign-up

This demo achieves the continuity requirement without introducing any Jazz-side linking or id translation layer.

## Failure Behavior

The flow is intentionally strict.

Client-side failure:

- if `db.getSelfSignedToken(...)` returns `null`, the client blocks sign-up and surfaces a simple continuity-required error

Server-side failure:

- if the request omits `currentUserId` or `proofToken`, reject sign-up
- if token verification fails, reject sign-up
- if the proved subject does not match `currentUserId`, reject sign-up
- if request-local proved identity is missing by the time persistence runs, reject sign-up rather than falling back to a generated Better Auth id

There is no fallback path that silently creates a new Better Auth-specific identity.

## Testing

Keep tests integration-shaped and focused on externally visible behavior.

### Runtime test

Add a runtime test for `db.getSelfSignedToken(...)` that verifies:

- a self-signed session returns a token
- a non-self-signed session returns `null`

### Example continuity test

Add an auth example test that:

- starts from an existing self-signed Jazz identity
- performs Better Auth sign-up through the normal UI flow
- verifies the created Better Auth user id matches the preexisting Jazz `userId`

### Example rejection test

Add a sign-up rejection test that verifies the example does not create a user when the proof token is missing or invalid.

## Implementation Notes

- This design deliberately uses `databaseHooks.user.create.before` instead of Better Auth `generateId`
- That is a conscious simplification for the demo: request-aware proof validation fits naturally in hooks, while `generateId` is not request-aware in the documented Better Auth API
- The demo teaches the continuity invariant, not Better Auth internals
