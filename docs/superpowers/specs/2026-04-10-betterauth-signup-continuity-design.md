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
- The runtime API for proof minting is explicit: `db.getSelfSignedToken({ ttlSeconds?: number, audience?: string }): Promise<string | null>`
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
db.getSelfSignedToken({ ttlSeconds?: number, audience?: string }): Promise<string | null>
```

Behavior:

- returns a freshly minted self-signed JWT when the current Jazz session is self-signed
- returns `null` when the current session is not self-signed
- throws for real runtime failures such as initialization problems or signing errors
- when `audience` is provided, sets the JWT `aud` claim to that value

The token subject is the current Jazz `userId`. The TTL is short-lived and intended for one sign-up attempt, with the example client requesting roughly 60 seconds. The `audience` field lets the client specify who the token is intended for — the server then validates that the token's `aud` matches its own expected value, preventing tokens minted for one service from being accepted by another.

This API is intentionally narrow. It proves possession of the local Jazz identity without exposing a generic "current auth token" abstraction.

## Client Flow

The Better Auth example keeps the current sign-up UI and changes only the submit path.

On sign-up submit:

1. Read the current Jazz identity from the existing client/runtime state.
2. Call `db.getSelfSignedToken({ ttlSeconds: 60, audience: "betterauth-signup" })`.
3. If the result is `null`, fail locally and do not call Better Auth sign-up.
4. Call `authClient.signUp.email(...)` with the existing visible fields plus:
   - `proofToken`
5. Continue with the existing post-sign-up authenticated flow.

The extra fields are added programmatically and are never shown in the UI.

## Better Auth Server Flow

The Better Auth example keeps the stock sign-up endpoint and adds continuity logic around it.

### Before-hook validation

A Better Auth `hooks.before` handler on `/sign-up/email` reads `proofToken` from the request body and validates the proof token server-side.

Validation rules:

- the proof token must parse as a self-signed Jazz JWT
- the proof token must be validly signed
- the proof token must not be expired
- the proof token `aud` claim must equal the expected audience string (e.g. `"betterauth-signup"`)
- the proof token must represent the self-signed issuer/auth mode expected by the Jazz verifier

The proved Jazz `userId` is extracted from the validated token's subject claim — the client never sends it separately, eliminating any mismatch surface.

If any check fails, the request is rejected and no Better Auth user is created.

After successful validation, the handler injects the proved Jazz `userId` into the request body by returning a modified context:

```ts
return {
  context: {
    ...ctx,
    body: { ...ctx.body, provedUserId },
  },
};
```

This uses Better Auth's built-in hook mechanism for passing data downstream — no `AsyncLocalStorage`, `WeakMap`, or mutation needed.

### User create override

`databaseHooks.user.create.before` reads `provedUserId` from the body (which Better Auth passes through as extra fields) and overwrites the user id:

```ts
user: {
  create: {
    before: async (user, ctx) => {
      const provedUserId = ctx.context.body?.provedUserId;
      if (!provedUserId) {
        throw new APIError("BAD_REQUEST", {
          message: "Missing proved identity — refusing to create user",
        });
      }
      return { data: { ...user, id: provedUserId } };
    },
  },
}
```

If `provedUserId` is missing (sign-up reached persistence without passing through proof validation), the hook throws rather than falling back to a generated id. This is the only persistence mutation introduced by the demo. Better Auth still owns the rest of the sign-up flow.

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

- if the request omits `proofToken`, reject sign-up
- if token verification fails, reject sign-up
- if `provedUserId` is missing from the body by the time `databaseHooks.user.create.before` runs, throw rather than falling back to a generated Better Auth id

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

- This design deliberately uses `databaseHooks.user.create.before` instead of Better Auth `generateId` — `generateId` only receives the model name, not request context, so it cannot validate proof tokens
- Data flows from the before-hook to the database hook via Better Auth's built-in context-return mechanism (`return { context: { ...ctx, body: { ...ctx.body, provedUserId } } }`) — no external state management needed
- The demo teaches the continuity invariant, not Better Auth internals
