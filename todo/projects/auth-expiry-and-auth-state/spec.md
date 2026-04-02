# Auth Expiry and Auth State

## Problem

Jazz's external auth story currently assumes a JWT passed as `Authorization: Bearer <jwt>`. That works for the happy path, but the developer experience falls apart once the token expires or the server starts returning unauthenticated responses. `/sync` failures surface as generic `401` errors, `/events` reconnects silently loop as if the network were flaky, and React/framework surfaces expose only a one-time decoded `session` snapshot.

For a real-world app, expiration should feel deliberate: Jazz should clearly transition into an unauthenticated state, pause authenticated sync, preserve local work, and give the host app an explicit place to renew auth.

## Appetite

small

## Solution

### Summary

Treat external auth as a JWT-driven client state:

1. the JWT authenticates `/sync` and `/events`
2. Jazz derives `Session` from that JWT for `useSession()`, user-scoped queries, and write authorship
3. when auth is lost, Jazz keeps the last derived session locally until the app updates auth or explicitly clears it

On any `401` from `/sync` or `/events`, Jazz should stop treating the failure as a generic transport wobble. Instead it should:

1. classify the failure as an auth loss
2. transition the client to `unauthenticated`
3. stop authenticated reconnect loops
4. preserve local state and pending outbox entries
5. notify the app through a typed auth-state surface
6. wait for the app to call `updateAuthToken(...)`

Jazz does **not** refresh tokens itself in this spec. Renewal stays app-owned.

### Breadboards

#### 1) Bearer JWT expires during normal sync

```text
alice opens app
  -> Jazz starts authenticated with jwtToken
  -> /events connected
  -> local writes sync normally

JWT expires
  -> next /sync POST or /events reconnect gets 401 { error: "unauthenticated", code: "expired" }
  -> Jazz sets auth state = unauthenticated(reason=expired)
  -> Jazz detaches server + pauses reconnect
  -> local DB keeps working; pending outbox stays queued

app sees unauthenticated state
  -> app runs its own refresh/login flow
  -> app gets fresh JWT
  -> app calls db.updateAuthToken(jwtToken)

Jazz resumes
  -> recomputes session from the new JWT
  -> reconnects /events with same client_id
  -> flushes queued sync payloads
```

#### 2) App reacts to auth loss

```text
alice opens app
  -> Jazz uses jwtToken for /events and /sync
  -> Jazz decodes Session locally

JWT expires or becomes invalid server-side
  -> next authenticated request gets 401 { error: "unauthenticated", code: "expired" | "missing" | "invalid" }
  -> Jazz sets auth state = unauthenticated
  -> useSession() keeps the last known Session
  -> authenticated sync pauses

app sees unauthenticated state
  -> app redirects to sign-in or silently renews with its own auth SDK
  -> app gets a fresh JWT
  -> app calls db.updateAuthToken(jwtToken)

Jazz resumes with the new credential
```

#### 3) How app code reacts

The app should have one obvious place to listen and one obvious place to recover.

```ts
const stop = db.onAuthChanged((state) => {
  if (state.status === "unauthenticated") {
    authUi.promptSignIn({ reason: state.reason });
  }
});

const next = await authSdk.refreshOrSignIn();

db.updateAuthToken(next.jwtToken);
```

### Fat Marker Sketch

```text
                 host app / auth SDK
                        |
                        | app-owned renewal
                        v
      +---------------------------------------------+
      | Jazz client                                  |
      |                                             |
      |  transport auth  -----> /sync + /events     |
      |  derived session ---> useSession(), writes  |
      |  auth state -------> app callback / hook    |
      +---------------------------------------------+
                        |
                        | 401 { unauthenticated, code }
                        v
      +---------------------------------------------+
      | Jazz sync server                             |
      | backend secret                               |
      |   > Authorization bearer                     |
      |   > local auth                               |
      |   > no session                               |
      +---------------------------------------------+
```

### Server Behavior

Resolution order becomes:

1. backend impersonation
2. `Authorization: Bearer <jwt>`
3. local auth headers
4. no session

#### Typed unauthenticated responses

All auth-protected endpoints used by the runtime should return a structured `401` body:

```ts
type UnauthenticatedCode = "expired" | "missing" | "invalid" | "disabled";

type UnauthenticatedResponse = {
  error: "unauthenticated";
  code: UnauthenticatedCode;
  message: string;
};
```

This must be consistent for:

- `POST /sync`
- `GET /events?client_id=...`
- app-scoped cloud equivalents

`expired` is used when JWT validation fails specifically because the token is past `exp`. `missing` is used when no acceptable credential is present. `invalid` is used for malformed or rejected credentials. `disabled` is used when the auth method exists but is not enabled for the target app.

### Client Runtime Behavior

#### New auth state surface

Add a typed auth state that sits next to the existing transport lifecycle:

```ts
type AuthFailureReason = "expired" | "missing" | "invalid" | "disabled";

type AuthState =
  | {
      status: "authenticated";
      transport: "bearer" | "local" | "backend";
      session: Session | null;
    }
  | {
      status: "unauthenticated";
      reason: AuthFailureReason;
      session: Session | null;
    };
```

Core surface:

```ts
interface DbConfig {
  jwtToken?: string;
}

interface Db {
  updateAuthToken(jwtToken?: string): void;
  getAuthState(): AuthState;
  onAuthChanged(listener: (state: AuthState) => void): () => void;
}
```

`useAuthState()` in framework adapters can be a thin wrapper around `onAuthChanged(...)`. `useSession()` should expose the current derived session and preserve the last known value while auth is unauthenticated.

#### Session derivation rules

Session resolution becomes:

- local auth: derive session as today
- external JWT auth:
  - decode the JWT payload as today
  - cache the derived session in client state
  - keep that cached session during auth loss until a new JWT is applied or auth is explicitly cleared

This keeps the API narrow while preserving authorship and user-scoped local UX during renewal.

#### What happens on 401

When `/sync` or `/events` gets a structured `401`:

1. mark auth state as `unauthenticated`
2. abort the active stream
3. detach the server from the runtime
4. stop scheduling reconnect attempts until `updateAuthToken(...)`
5. preserve the current derived session for local reads/writes
6. keep local storage, local subscriptions, and pending sync payloads intact

Repeated `401`s while already unauthenticated should be deduplicated so the app does not get spammed with the same event.

#### Resuming after renewal

`updateAuthToken(...)` should:

1. update the main-thread JWT
2. propagate the new auth to the worker via `update-auth`
3. recompute the derived session from the new JWT
4. transition auth state back to `authenticated`
5. reconnect `/events`
6. flush queued sync work

`updateAuthToken(...)` supports:

- `undefined -> principal` initial sign-in
- `principal -> same principal` token renewal
- `principal -> undefined` explicit sign-out

It does **not** support hot-swapping between two different non-null principals on one live client. If `user_id` changes from `alice` to `bob`, the app should recreate the client.

This allows the common renewal path to keep local authorship stable while the app renews auth, without widening the API beyond the JWT itself.

#### Provider behavior

`JazzProvider` should stop treating auth refresh as a full client recreation event. For a stable app/schema/server tuple, changes limited to `jwtToken` should call `updateAuthToken(...)` on the existing client rather than tearing down the DB and worker.

### Rabbit Holes

- **Do not conflate auth validity and local session.** Even in the bearer-only model, an expired token does not mean Jazz should immediately forget who the local user was.
- **Do not treat auth loss as a network error.** Silent reconnect loops on `/events` hide the real problem and make expiration impossible to handle cleanly.
- **Do not allow principal hot-swap on a live client.** Replaying queued outbox data from one principal after switching to another is a correctness bug, not just a DX issue.
- **Do not smuggle renewal into Jazz.** Hidden refresh calls would couple Jazz to app-specific auth SDKs and make failures much harder to reason about.
- **Do not over-spec future transports.** This phase is about auth expiry and app-owned renewal, not about choosing every future credential transport.

### No-gos

- No Jazz-managed refresh token exchange or built-in refresh endpoint client
- No proactive client timers based on JWT `exp`; auth loss is response-driven in this MVP
- No multi-principal hot-swap inside one live `Db`/`JazzProvider`
- No redesign of permissions or session semantics beyond keeping the last derived session through auth loss
- No cookie transport design in this spec

### Testing Strategy

Prefer integration-first coverage using the existing server/runtime/browser stacks.

- Server auth integration:
  - bearer JWT in `Authorization` still authenticates normally
  - expired bearer returns `401` with `code: "expired"`
  - missing bearer returns `401` with `code: "missing"` when the endpoint requires auth
  - malformed or rejected bearer returns `401` with `code: "invalid"`
- Browser worker integration:
  - alice starts authenticated, token expires, `/events` returns `401`, and the worker stops reconnecting until auth is updated
  - alice makes local writes while unauthenticated; they remain queued and flush after `updateAuthToken(...)`
- Framework adapter integration:
  - `useSession()` remains on alice's last derived session during auth loss and changes when a new JWT is applied or auth is cleared
  - `useAuthState()` or equivalent listener surfaces one `unauthenticated` transition per auth-loss episode
  - updating auth in `JazzProvider` does not recreate the underlying DB client for same-principal renewal
- Guardrail tests:
  - `updateAuthToken(...)` rejects `alice -> bob` principal changes on a live client
  - local anonymous/demo auth behavior remains unchanged when cookie auth is not configured
