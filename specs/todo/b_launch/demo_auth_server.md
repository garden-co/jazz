# Demo Auth Server — TODO

Replace client-side synthetic users with a local [Better Auth](https://better-auth.com) instance that issues real JWTs and exposes a JWKS endpoint. Development apps exercise the same auth flows as production from day one.

Related specs: [`unified_auth_methods.md`](unified_auth_methods.md) (session resolution, principal model), [`auth_integrations.md`](auth_integrations.md) (production Better Auth / WorkOS integration).

## Why

Today's "demo auth" is entirely client-side — tokens are generated in the browser, hashed deterministically, and sent as custom headers (`X-Jazz-Local-Mode` / `X-Jazz-Local-Token`). This means:

- The `Authorization: Bearer` → JWKS validation path is never exercised until a developer plugs in a real auth provider.
- Switching to production auth swaps the entire auth mechanism, not just a config value.
- The client-side user switcher component is dev-only UI that has no production equivalent.

A local auth server that issues real JWTs closes this gap. Switching to production auth becomes "change the issuer URL and JWKS endpoint."

## Goals

- One-command local development environment with real JWT auth flows
- Full redirect-based auth flow, even in development — no dev-only client-side components
- Same library (Better Auth) in dev and prod — evolve config, don't replace systems
- User switching via the auth server's picker UI, not a client-side widget
- Smooth upgrade to production: with Matteo's Better Auth adapter, demo users live in Jazz — no migration, just add real credential methods

## Non-Goals

- Cloud integration (Jazz Cloud trusting a localhost auth server) — parked for v1, local only
- Full Better Auth plugin surface in v1 — start minimal, people can add plugins as needed
- Replacing client-only auth — it remains as a fallback for offline / serverless / CI use cases

## Terminology

| Term                 | Meaning                                                                                                                       |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| **Demo auth**        | The new default: local Better Auth instance with anonymous plugin + user picker UI. Takes over the existing "demo auth" name. |
| **Client-only auth** | Renamed from "synthetic users." Client-generated tokens, no server needed. Fallback for offline prototyping and CI.           |

The existing `LocalAuthMode` type (`"anonymous"` | `"demo"`) and associated headers (`X-Jazz-Local-Mode`, `X-Jazz-Local-Token`) remain for client-only auth. Demo auth uses standard `Authorization: Bearer` headers.

## Architecture

```
┌──────────────────────────────────────────────┐
│  npx jazz-tools dev                          │
│  ├── Jazz server        (port 4200)          │
│  └── Demo auth server   (port 4201)          │
│      ├── Better Auth (anonymous plugin)      │
│      ├── JWKS endpoint  (/.well-known/jwks)  │
│      └── User picker UI (/pick)              │
└──────────────────────────────────────────────┘

Client ──login──▶ Demo auth (/pick)
                  │ select user
                  ▼
Client ◀──redirect── JWT issued
  │
  │ Authorization: Bearer <jwt>
  ▼
Jazz server ──validates──▶ Demo auth JWKS
```

## Developer Experience

### Starting

`npx jazz-tools dev` starts both the Jazz server and the demo auth server in one process. `npx jazz-tools server` remains production-only — no demo auth, no dev conveniences.

On startup, the dev command prints:

```
Jazz dev server:  http://localhost:4200
Demo auth:        http://localhost:4201
```

### Client Configuration

The client config accepts an `authServer` URL. If omitted, defaults to Jazz server port + 1 for zero-config dev.

```typescript
const db = createDb({
  appId: "my-app",
  server: "http://localhost:4200",
  authServer: "http://localhost:4201", // default: inferred from server port
});
```

In production, `authServer` points to the real auth provider:

```typescript
const db = createDb({
  appId: "my-app",
  server: "https://api.myapp.com",
  authServer: "https://auth.myapp.com",
});
```

### Auth Flow

1. Client detects no valid session
2. Redirects to `{authServer}/pick` — the demo auth server's user picker
3. User clicks a name (Alice, Bob, Carol, or creates a new user)
4. Demo auth issues a JWT, redirects back to the app
5. Client stores the JWT, connects to Jazz server with `Authorization: Bearer`

No client-side auth components needed. The app's login/logout flow is identical in dev and prod.

### User Switching

Switching users means redirecting back to `/pick`. Same flow as initial login — no special mechanism. The picker page shows all existing demo users; clicking one issues a new JWT and redirects back.

## Demo Auth Server Internals

### Better Auth Configuration (v1)

- **Anonymous plugin** — creates accounts without credentials
- **JWT plugin** — issues JWTs with JWKS endpoint
- **Storage** — local SQLite (Better Auth default), later replaced by Jazz via Matteo's Better Auth adapter

### User Picker UI

A simple server-rendered page listing available demo users:

- Ships with sensible defaults: Alice, Bob, Carol
- Supports creating new named users on the fly
- Persists users across server restarts (SQLite)
- No credentials required — click a name, get a JWT

### JWT Claims

```json
{
  "sub": "demo-user-alice-<id>",
  "iss": "http://localhost:4201",
  "iat": 1735689600,
  "exp": 1735776000
}
```

Standard claims. The Jazz server validates via JWKS — no special handling for demo vs. production JWTs.

### JWKS Trust

The Jazz server started by `jazz-tools dev` is automatically configured with the demo auth server's JWKS URL. No manual `--jwks-url` flag needed in dev mode.

## Upgrade Path: Dev to Production

Since demo auth _is_ Better Auth, the upgrade is config evolution, not system replacement:

1. **Add credential methods** — enable email/password, OAuth, etc. in Better Auth config
2. **Point at production database** — swap SQLite for Postgres (or Jazz, via Matteo's adapter)
3. **Update client config** — change `authServer` to the production URL
4. **Disable anonymous plugin** — require real credentials

With Matteo's Better Auth adapter storing users in Jazz, demo users created during development are already in the database. Adding credential methods to existing users makes them "real" — no migration, no data loss.

## Relationship to Client-Only Auth

Client-only auth (formerly "synthetic users") remains for:

- **Offline prototyping** — no server needed at all
- **CI / automated tests** — deterministic, no network
- **Serverless demos** — pure local-first without infrastructure

It continues to use `X-Jazz-Local-Mode` / `X-Jazz-Local-Token` headers and client-side principal derivation as specified in [`unified_auth_methods.md`](unified_auth_methods.md). The `<SyntheticUserSwitcher />` component is renamed but otherwise unchanged.

Demo auth is the recommended default. Client-only auth is documented as a fallback.

## Open Questions

- **Cloud trust:** How does Jazz Cloud validate JWTs from a localhost demo auth server? Options include key registration at startup, hosted demo auth, or tunnelling. Parked for v1.
- **Principal continuity across auth modes:** If a developer prototypes with client-only auth then switches to demo auth, is there a migration path for existing data? Or is this a clean break?
- **Scope of picker UI:** Should the picker show metadata (principal ID, claims) for debugging, or stay minimal?
- **Port collision:** What happens if port 4201 is taken? Auto-increment, or fail with a clear message?

## Test Plan

- `jazz-tools dev` starts both servers; Jazz server is pre-configured with demo auth JWKS
- Full redirect flow: unauthenticated client → picker → JWT → authenticated session
- User switching: redirect to picker, select different user, new JWT, different session
- JWT validation: Jazz server accepts demo auth JWTs, rejects invalid/expired tokens
- User persistence: created demo users survive server restart
- Client config: omitting `authServer` defaults to server port + 1
- Production config: explicit `authServer` URL overrides the default
- Client-only auth continues to work independently (no demo auth server required)
