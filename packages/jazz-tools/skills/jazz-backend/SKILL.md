---
name: jazz-backend
description: Build and troubleshoot Jazz TypeScript backends, API routes, server rendering, jobs, webhooks, and self-hosted sync setups. Use for createJazzContext, jazz-napi, backend-scoped or request-scoped database access, caller permission evaluation, write attribution, server-side JWT verification, persistent server storage, backend/admin secrets, Next.js or SvelteKit server code, edge runtime Wasm setup, and Jazz server configuration.
---

# Jazz Backend

Build server-side features through `jazz-tools/backend` and the installed native or Wasm runtime.
Keep backend authority, requester authority, and write attribution explicit.

## Start from the runtime

1. Read the installed `jazz-tools` and `jazz-napi` versions and inspect their public types when an
   option is uncertain.
2. Locate `schema.ts`, `permissions.ts`, the existing backend context, environment variables, and
   process shutdown path.
3. Determine whether the code runs in Node.js, a framework server runtime, or an edge-style Wasm
   runtime.
4. Determine whether the backend is embedded/local-only or connected to a Jazz sync server.
5. Read the reference that matches the task:
   - [request-scoping.md](references/request-scoping.md) for context lifetime, backend access,
     requester permissions, sessions, and attribution.
   - [runtimes-and-hosting.md](references/runtimes-and-hosting.md) for NAPI drivers, self-hosted
     servers, secrets, framework servers, edge runtimes, and cleanup.

## Create one context with an explicit role

- Create `JazzContext` once at process or application-server startup and reuse it. Do not initialize
  a native runtime for every request.
- Use `context.asBackend()` for trusted server-owned work that bypasses row policies.
- Use `await context.forRequest(req)` when the caller's authenticated session must govern both row
  permissions and authorship.
- Use `context.forSession(session)` when trusted application code already resolved a Jazz session.
- Use `withAttribution*` when backend permissions must remain in force but writes should be stamped
  as a user.
- Use unscoped `context.db()` for embedded or local-only access. On a server-connected backend,
  choose `asBackend()` or a requester/session-scoped handle deliberately.

## Configure storage and sync deliberately

- Install `jazz-napi` as a direct dependency for Node.js backends.
- Give persistent Node runtimes a stable `dataPath` and shut the context down on process exit.
- A memory driver requires an upstream `serverUrl`; it cannot be the only durable copy.
- Server-connected backend authority and scoped handles (`asBackend`, `forRequest`, `forSession`,
  and `withAttribution*`) require `backendSecret`. Local-only `context.db()` access does not.
- Keep `BACKEND_SECRET` and `JAZZ_ADMIN_SECRET` server-only. Do not place them in public-prefixed
  framework environment variables.
- Await the required write tier before returning success from endpoints that promise server-visible
  or globally visible persistence.

## Resolve requests and auth safely

- `forRequest` accepts standard request header shapes from Fetch, Express, Hono, and similar
  frameworks. It expects bearer auth; it does not parse arbitrary application cookies.
- Configure `jwksUrl` or `jwtPublicKey`, never both, when external JWTs reach `forRequest`.
- Resolve cookie sessions through the application's auth layer, then call `forSession(session)`.
- Set `allowLocalFirstAuth: false` when a backend endpoint must reject Jazz self-signed local-first
  tokens.

## Cross into adjacent work deliberately

- Load `jazz-auth` for provider setup, identity upgrades, token refresh, cookie lifecycle, or local-
  first recovery.
- Load `jazz-schema-evolution` for structural schema, permissions, migrations, or catalogue
  deployment.
- Load `jazz-files` for chunked file/blob storage.
- Load `jazz-testing` only when the requested work includes backend or server test code.

## Verify the change

1. Exercise backend-owned and caller-scoped paths as distinct identities.
2. Confirm authorship metadata matches the chosen scoping or attribution helper.
3. Confirm secrets remain outside browser bundles and logs.
4. Confirm persistent paths, flush/shutdown behavior, and process signal handling.
5. Test the real server topology when JWT validation, permissions, sync, or runtime loading is the
   behavior under test.

## Avoid these failure modes

- Do not create a new `JazzContext` per request.
- Do not use `asBackend()` merely to make a permission failure disappear.
- Do not confuse attribution with impersonation: `withAttribution*` does not apply user policies.
- Do not expect `forRequest` to resolve an application cookie.
- Do not use a memory-only backend without an upstream durable server.
- Do not publish schemas or permissions as an incidental backend startup check.
- Do not leave native clients or local servers running after their owner shuts down.
