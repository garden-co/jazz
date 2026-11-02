---
"jazz-tools": patch
---

**Breaking:** clients no longer sign in to Jazz with a JWT. Sign-in goes through the app's backend: `createJazz({ authenticate })` reads the app's own session, and the client calls `accounts.loginWithBackend()` (or `backendAuth()` / `betterAuth()` in the framework bindings). A local-first guest keeps its data by calling `accounts.linkWithBackend()` after signing in. The `jwtAuth` helpers are removed, and `registerJWT`, `loginJWT` and `loginOrRegisterJWT` are internal (test fixtures only) and no longer supported for apps. The sync server's own JWT flags (`--jwks-url`, `--jwt-*`) are unchanged.

### Migrating an app that signs in with a JWT

1. Add `createJazz` to your backend (`src/lib/jazz.server.ts`) with your app id, schema, permissions and store (`remote({ url, adminSecret, backendSecret })` to keep using Jazz Cloud or your own sync server), and an `authenticate(request)` that reads your auth library's session and returns `{ userId, claims }`.
2. Mount it at `/jazz` (Next.js: `app/jazz/[...path]/route.ts` exporting `GET`, `POST` and `DELETE` from `jazz.next()`; Hono and other fetch hosts: `jazz.fetch`; Node: `nodeHandler` + `attach`).
3. In the browser, set `serverUrl="/jazz"` and replace the JWT auth with `backendAuth()` (or `betterAuth(authClient)`). Remove the code that minted or fetched Jazz JWTs, and the JWKS setup for clients.
4. Keep `JAZZ_ADMIN_SECRET` and `BACKEND_SECRET` on the server only.
   **Not supported yet:** carrying over accounts of users who signed in with a JWT. Those accounts belong to the identity (your JWT's issuer, its `sub`), while `createJazz` names a user `jazz:app:<appId>` with your `userId`, so a returning JWT user gets a new account. Local-first guests are not affected.
5. A page served from another origin than the backend must be listed in `createJazz({ allowedOrigins })`.
