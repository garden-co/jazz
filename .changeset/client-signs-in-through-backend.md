---
"jazz-tools": patch
---

**Breaking:** clients no longer sign in to Jazz with a JWT. Sign-in goes through the app's backend: `createJazz({ authenticate })` reads the app's own session, and the framework bindings follow the user it names (call `reauthenticate()` after your own sign-in or sign-out). A local-first guest keeps its data by calling `accounts.linkWithBackend()` after signing in. The `jwtAuth` helpers are removed, and `registerJWT`, `loginJWT` and `loginOrRegisterJWT` are internal (test fixtures only) and no longer supported for apps. The sync server verifies no user JWTs either (see the core-admits-only-its-backend changeset).

### Migrating an app that signs in with a JWT

1. Add `createJazz` to your backend (`src/lib/jazz.server.ts`) with your schema, permissions and store (`remote({ url, appId, secret })` with your app id and its admin secret to keep using Jazz Cloud or your own sync server), and an `authenticate(request)` that reads your auth library's session and returns `{ userId, claims }`.
2. Mount it at `/jazz` (Next.js: `app/jazz/[...path]/route.ts` exporting `GET`, `POST` and `DELETE` from `jazz.next()`; Hono and other fetch hosts: `jazz.handle`; Node: `nodeHandler` + `attach`).
3. In the browser, set `serverUrl="/jazz"`, remove the JWT auth, and call `reauthenticate()` from `useJazzAuth()` after your auth library signs in or out (apps start signed out; pass `initial="local-first"` for local-first guests). Remove the code that minted or fetched Jazz JWTs, and the sync server's JWKS settings.
4. Keep `JAZZ_ADMIN_SECRET` on the server only; the backend no longer needs `BACKEND_SECRET`.
   **Returning JWT users:** their accounts belong to the identity (your JWT's issuer, its `sub`), while `createJazz` names a user `jazz:backend` with your `userId`. The alpha.59 import (`jazz-alpha59-alpha60-import import`, a binary on the release's GitHub page) links the two on the same account by default, so an `authenticate` that returns the old `sub` as `userId` (as `fromJWT()` does) opens the user's old account. Local-first guests are not affected.
5. A page served from another origin than the backend must be listed in `createJazz({ allowedOrigins })`.
