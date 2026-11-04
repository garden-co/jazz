---
"jazz-tools": patch
---

**Breaking:** your app's backend runs Jazz with `createJazz`, which holds the core's secrets and asks your `authenticate` who each request is. The Node path that connected to a core directly with its backend secret is gone:

- **`createJazzSession` from `jazz-tools/backend`** refuses `initial: { backendSecret }`, pointing to `createJazz`. It stays for backend clients, which connect to your main backend with a `credential` (see Backend clients). The browser and React Native `createJazzSession` no longer accept a backend secret as `initial` either; `initial` is `"local-first"` or `"signed-out"`.
- **External JWT verification in Node.** `createJazzSession` no longer takes `jwksUrl`, `jwtPublicKey`, `jwtIssuer` or `jwtAudience`, and refuses them as not supported; the `BackendJwtPublicKey` type and `resolveRequestSession` are no longer exported. Verify your identity provider's tokens in `createJazz`'s `authenticate` with `fromJWT({ jwksUrl, issuer, audience })`, and `jazz.forRequest(request)` acts as the user it names. Local-first guests' bearer tokens are still verified.
- **`jazz-tools/effect/backend`.** `JazzBackend.layer` takes `createJazz`'s options (or use `JazzBackend.layerJazz(jazz)` for a Jazz you already created); `forRequest` and `forCurrentRequest` act as the user your `authenticate` names. `JazzBackend.forAccount` is gone.

  The rest of the Effect backend service changed with it: `JazzBackendService.session`, `.client` and `withAttributionForRequest` are removed; `withAttribution` takes a `Request`; `JazzBackend.fromSession`/`layerSession` are now `fromJazz`/`layerJazz`; `forRequest` and `forCurrentRequest` take only a `Request` (no `RequestLike` or options); `BackendRequestOptions` and `BackendRequestAuthConfig` are no longer exported.

- **`becomeBackend` is removed** from sessions (all frameworks' session actions), from `AccountManager`, and `"becomeBackend"` from `JazzSessionOperation` and `AccountOperation`. A process becomes your app's backend only by running `createJazz`.
