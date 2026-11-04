---
"jazz-tools": patch
---

**Breaking:** your app's backend runs Jazz with `createJazz`, which holds the core's secrets and asks your `authenticate` who each request is. The Node path that connected to a core directly with its backend secret is gone:

- **`createJazzSession` from `jazz-tools/backend`** refuses `initial: { backendSecret }`, pointing to `createJazz`. It stays for backend clients, which connect to your main backend with a `credential` (see Backend clients). The browser, React Native and Expo `createJazzSession` are unchanged.
- **External JWT verification in Node.** `createJazzSession` and `createJazzContext` no longer take `jwksUrl`, `jwtPublicKey`, `jwtIssuer` or `jwtAudience`, and refuse them as not supported; the `BackendJwtPublicKey` type and `resolveRequestSession` are no longer exported. Verify your identity provider's tokens in `authenticate` (for example with `jose`), and `jazz.forRequest(request)` acts as the user it returns. Local-first guests' bearer tokens are still verified.
- **`jazz-tools/effect/backend`.** `JazzBackend.layer` takes `createJazz`'s options (or use `JazzBackend.layerJazz(jazz)` for a Jazz you already created); `forRequest` and `forCurrentRequest` act as the user your `authenticate` names. `JazzBackend.forAccount` is gone.
