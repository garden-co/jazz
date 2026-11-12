---
"jazz-tools": patch
---

**Breaking:** `createJazzSession` is now `createJazzClient`, for frontend clients (browser, React Native, Expo) and backend clients alike, and `JazzSessionConfig` is now `JazzClientConfig`. The low-level `createJazzClient` that opened a client for an `AccountHandle` is no longer exported: `createJazzClient` (or your framework's provider) owns every client's account lifecycle.

Everything left from connecting to a Jazz core directly is gone. Your backend runs Jazz with `createJazz`, which holds the core's secrets:

- The Node client (`jazz-tools/backend`) has no `forReqUser`, `forAccount`, `withAttribution` or `withAttributionForRequest`. Use `jazz.forReqUser(request)` and `jazz.withAttribution(request)` on your `createJazz`, which ask your `authenticate` who the user is.
- The Node client no longer takes `allowLocalFirstAuth` (it only governed those request scopes), and `BackendContextConfig` is no longer exported. A Jazz core's own `allowLocalFirstAuth` setting is unchanged.
- `initial` on a Node client is `"local-first"` or left out.
