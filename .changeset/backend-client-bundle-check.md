---
"jazz-tools": patch
---

Trusted backend clients check their permissions against the main backend's ([jazz#5808](https://github.com/garden-co/jazz/issues/5808)). `createJazz` now answers a trusted backend client's admission with the bundle it pins the client to (`X-Jazz-Bundle`). A `createJazzClient` opened with a `jazz.permissions.json` compares that with the bundle its own app and permissions make, and `client.forUser(...)` throws (`code: "bundle_mismatch"`, naming both bundle ids) on a mismatch. Before, a stale or laxer permissions file silently widened what a user's reads returned. The client's own system reads and writes are unaffected. Against an older main backend, or when the client is opened with compiled engine policies, nothing is checked, as before.
