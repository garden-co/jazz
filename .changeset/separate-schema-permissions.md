---
"jazz-tools": patch
---

Require separate schema and permissions inputs for `startLocalJazzServer` and `createJazzSession` from `jazz-tools/backend`. Explicit permissions replace all embedded policies, including when the bundle is empty or omits tables. A local server can still start without a schema for a later `deploy`.

Remove the `mergePermissionsIntoWasmSchema` export from `jazz-tools/testing`. Pass the schema and permissions separately instead.
