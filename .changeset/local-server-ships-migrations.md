---
"jazz-tools": patch
---

`startLocalJazzServer({ schema: app, permissions })` now publishes an app that ships migrations (`defineApp(schema, { migrations })`) the way `createJazz` does, with every schema version and lens it ships. Before, the test server took the app's current schema as its first version, so the app's own `createJazz` then failed with "deployment cannot replace the runtime genesis schema", and clients opening with the app's lenses refused queries. Apps without migrations start as before. Combining such an app with `trustedWorkload` throws an explicit "not supported yet" error.
