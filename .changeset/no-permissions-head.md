---
"jazz-tools": patch
"@jazz/rust": patch
---

The core no longer keeps a current permissions head. `GET /admin/permissions` and `fetchStoredPermissions()` are removed: list bundles with `GET /admin/bundles` and `fetchPublishedBundles()`. A core no longer re-applies a stored head on startup, and deploying admits a deployment's schemas without making any of them current.

The migration graph (`GET /admin/migrations/graph`, `fetchMigrationGraph()`) reports `latestBundleSchemaHash`, the schema of the bundle published last, in place of `activeSchemaHash`. `jazz-tools migrations create` without a local snapshot uses that schema as its baseline, and the graph marks it "(latest bundle)".
