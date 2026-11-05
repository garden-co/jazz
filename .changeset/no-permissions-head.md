---
"jazz-tools": patch
---

The core no longer keeps a current permissions head: every session is governed by the bundle it pins.

- `GET /admin/permissions` and `fetchStoredPermissions()` are removed, along with the `StoredPermissionsResponse` and `StoredPermissionsHead` types. List bundles with `GET /admin/bundles` and `fetchPublishedBundles()`. `FetchStoredPermissionsOptions` is renamed `FetchPublishedBundlesOptions`.
- Deploying admits a deployment's schemas (the first one included) without making any of them current, and a core no longer re-applies a stored head on startup. A server refuses a catalogue change that would make a schema current.
- The `permissions_head_missing` rejection reason is removed: no write waits for a permissions head any more; a client meeting it from an older core treats it as an unknown rejection reason.
- Rust: `prepare_catalogue_change` no longer takes a `revision`.
- The migration graph (`GET /admin/migrations/graph`, `fetchMigrationGraph()`) reports `latestBundleSchemaHash`, the schema of the bundle published last, in place of `activeSchemaHash`. Bundles are stamped in publication order, so a bundle published in the same millisecond as another still sorts after it, and publishing an existing bundle again does not make it the latest. `jazz-tools migrations create` without a local snapshot uses that schema as its baseline, and the graph marks it "(latest bundle)". `fetchMigrationGraph()` still reads an older core's `activeSchemaHash`.
