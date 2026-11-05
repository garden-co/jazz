---
"jazz-tools": patch
"jazz-napi": patch
---

Deployments no longer activate permissions on the core. The `/admin/deploy` route is removed: `/admin/bundles` publishes a deployment's schemas, migrations and permissions as a bundle that sessions are pinned to, and makes nothing current. `deploy()` from `jazz-tools/dev` and `jazz-tools/testing` publishes the same way and returns the bundle's id as `bundle`. A new deployment's target no longer has to be reachable from an active schema; its history must still have a single root, so backends on sibling branches serve side by side.

`GET /admin/bundles` lists every published bundle with its write schema and permissions, and `fetchPublishedBundles()` from `jazz-tools` reads it. The standalone Inspector shows the permissions of the latest bundle published for the schema it inspects.

`createJazzSession()` from `jazz-tools/backend` takes `bundle`, the id `deploy()` returned. A backend that acts for its users without `createJazz` must pass it: an unpinned backend link can no longer act for a user, only as itself.
