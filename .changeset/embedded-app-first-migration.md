---
"jazz-tools": patch
---

An app on its own store (`createJazz` with `sqlite()`) can now follow the migrations docs without a Jazz server. The first `jazz-tools migrations create` with no snapshot and no `--server-url` snapshots `schema.ts` as the project's first version, as the docs describe. `migrations create` now also updates `migrations/index.ts`, so a new migration ships without another `validate`. `validate` says when `schema.ts` changed since the latest snapshot, instead of the permission-only note. The "disconnected roots" deploy error now says how to connect the versions.
