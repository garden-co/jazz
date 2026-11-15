---
"jazz-tools": patch
"jazz-napi": patch
---

A `sqlite({ path })` store now keeps its identity inside `jazz.sqlite`: its app id, its own store id, where its large values live, and the secret its agent tokens are signed with. On its first start with this release, a store moves the `app-id`, `store-id`, `blob-store` and `inspect-token-secret` files beside it into the database, then deletes them; a file that disagrees with what the store already holds is refused, not chosen. `jazz.sqlite` alone is now the whole store, so a copy or restore of it is the same store, for the same app, with the same chunks. To open a copy next to the original (a staging copy of a backup), run `npx jazz-tools store detach <path>` on the stopped copy and give it its own chunk directory or `s3()` prefix: otherwise it claims the original's chunks. A self-hosted server opened with a different `--app-id` than its data dir was first started with now refuses to start instead of serving another app's data.
