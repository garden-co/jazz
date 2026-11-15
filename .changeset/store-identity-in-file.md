---
"jazz-tools": patch
"jazz-napi": patch
---

A `sqlite({ path })` store and a self-hosted server's data dir now keep their identity inside `jazz.sqlite`: the app id, the store's own id, and where its large values live (an in-process `sqlite()` store also keeps the secret its agent tokens are signed with). `jazz.sqlite` alone is now the whole store, so a copy or restore of it is the same store, for the same app, with the same chunks. The `app-id`, `store-id`, `blob-store` and `inspect-token-secret` files stay beside it as copies each start rewrites, so an earlier release opened on the directory keeps the same identity; a copy that disagrees with `jazz.sqlite` is refused, not chosen. To open a copy next to the original (a staging copy of a backup), run `npx jazz-tools store detach <path>` on the stopped copy and give it its own chunk directory or `s3()` prefix: otherwise it claims the original's chunks. **New refusal:** a store keeps the app it was first opened for, so a self-hosted server started with a different `--app-id` on an existing data dir now refuses to start instead of serving that data as another app.
