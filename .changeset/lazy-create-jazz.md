---
"jazz-tools": patch
"jazz-napi": patch
---

`createJazz` is synchronous and opens nothing when called, so a module can export `const jazz = createJazz({ ... })` and `export const { GET, POST, DELETE } = jazz.next()` at its top level, with no `await`, `globalThis` cache or wrapper. The store opens on first use: a request, a `jazz.db` call, `forRequest`, `withAttribution`, `accountFor`, `linkIdentity`, or `await jazz.ready()` (which resolves to the Jazz, to fail at startup or to read `appId`). `jazz.appId` is known once the store has opened, and throws before.

A process runs one Jazz per store (a `sqlite()` directory, a `vercel()` core): a later `createJazz` for it, as a dev server's hot reload or another module graph makes, returns the same Jazz and applies that call's app, permissions and options on its next use. Store settings that would need another core are refused while it is open; a credential hook is not such a setting, so the open store calls the latest call's. A reload that leaves the app and permissions as they were (equal ones rebuilt by a hot reload included) keeps the backend's session; one that publishes another bundle keeps the old session open a minute longer for the writes and `forRequest` dbs still using it. Each `remote()` and `memory()` store stays its own.

`jazz.db` works before the store opens: writes return at once (an insert's row, id included) and apply in order as it opens, reads wait for it, and subscriptions start then. A SQLite directory another process holds still refuses to open, and now says that is what happened.

`jazz.db` holds at most 10,000 writes before the store opens, and a socket held meanwhile closes (1013) past 4 MiB. A failed open is tried again after a backoff (1s, doubling to 30s) rather than on every request, or at once by a later `createJazz`. `jazz.close()` applies the writes made before it, even while the store is still opening, and waits at most 30s for that open; from the close on, `jazz.db` throws. A `createJazz` for the store while it closes opens once the close has let go of it.
