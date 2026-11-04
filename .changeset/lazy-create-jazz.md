---
"jazz-tools": patch
"jazz-napi": patch
---

`createJazz` is synchronous and opens nothing when called, so a module can export `const jazz = createJazz({ ... })` and `export const { GET, POST, DELETE } = jazz.next()` at its top level, with no `await`, `globalThis` cache or wrapper. The store opens on first use: a request, a `jazz.db` call, `forRequest`, `withAttribution`, `accountFor`, `linkIdentity`, or `await jazz.ready()` (which resolves to the Jazz, to fail at startup or to read `appId`). `jazz.appId` is known once the store has opened, and throws before.

A process runs one Jazz per store (a `sqlite()` directory, a `vercel()` core, a `memory()` store's `appId`): a later `createJazz` for it, as a dev server's hot reload or another module graph makes, returns the same Jazz and applies that call's app, permissions and options on its next use. Store settings that would need another core are refused while it is open. Each `remote()` store, and each `memory()` store without an `appId`, stays its own.

`jazz.db` works before the store opens: writes return at once (an insert's row, id included) and apply in order as it opens, reads wait for it, and subscriptions start then. A SQLite directory another process holds still refuses to open, and now says that is what happened.
