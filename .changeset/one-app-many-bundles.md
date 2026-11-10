---
"jazz-tools": patch
---

A `sqlite()` store is shared again between `next build` output and code Node runs unbundled (`next dev`, a `tsx` seed script, a custom server's own `createJazz`), for an app with migrations. Next's server build can load `jazz-tools` twice: once for the module that defines the app, once for the one that opens the store. The migrations `defineApp` records were private to the first copy, so the second opened the app without them, as though its current schema were its first. The store then refused one runtime or the other: "it was opened without the app's lenses into it" or "deployment cannot replace the runtime genesis schema". The app's schema now carries its migrations itself, so every copy reads them.
