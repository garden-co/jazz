---
"jazz-tools": patch
---

createJazz no longer takes an `appId`. `remote({ url, appId, secret })` names the app the remote core serves. `sqlite()` generates an app id on first start and keeps it in `<path>/app-id`, `vercel()` keeps its id on the core's Drive, and `memory()` makes a fresh one. Each of them takes an optional `appId`, only to keep an existing app's id. `jazz.appId` is the id the core runs as. Passing `appId` to createJazz now fails with an error saying where it moved.
