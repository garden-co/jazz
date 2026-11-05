---
"jazz-tools": patch
---

createJazz no longer takes an `appId`. `remote({ url, appId, secret })` names the app the remote core serves. `sqlite()` generates an app id on first start and keeps it in `<path>/app-id`, `vercel()` keeps its id on the core's Drive, and `memory()` makes a fresh one. None of them takes an `appId`, and passing one throws: an alpha.59 app's id reaches a new store through `jazz-tools migrate-alpha59 --app-id`, which writes it to `<path>/app-id`. `jazz.appId` is the id the core runs as. Passing `appId` to createJazz now fails with an error saying where it moved.
