---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

### Upgrading from alpha.59

alpha.60 is a breaking release for every app. Upgrade in this order:

1. **Sync server first.** An alpha.60 sync server (core) speaks wire protocol v6 and admits only its app's backend, so alpha.59 clients can't reach it, and `createJazz` refuses to start against an older core. Plan one cutover per app, and ship the new backend and clients with it, because both change. On Jazz Cloud you start the upgrade yourself, when your new backend and clients are ready to ship; we don't move your app for you. On your own server, stop it, move its data dir with the alpha.59 importer, and drop the JWT flags and their `JAZZ_*` variables (see core-admits-only-its-backend). Download the `jazz-alpha59-export` binary for your platform from this release's GitHub assets, export the old data dir with it, then run `jazz-tools migrate-alpha59 import` and `jazz-tools migrate-alpha59 verify` (see alpha59-migration and the Backups & Upgrades docs page). The import checks the result against what alpha.59 reads, and links each user's old identity to `(jazz:backend, sub)`, so returning users keep their accounts when your `authenticate` answers with their old `sub`. Later releases keep the importer, so you can upgrade from alpha.59 whenever you're ready.
2. **Your backend.** Add `createJazz` with `remote({ url, appId, secret })` and an `authenticate` that reads your app's own session (an app that signed users in with a JWT can use `fromJWT()`, which answers with the token's `sub`), and mount it at `/jazz` (see client-signs-in-through-backend). Your backend now publishes the schema, permissions and migrations when it starts. `jazz-tools deploy` and the dev plugins are gone, and so are the starters: `npm create jazz` now prints a brief for your coding agent instead of scaffolding an app.
3. **Your schema.** Import `schema as s` and `migration as m` instead of `col`, run `jazz-tools validate`, and pass the generated migrations to `s.defineApp(schema, { migrations })`.
4. **Your clients.** Drop `appId` from browser clients (pass `scope` with the old value to keep existing local-first identities). Point `serverUrl` at your backend (`"/jazz"` by default) and remove JWT auth. Browser and React Native apps, the Node and WASM bindings and the React Native native build all move together: the native relay ABI is now 4, so a JavaScript bundle and a native build from different releases refuse each other.

Local data on devices doesn't carry over. Opening a local store written by alpha.59 throws `UnsupportedStorageCodecsError` (match it with `isUnsupportedStorageCodecsError`) and leaves the store unchanged. Delete it, and the client resyncs from the server. Writes that were still waiting to upload when a device upgraded are lost, so let clients sync before you cut over.
