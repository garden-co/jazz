---
"jazz-tools": patch
---

Browser clients no longer name an app id. A client's local data, local-first secret, guest proofs and founder account are scoped by the endpoint it syncs with, and `serverUrl` defaults to `"/jazz"`, the app's own backend. The backend checks a guest's proof against its own endpoint URL and vouches for it to the core, so `appId` now lives only in `createJazz` and `remote()`.

Upgrading: a browser that drops `appId` without passing `scope` starts with a new local-first identity and a new local database. An app with existing local-first users replaces `appId: "…"` with `scope: "…"` (the same value) to keep their accounts and unsynced local data. Behind a proxy that changes the host or path browsers use, set `createJazz({ publicUrl })` to that URL.
