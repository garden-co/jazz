---
"jazz-tools": patch
---

Browser clients no longer name an app id. A client's local data, local-first secret, guest proofs and founder account are scoped by the endpoint it syncs with, and `serverUrl` defaults to `"/jazz"`, the app's own backend. The backend checks a guest's proof against its own endpoint URL and vouches for it to the core, so `appId` now lives only in `createJazz` and `remote()`.

Apps that already have local-first users pass their old app id as `scope`, so those users keep their secrets and accounts. Backend and test code can still pass `appId`.
