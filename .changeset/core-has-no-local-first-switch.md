---
"jazz-tools": patch
"jazz-napi": patch
---

**Breaking:** a core no longer has its own switch for local-first guests (device accounts). Which guests sync is decided only by the app's `createJazz` `authenticate` returning `acceptDeviceAccounts(request)`, and a core admits only the sockets its backend opens. Before, a core could refuse every guest while the app accepted them (`serveCore` and `jazz-tools server` with `NODE_ENV=production` did unless told otherwise), which left guests offline. Removed with it:

- `jazz-tools server` no longer takes `--allow-local-first-auth`. It refuses to start with `JAZZ_ALLOW_LOCAL_FIRST_AUTH` set to a false value, since a core can no longer turn guests away itself; a true value is ignored.
- `serveCore()`, `JazzCore.open()` and `JazzServer.start()` (jazz-napi) refuse an `allowLocalFirstAuth` option as unknown, and `startLocalJazzServer()` / `startTestJazz({ coreOptions })` no longer take it.
- Rust: `AuthConfig` has no `allow_local_first_auth` field, `ServerBuilder::with_local_first_auth` and the test server's `without_local_first_auth` are gone.

Jazz Cloud's per-app setting (`jazz-tools apps auth set --allow-local-first-auth`) is unchanged. A backend still says clearly when a core refuses guests (an older one, or a Cloud app with them off): linking answers `local_first_auth_disabled`, not that the user already has an account.
