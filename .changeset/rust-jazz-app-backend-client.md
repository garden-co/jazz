---
"jazz-tools": patch
---

**Rust (`jazz-app-support`): backend clients.** A trusted machine can now sync with an app through its main backend's `/jazz`, as TypeScript's `createJazzClient({ serverUrl, credential })` does.

- **Opening:** `Config::new(app_id).server("https://my-app.com/jazz").credential(provider)` opens as a backend client. It needs the app's `jazz.permissions.json`, as `createJazzClient` takes `permissions`. `provider` gives a fresh machine credential, such as an OIDC workload token, for every connection and request.
- **Trusted or scoped:** the main backend's `backendClients` (`trustWorkload`) decides, and `jazz.backend_client()` reports which.
  - A trusted client acts as the system.
  - A scoped client acts as its one user.
  - A credential trusted as neither fails with `Error::Refused`.
- **Acting for a user:** `jazz.for_user(user_id, claims)` on a trusted client returns a handle that reads and writes as that user, under their permissions. It never creates an account:
  - a user with none fails with `Error::NoAccount`;
  - an identity the main backend refuses fails with `Error::Refused`.
- **Closing:** `jazz.close(timeout)` waits for queued writes to reach the backend, then stops (`CLOSE_TIMEOUT` is 10 s). With an in-memory store, writes that never arrived are lost, and it fails with `Error::Unsynced { pending }`. `Duration::ZERO` closes at once and accepts that loss.
