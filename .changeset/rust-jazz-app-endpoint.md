---
"jazz-tools": patch
---

**Rust (`jazz-app`): a backend serves its users' clients through the app's Jazz endpoint, as `createJazz` does.**

- **Endpoint:** `Endpoint::new(&backend, authenticate)` serves `/jazz` for a backend opened with `Config::backend`. It is framework-neutral: `endpoint.handle(Request { method, path, headers, body })` answers the account routes, and `endpoint.serve_socket(request, incoming, outgoing)` relays an upgraded sync socket. With the `axum` feature, `endpoint.router()` does both.
- **`authenticate`:** an async closure or trait that names the request's user from its cookies or bearer: `Answer::User(AuthUser { user_id, claims, expires_at })`, `Answer::DeviceAccount` (TS `acceptDeviceAccounts`), or `None`.
- **Sync sockets:** the client's prelude is admitted as TS `admitPrelude` does, either a device account's own proof or the session of the user `authenticate` names. The prelude is rewritten to the backend's word and pinned to the bundle the backend published. Shortly before the session's expiry, the client is asked to reconnect.
- **Account routes:** `backend-sign-in` and `backend-user`.
- **Acting as a user:** `endpoint.for_user(&request)` (TS `forReqUser`) and `endpoint.for_auth_user(&user)` return a `Jazz` whose reads and writes run as that user, under their permissions. They run on a client the endpoint opens on first use, which the core trusts to act for them; `endpoint.shutdown()` stops it. `account_for` and `create_account_for` work as in TS.
- **Not supported yet:** `backend-link`, trusted backend clients and the HTTP-stream transport. Each answers 501.
