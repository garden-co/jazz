---
"jazz-tools": patch
"jazz-napi": patch
---

**Breaking:** a sync server (core) admits only its app's backend. Browsers, React Native apps and other clients sync through the app's `createJazz` handler (`serverUrl: "/jazz"` or the backend's `/jazz` URL); a client that connects straight to a core is refused with "this core admits only its app's backend". Local-first guests keep working: the backend relays their proofs. Removed with it:

- **Server JWT verification.** `jazz-tools server` no longer takes `--jwks-url`, `--jwt-public-key`, `--jwt-issuer`, `--jwt-audience`, `--auth-cookie-name` or `--trust-proxy`, and refuses to start while their `JAZZ_*` environment variables are set, naming them. `JazzServer.start()` (jazz-napi) rejects `jwksUrl`, `jwtIssuer` and `jwtAudience` (and any other unknown option) instead of ignoring them; `JazzCore.open()` no longer takes `jwtPublicKey` and the other JWT options. `startLocalJazzServer()` drops its `jwksUrl`, `jwtIssuer` and `jwtAudience` options. The `JAZZ_JWKS_CACHE_TTL_SECS` and `JAZZ_JWKS_MAX_STALE_SECS` variables are gone. Trusted workloads (`--trusted-workload-*`) still verify their OIDC tokens.
- **Public account routes.** `/apps/:id/accounts/register`, `login`, `login-or-register` and `found-local-first` are gone. The backend vouches for its users at `/apps/:id/admin/accounts/*` with its backend secret (or admin secret), which `createJazz` does for you.
- **Test helpers.** jazz-napi no longer exports `TestJwtIssuer`. `startTestJwtIssuer().jwtForUser()` is now async and its default claims are `{}` (it was `{ role: "user" }`); pass the claims a test needs.
- **Cloud CLI.** `jazz-tools apps auth set` takes only `--allow-local-first-auth`; `--jwks-url`, `--jwt-issuer`, `--jwt-audience`, `--jwt-public-key` and `--jwt-public-key-file` are refused as not supported.

### Migrating

- A web or React Native app: add `createJazz` to your backend with `remote({ url, appId, secret })` (your sync server, your app id and its admin secret), mount it at `/jazz`, and point the client's `serverUrl` there. Sign users in with your backend's own session (`authenticate`), as the client-signs-in-through-backend changeset describes.
- A Rust or other native service that held a user JWT: it is the app's backend now. Give it the core's backend secret and state the user it acts for (`AppContext { backend_secret, backend_session: Some(Session::new(format!("jazz:app:{app_id}"), user_id)), .. }`), after resolving that user's account at `/admin/accounts/login-or-register`. `examples/todo-server-rs` shows this.
- Drop `--jwks-url` and the other JWT settings from your sync server's command line and environment; it fails to start while they are set.
