---
"jazz-tools": patch
---

**Rust (`jazz-app`): end-user clients, signed in with a device account or the app's bearer.**

- **Connecting:** an end user's client opens with `Config::new(app_id).server("https://app.example/jazz").user(auth)`. It syncs through the app backend's `createJazz` endpoint, as a browser does.
- **Device accounts:** `Auth::Device(DeviceKey::generate()?)`, or `DeviceKey::load_or_create(path)` on a host without a keychain.
  - The key is the account. It reads and writes as `jazz-auth-v1:…`, the same form as TypeScript's `exportDeviceKey`.
  - The client mints its self-signed tokens and renews them itself.
- **Bearer:** `Auth::bearer(|reason: TokenReason| async move { Ok(my_auth.token(reason.is_fresh()).await) })`.
  - The backend's `authenticate` names the user from the token.
  - The provider is asked again before a JWT expires (`Renew`), and for a new token when the backend refuses one (`Expired`) or on `refresh()` (`Refresh`).
  - When it returns `None`, the user is signed out.
- **Session:** `jazz.session()` gives the state (`SignedIn`, `Expired` or `SignedOut`) and a stream of its changes, plus `refresh()`. `jazz.sign_out()` signs the user out.
- **Sync status:** `jazz.sync_status()` and `jazz.sync_changes()` report `LocalOnly`, `Connecting`, `Connected` or `Offline { reason, retrying }`.

A `Config::server` without `backend` or `user` is now a configuration error.
