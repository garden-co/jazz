---
"jazz-tools": patch
---

**Rust (`jazz-app-support`): `JazzSession`, a user-following session, as TypeScript's `JazzSession`.**

- **Opening:** `JazzSession::open(files, SessionConfig::new(app_id).server(url).storage(..).bearer(provider))`.
- **Following the app's backend:** the session follows the user the app's bearer names. Call `refresh()` when the app's own auth changes.
  - The same user keeps the same `Jazz`.
  - Another user gets their own, and the old one fails with `Error::Replaced`; its subscriptions end.
  - No user signs out a backend user, but keeps a device guest.
  - A user who has no account yet, while a guest is signed in, gets the guest linked to them, keeping its data (`link_guest`, on by default, as TypeScript's `linkGuest`). A link the backend refuses for good signs them in on their own account instead, once. With `link_guest(false)`, the guest stays for the app to link with `link_device_account`.
- **Device accounts:** `create_device_account`, `restore_device_account(key)`, `reopen_device_account`, `link_device_account` and `export_device_key`.
- **State and lifecycle** (status strings as TypeScript's: `signed-out`):
  - `snapshot()` and `changes()` give the status (`starting | ready | signed-out | transitioning | error | closed`), the user, a `clientEpoch`, and a failed operation's `error` and `recovery`.
  - `jazz()` gives the signed-in user's `Jazz`, or `Error::NoUser { status }`.
  - Also `sign_out`, `retry` and `close`.
- **Storage:** with `Storage::Dir(dir)`, each account's store lives in `dir/<account id>/`, and the device key in `dir/device.key`.
- **Runtime:** every operation runs on the session's own runtime, so it can be called from any executor.
