---
"jazz-tools": patch
---

**Rust, Swift and Go: opening a device session no longer replaces the device account's kept key.** Affected: 2.0.0-alpha.66 to 2.0.0-beta.0. `JazzSession::open` with `Initial::Device`, and jazz-ffi sessions with `"initial": "device"` (Go's, and Swift's `JazzSession`, which starts that way without a bearer), wrote a new account's key over `device.key` when the kept account couldn't be opened for a config error (a plain-http server URL, for example) or its key file didn't parse or couldn't be read. The app went on as a new guest, without the original account and its unsynced writes. Kotlin has no session API and isn't affected: its apps keep the key themselves.

- A new account is made only when no key is kept (no key file). Any other failure leaves the file as it is and is the snapshot's `error`, with status `error`: fix the cause and `retry`, or open again.
- Sessions opened at once on a first launch keep one key, where the file system has hard links.
- `create_device_account` still replaces a kept key, readable or not, but only once the new account signed in. `retry` after a failed create is for the same new account, rather than making another.

Upgrading doesn't undo an earlier replacement. The replaced key itself can't be recovered unless it was exported or backed up (`restore_device_account` then brings the account back). The account's local store isn't deleted: it stays in `<dir>/<account id>/`, unsynced writes included, so don't delete that directory. An account that was linked to a signed-in user opens again when that user signs in. For another, the app's operator may be able to link it to a signed-in user.
