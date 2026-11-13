---
"jazz-tools": patch
---

Signing out of a device account now means what it says (E7):

- **The key is kept.** The session stays signed out, across reloads too. `initial: "device"` creates a new device account only on a device that keeps none and where no one signed out. Before, a reload after `signOut()` started a brand-new device account with no data.
- **New `session.reopenDeviceAccount()`.** It signs back in as the kept device account (the most recent, if several), with its id and its data.
- **`exportDeviceKey()` works while signed out.** It exports the kept key, and throws a `NoKeptDeviceAccountError` when there is none.
- **`clearOnSignOut` forgets the key on sign-out** and wipes the account's data, so nothing on the device can reopen it.

`createDeviceAccount()` still starts a fresh device account.
