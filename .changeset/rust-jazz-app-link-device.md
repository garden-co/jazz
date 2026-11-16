---
"jazz-tools": patch
---

**Rust (`jazz-app`): link a device account to the user's own sign-in.** `session.link(bearer)` carries a device account over to the user the app's backend names from `bearer`, so the account and its data move with them. It works as TypeScript's `linkDeviceAccount` does, through the `backend-link` challenge.
