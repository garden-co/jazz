---
"jazz-tools": patch
---

**Breaking:** a device account's auth mode is now `"device-account"` (was `"local-first"`), everywhere it shows: `session.authMode` in policies, `Session.authMode`, `getAuthState().authMode` and `session.authMode`, `useAuthState().authMode`, and `createPolicyTestApp().as({ authMode })`. TypeScript flags comparisons with `"local-first"`; plain-JS code such as `authMode !== "local-first"` stops matching without an error, so search your app for it.

A permissions file or `schema.ts` rule that compares `session.authMode` with `"local-first"` is now refused with an error naming `"device-account"`. Without the error, `session.authMode != 'local-first'` would start letting device accounts in. Replace `'local-first'` with `'device-account'` and redeploy. A rule that was published before you upgrade and still has `"local-first"` fails closed. Every query its policy guards then fails for every user, signed-in users included, until you redeploy.

**Jazz Cloud:** upgrade your app's core (`apps upgrade`) and redeploy your backend with this version together. While they run different versions, `eq(session.authMode, …)` and `ne(session.authMode, …)` mean different things on each side: with a newer backend and an older core, `ne(session.authMode, 'device-account')` lets guests in. To stay correct during a staggered upgrade, test the issuer instead, which both versions read the same way: `eq(session.identity.issuer, 'urn:jazz:local-first')`.

`AccountAuthError` codes are renamed: `local_first_account_required` is now `device_account_required`, `local_first_recovery_unavailable` is now `device_account_recovery_unavailable`, and `local_first_restore_unavailable` is now `device_account_restore_unavailable`. Code that matches `error.code` against the old strings stops matching without an error.

Device-account JWTs (issuer `urn:jazz:local-first`) and local storage stay as they were, so devices keep their data.
