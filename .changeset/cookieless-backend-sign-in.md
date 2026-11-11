---
"jazz-tools": patch
---

`loginWithBackend()` and `linkWithBackend()` now refuse outside a browser page or worker (Node, Deno, a CLI), on `jazz-tools/client`, `jazz-tools/thin` and the root `createAccountManager`, with an error that says bearer sign-in is supported on React Native (`bearer` on `createAccountManager` from `jazz-tools/react-native` or `jazz-tools/expo`). Before, a bearer added through the `fetch` option signed the user in, but the sync connection carried no identity: the session reported ready, global waits never settled, and "Account auth refresh failed" was logged every few seconds. A user the backend signed in no longer runs that token refresh at all, so a browser whose backend session lapses stops logging it too.
