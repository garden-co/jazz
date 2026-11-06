---
"jazz-tools": patch
---

`loginWithBackend()` and `linkWithBackend()` from `jazz-tools/client` now refuse on a host that sends no cookies (Node, Deno, a CLI), with an error that names the supported ways to sign in with a bearer token: `bearer` on React Native, or `createJazzClient` from `jazz-tools/backend` with `credential` on Node. Before, a bearer added through the `fetch` option signed the user in, but the sync connection carried no identity: the session reported ready, global waits never settled, and "Account auth refresh failed" was logged every few seconds. A user the backend signed in no longer runs that token refresh at all, so a browser whose backend session lapses stops logging it too.
