---
"jazz-tools": patch
---

React Native: `db.getAuthState().error`, and the sync status's `link.authReason`, now clear when the sync server admits the app's connection again after refusing its credential, as they already did in the browser. Before, an earlier refusal's reason could linger after a reconnect.
