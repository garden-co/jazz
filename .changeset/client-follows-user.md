---
"jazz-tools": patch
---

`<JazzProvider user={…}>` and `session.setUser(…)` follow the user your auth library has signed in (`undefined` while loading, `null` when signed out). Signing out closes that account's client, and `clearOnSignOut` wipes its local storage for shared devices. `createJazz` re-runs `authenticate` every `recheckSeconds` (default 60) and ends a sync connection whose user is no longer signed in.
