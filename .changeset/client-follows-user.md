---
"jazz-tools": patch
---

Browser clients follow the user your auth library has signed in. Every framework's `JazzProvider` takes `user` (`undefined` while your auth loads, `null` when signed out, the user's id when signed in), and vanilla sessions have `setUser(…)`. Without either, Jazz asks your backend who is signed in on start, when the page regains focus and when another tab signs in or out. Signing out closes that account's client, and `clearOnSignOut` wipes its local storage for shared devices. `createJazz` re-runs `authenticate` every `recheckSeconds` (default 60) and ends a sync connection whose user is no longer signed in.
