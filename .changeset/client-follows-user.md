---
"jazz-tools": patch
---

Browser and React Native clients follow the user your auth library has signed in. Every framework's `JazzProvider` takes `user` (`undefined` while your auth loads, `null` when signed out, the user's id when signed in), and sessions have `setUser(…)`. Without either, Jazz asks your backend who is signed in on start, when the page or app returns to the foreground and when another tab signs in or out; a signed-out answer signs out only once a second answer agrees. Signing out, or switching straight to another user, closes that account's client; in browsers, `clearOnSignOut` wipes its local storage for shared devices (not yet supported in React Native). `createJazz` re-runs `authenticate` every `recheckSeconds` (default 60) and ends a sync connection whose user it no longer names; the client reconnects with fresh cookies, and if no one is signed in that reconnect fails as an auth failure, so the client stops retrying and signs out.
