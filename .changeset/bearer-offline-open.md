---
"jazz-tools": patch
---

**Native clients (`jazz-app-support`, `jazz-ffi`): a user signed in with the app's bearer opens their persistent store offline.** Each successful bearer sign-in records the bearer JWT's `iss`/`sub` and the account the server named in `<dataDir>/bearer-accounts.json`. When the app's endpoint can't be reached at open (no HTTP answer: connecting fails, the connection drops, or the request times out), a JWT bearer that signed in on this device before opens that account's store, with reads and local writes working.

Until the server has signed the bearer in again as the same account, the client makes no request and uploads nothing as that user. `reconnect` and `disconnect` don't change this: they take effect once it is verified. The session keeps trying to sign in, with a backoff, while the server can't be reached or answers anything other than a sign-in or a no (a `5xx`, `429`, `404`, a redirect, or a page that isn't a sign-in, such as a captive portal's). If the server names another account, or says no to the bearer (`401`, `403`), the client stops and the session reads signed out. Writes made offline stay in that account's local store, and upload the next time that account signs in on this device. While it waits, the session reads signed in as the remembered user.

An opaque bearer, a user who never signed in on this device, a deleted store, and a server that answers at open (`4xx`/`5xx`) still fail the open, now with a message saying that opening offline needs an earlier sign-in on this device.
