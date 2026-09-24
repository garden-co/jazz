---
"jazz-tools": patch
---

Add `ReadTier.LocalFirstUnlessEmpty` (`"local-first-unless-empty"`) and deprecate `ReadTier.RemoteIfPossible`.

The new tier serves local-first results immediately and keeps syncing, like `ReadTier.LocalFirst`. The only difference is an empty local result: while the server is reachable, or its first connection is still being set up, the first delivery waits for the server's first answer instead of showing an empty state. A fresh device therefore opens on synced data, and a device with cached data opens instantly.

It never waits on a server it cannot reach. An empty result is delivered at once when no server is configured, after `db.disconnect()`, while the connection is down or retrying, or when the connection attempt fails. It is also delivered when the connection drops during the wait, or when a first connection has not come up within five seconds.

`"remote-if-possible"` is still accepted as a deprecated alias with the new behavior. Previously it waited for the server whenever the app had not called `db.disconnect()`, so a dropped or never-established connection left it waiting, or failing, instead of showing local data. Apps that need server-confirmed results should use `ReadTier.Remote`.
