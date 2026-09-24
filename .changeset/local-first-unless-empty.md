---
"jazz-tools": patch
---

**Breaking:** replace `ReadTier.RemoteIfPossible` (`"remote-if-possible"`) with `ReadTier.LocalFirstUnlessEmpty` (`"local-first-unless-empty"`).

The new tier serves local-first results immediately and keeps syncing, like `ReadTier.LocalFirst`. The only difference is an empty local result: while the server is reachable, or its first connection is still being set up, the first delivery waits for the server's first answer instead of showing an empty state. A fresh device therefore opens on synced data, and a device with cached data opens instantly.

The gate lives in the core database, so browser, Node, React Native and Rust clients share one behavior. It never waits on a server it cannot reach. An empty result is delivered at once when no server is configured, after `db.disconnect()`, while the connection is down or retrying, or when the connection attempt fails. It is also delivered when the connection drops during the wait, or five seconds after a connection attempt started if it has not come up. Queries with an `offset` read the server's page whenever the server can answer, because a partly synced cache would apply the offset to the wrong rows. When the server cannot answer, they show the cached page.

`ReadTier.RemoteIfPossible` and `"remote-if-possible"` are removed, in TypeScript and in Rust (`ReadTier::RemoteIfPossible`); passing the old name throws an error. It waited for the server whenever the app had not called `db.disconnect()`, so a dropped or never-established connection left it waiting, or failing, instead of showing local data. Switch to `ReadTier.LocalFirstUnlessEmpty`, or to `ReadTier.Remote` for server-confirmed results.
