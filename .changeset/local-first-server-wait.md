---
"jazz-tools": patch
---

**Breaking:** reads now have exactly two tiers, `ReadTier.LocalFirst` (`"local-first"`) and `ReadTier.Remote` (`"remote"`). A local-first read can wait a bounded time for the server on its initial load with the new `firstLoadRemoteWaitMs` option (default `0`, which shows local data at once):

```ts
db.subscribe(app.todos, onRows, { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: 2_000 });
```

While the client is online, a subscription's first callback or a one-shot read waits up to that long for the server's answer and shows it; if the server has not answered in time, it shows local data. It never waits offline, without a server, after `db.disconnect()`, or once the connection drops. Later changes behave as for any local-first read. Rust clients get the same through `JazzClient::query_local_first` and `JazzClient::subscribe_local_first`.

Removed read tiers now throw with a migration message:

- `ReadTier.LocalFirstUnlessEmpty` (`"local-first-unless-empty"`, Rust `ReadTier::LocalFirstUnlessEmpty`): use `ReadTier.LocalFirst` with `firstLoadRemoteWaitMs`. Unlike the old tier, the wait also applies when the cache already has rows, but never lasts longer than the timeout.
- The read names `"local"` and `"global"`: use `"local-first"` and `"remote"`. A `"remote"` read shows your own pending writes once the server confirms them, where a `"global"` read showed them at once.

Write durability is unchanged: `wait({ tier: "local" | "global" })` keeps its tiers.
