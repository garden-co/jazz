---
"jazz-tools": patch
---

Read tiers are down to two: `ReadTier.LocalFirst` and `ReadTier.Remote`. A local-first read can now wait a bounded time for the server on its initial load with the new `firstLoadRemoteWaitMs` option (default `0`, which shows local data at once):

```ts
db.subscribe(app.todos, onRows, { tier: ReadTier.LocalFirst, firstLoadRemoteWaitMs: 2_000 });
```

While the client is online, a subscription's first callback or a one-shot read waits up to that long for the server's answer and shows it; if the server has not answered in time, it shows local data. It never waits offline, without a server, after `db.disconnect()`, or once the connection drops. Later changes behave as for any local-first read. Rust clients get the same through `JazzClient::query_local_first` and `JazzClient::subscribe_local_first`.

`ReadTier.LocalFirstUnlessEmpty` is deprecated and warns once; it keeps its behaviour until it is removed in the next breaking release. Switch to `ReadTier.LocalFirst` with `firstLoadRemoteWaitMs`. Unlike the old tier, the new option also waits when the cache already has rows, but never longer than the timeout.
