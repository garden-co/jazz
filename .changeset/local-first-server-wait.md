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
- The read names `"local"` and `"global"`: use `"local-first"` and `"remote"`. A one-shot `"remote"` read still sees your earlier writes, but a `"remote"` subscription shows your pending writes only once the server confirms them, where a `"global"` subscription showed them at once.

Outside the browser, a client with a server now reads `"remote"` by default, where it used to read `"global"`. A one-shot remote read is answered after your earlier writes are uploaded, so it still sees them. This doesn't yet hold across a reconnect (#3863). This also fixes rows that briefly disappeared from a subscription when the server acknowledged the writes that created them, which only happened with the old `"global"` read.

TypeScript rejects `firstLoadRemoteWaitMs` on a `"remote"` read; `tier` selects which options a read accepts.

Write waits now accept only `"local"` and `"global"`. The deprecated `"edge"` overload is removed from TypeScript, and `wait({ tier: "edge" })` rejects at runtime with a `TypeError` instead of warning and waiting for `"global"`. Use `wait({ tier: "global" })` for server confirmation. The error rejects only the wait: the write was already applied, so wait on the existing handle with a valid tier rather than retrying the write. The durability semantics of `"local"` and `"global"` are unchanged.
