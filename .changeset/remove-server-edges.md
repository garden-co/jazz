---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Remove server edges. Clients connect to Core, which authorizes reads and writes and confirms global durability. Browser workers and native local persistence relays remain supported, as do local queries and optimistic writes.

For applications using the old durability options:

- Replace write waits using `{ tier: "edge" }` with `{ tier: "global" }` when the write must reach the server. Keep `{ tier: "local" }` for local persistence.
- Use `ReadTier.Remote` for reads that need Core confirmation, or `ReadTier.LocalFirst` for immediate local reads. `ReadTier.RemoteIfPossible` retains its explicit-offline fallback behavior.
- Remove server upstream/edge configuration. A server now runs Core; a connection gateway may still route traffic without running a Jazz database.

Existing globally confirmed data keeps its storage encoding. Legacy edge durability is interpreted as local persistence. Locally authored edits accepted only by an old edge remain eligible for normal resubmission to Core, with their original authorship and transaction identity; that old acceptance does not bypass current write permissions. Do not clear local databases to migrate.

For a historical semantic-edge store, an edit can remain pending if Core lacks a parent created by another author or by the old edge. That parent must arrive through an authorized recovery path; reconnecting as the child’s author does not grant authority to upload someone else’s work. Keep the local store intact. Recovery for such shared-edge histories is tracked in [#3242](https://github.com/garden-co/jazz/issues/3242).
