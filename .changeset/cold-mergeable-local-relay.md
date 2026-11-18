---
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Mergeable transaction reads now fetch as reads outside a transaction do.
A cold foreground loads them from its durable local relay, even while its
server is offline, and a Remote read waits for the server, on any client.
Branch-view reads fetch too, including over a frozen base the foreground has
not synced yet. Preserve staged writes, cancellation and mergeable conflict
semantics instead of returning an empty foreground cache. Inside a
transaction, a Remote read still shows other writers' unsettled rows, as a
serializable read does. These reads now go through the same synced query-shape
check as reads outside a transaction, so a shape the relay cannot answer is
refused there too.
