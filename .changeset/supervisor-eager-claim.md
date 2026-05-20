---
"jazz-tools": patch
---

Fix multi-tab `createDb()` deadlock in the leader-tab persistent runtime.

When two tabs called `createDb()` before either had performed any read or write, the second tab's `createDb()` would time out after 15 s with `Timed out waiting for leader-tab runtime endpoint`:

- The first tab acquires the `navigator.locks` lease and becomes leader, but the supervisor previously withheld `claim-leader` from the broker until `WorkerBridge.init()` resolved — which is only triggered lazily by the first `getClient()` call.
- With no claimed leader at the broker and the lock held by the first tab, the second tab could neither become leader nor receive a follower endpoint.

The supervisor now claims leadership eagerly upon winning the lock. The race the old withhold guarded against (a `follower-port` arriving before Rust owns the worker's `onmessage`) is now handled downstream:

- the worker JS shim buffers `event.ports` alongside `event.data` so `attach-tab-port` messages arriving during the WASM bootstrap don't lose their transferred `MessagePort`;
- `runAsWorker` extracts those ports into a pre-Ready buffer and drains them via `handle_attach_tab_port` after the runtime transitions to `Ready`.
