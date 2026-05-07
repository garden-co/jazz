---
"jazz-tools": patch
---

Move the dedicated-worker bridge orchestration (init handshake, peer routing, lifecycle hints, shutdown handshake, outbox routing, server-payload forwarding) out of TypeScript and into Rust (`crates/jazz-wasm`). `JsSyncSender` and the `onSyncMessageToSend` WASM API are gone; the runtime now posts directly to the worker via `worker.postMessage`. The TypeScript `WorkerBridge` is a thin adapter over the Rust-owned `WasmWorkerBridge`; `jazz-worker.ts` is reduced to a WASM-bootstrap shim. Public `Db` API and the on-the-wire structured-clone protocol shape are unchanged.
