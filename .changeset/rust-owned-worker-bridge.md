---
"jazz-tools": minor
"jazz-wasm": minor
---

Rewrites the dedicated-worker bridge in Rust. The `WasmRuntime` now exposes
`createWorkerBridge(worker, options)` which builds a `WasmWorkerBridge` that
owns the worker `Worker` handle, the postMessage protocol envelope, the init
handshake state machine, and the peer routing table. The TypeScript
`WorkerBridge` shrinks to a thin adapter, the `jazz-worker.ts` shim drops to a
WASM-loader + bootstrap, and the legacy `worker-protocol.ts` is removed.
Wire-format payloads (everything after init/ready) are now postcard-encoded
`Uint8Array` instead of JS objects.
