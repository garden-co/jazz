---
"jazz-tools": patch
---

Browser account contexts now start their SharedWorker, and ask it to fetch and compile its WASM, while the page is still loading its own runtime and resolving the account's credential, instead of only after both finish. The worker still admits its durable owner before configuring anything with WASM, and an early lease is only adopted for the exact root it was acquired for. URL-addressed WASM assets served as `application/wasm` are now compiled with `WebAssembly.compileStreaming` as they download. Any other content type, or a runtime without streaming compilation, falls back to the buffered load and its magic-byte check.
