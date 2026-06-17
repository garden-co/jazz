---
"jazz-tools": patch
"jazz-wasm": patch
---

Stop the inert "memory access out of bounds" WASM trap from surfacing as an uncaught error when a page is reloaded or closed while two or more Jazz clients share the tab. Each client's WebSocket transport is abandoned mid-navigation and the dying page's WASM heap traps; the runtime now swallows that one specific trap inside the `pagehide` teardown window (on both the main thread and the worker), so it no longer reaches the console or the app's error handlers. A genuine out-of-bounds error during normal operation still surfaces.
