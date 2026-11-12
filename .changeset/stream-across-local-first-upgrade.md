---
"jazz-tools": patch
---

A streaming write (`insertStreaming`, `updateStreaming`, `upsertStreaming`) still in flight when the thin client upgrades to local-first now finishes on the thin runtime before that runtime stops, and its value lands intact. Before, the upgrade closed the thin runtime under the stream, which failed with `ThinConnectionLostError: the thin runtime is closed`. If the upgraded client is shut down without waiting for sync while a stream is still in flight, the stream fails with a `ThinConnectionLostError` that says why and that the value was not written.
