---
"jazz-tools": patch
---

A streaming write (`insertStreaming`, `updateStreaming`, `upsertStreaming`) still in flight when the thin client upgrades to local-first now finishes on the thin runtime before that runtime stops, and its value lands intact. Before, the upgrade closed the thin runtime under the stream, which failed with `ThinConnectionLostError: the thin runtime is closed`. A shutdown the app asks for afterwards (a sign-out, an account switch, a close) is never held by such a stream: the stream is cut off and fails with a `ThinConnectionLostError` that says why and that the value was not written.
