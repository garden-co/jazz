---
"jazz-tools": patch
---

Fix backend N-API sync regression where outbound messages were dropped before they reached the server.

`createJazzContext(...).asBackend()` now accepts the real nested N-API sync callback shape used by published alpha builds, so backend query subscriptions and other upstream sync traffic can leave the local runtime again.
