---
"jazz-tools": patch
"jazz-napi": patch
---

Server: a Backend or Admin websocket no longer busy-loops if the account registry stops, and an account websocket then closes with a retryable `NotReady` error.
