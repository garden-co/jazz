---
"jazz-tools": patch
"jazz-napi": patch
---

A sync server no longer holds unbounded output for a client that reads slowly or stops reading. Once what earlier ticks queued for a session and the client has not read passes twice the session's biggest single tick (at least 64 MiB), the server closes the session with `Backpressure` and asks the client to retry later, instead of growing until the process runs out of memory. One tick's output, such as a large first result, is always sent whole.
