---
"jazz-tools": patch
"jazz-napi": patch
---

Server: a large-value store I/O thread that dies now ends the server process with exit code 70, after logging `large-value chunk lane died; the server exits` with the app and lane, so its orchestrator restarts it. Before, uploads and fetches stopped silently. Store failures still fail only their batch and are retried with backoff.
