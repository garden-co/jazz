---
"jazz-tools": patch
---

The Linux `jazz-tools server` binary now samples its heap continuously at low cost, so `GET /debug/pprof/heap` (admin secret, or the new token-protected `--diagnostics-listen` listener) returns a pprof profile of in-use memory with function names and the binary's build ID.
