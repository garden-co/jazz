---
"jazz-tools": patch
---

The Linux `jazz-tools server` binary now runs on jemalloc with always-on sampled heap profiling and keeps line tables, so `GET /debug/pprof/heap` (admin secret, or the new token-protected `--diagnostics-listen` listener) returns a symbolized pprof profile of in-use memory.
