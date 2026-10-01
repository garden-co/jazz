---
"jazz-tools": patch
---

The Linux `jazz-tools server` binary can sample its heap: start it with `JAZZ_HEAP_PROFILE_SAMPLE_BYTES` set (for example `524288`) and `GET /debug/pprof/heap` (admin secret, or the new token-protected `--diagnostics-listen` listener) returns a pprof profile of in-use memory with function names and the binary's build ID. Sampling is off by default, and the route then answers `404` saying how to turn it on.
