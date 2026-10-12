---
"jazz-tools": patch
---

The Linux `jazz-tools server` binary samples its heap: `GET /debug/pprof/heap` (admin secret, or the new token-protected `--diagnostics-listen` listener) returns a pprof profile of in-use memory with function names and the binary's build ID. `JAZZ_HEAP_PROFILE_SAMPLE_BYTES` changes the mean bytes between samples (512 KiB by default); `0` turns sampling off, and the route then answers `404`.
