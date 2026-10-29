---
"jazz-tools": patch
---

Keep the Jazz server inside its memory limit under load. The server now caps live WebSocket connections (default 1024, `--max-connections` / `JAZZ_MAX_CONNECTIONS`) and answers further upgrades with `503` and `Retry-After`, closes connections that send nothing for 60 seconds, and sizes one shared storage cache from the container's memory limit (`--memory-limit-bytes` / `JAZZ_MEMORY_LIMIT_BYTES` to override) instead of giving each internal store its own 256 MiB cache.
