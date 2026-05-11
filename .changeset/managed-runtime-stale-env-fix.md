---
"jazz-tools": patch
---

`ManagedDevRuntime` no longer throws when a prior in-process run leaves `*_JAZZ_SERVER_URL` set in `process.env`. The env var on its own is now treated as our own persisted value and ignored in favour of spinning up a fresh local server. The plugin still takes the "connect to an external server" path when the caller explicitly supplies an `adminSecret` option or sets `JAZZ_ADMIN_SECRET`. This makes Vite HMR restarts and repeated test runs work without stale-state errors.
