---
"jazz-tools": patch
---

**Rust (`jazz-app`): sync status matches TypeScript's `SyncStatus`.**

- **Fields:** `jazz.sync_status()` now reports `pending_uploads`, `oldest_pending_at`, `rejected`, `last_error` and `synced` alongside `link`, and serializes them the same way TypeScript does.
- **Link:** it reads as `Connecting` while a dropped link retries, until an outage is published, so a short blip never shows as offline. `link.state` is `local-only`, `connecting`, `connected` or `offline`, and `Offline.reason` is `network`, `auth`, `stopped` or `disconnected` (kebab-case, as in TypeScript).
