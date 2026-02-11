# Client State Cleanup — TODO

Garbage collection of server-side state for disconnected clients.

## Overview

Stable client IDs let the server maintain sync cursors and reactive query subscriptions across reconnects. But clients can disappear permanently (user uninstalls app, clears browser data), leaving orphaned state on the server.

Need a strategy for when and how to clean up:

- Sync cursors (last-seen sequence numbers per client)
- Active query subscriptions
- Client role/session records
- Any per-client caches or buffers

## Open Questions

- TTL-based expiry (e.g., no activity for 30 days)?
- Should cleanup be eager (background GC) or lazy (clean on next access attempt)?
- How to handle clients that reconnect after their state was cleaned up (full re-sync)?
- Per-app configurable retention policies?
- Impact on storage: how much server-side state does each client actually consume?
