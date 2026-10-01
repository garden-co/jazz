---
"jazz-tools": patch
---

Fix browser subscriptions that silently stopped delivering while a backend rapidly rewrote rows next to a large (chunked) value. Persisting a publication now keeps driving the query work that owns an in-flight chunk install, so the SharedWorker no longer deadlocks on IndexedDB's write gate, and direct query openings no longer take over the runtime owner's wake-up for suspended cold work.
