---
"jazz-tools": patch
---

Keep reconnecting to the sync server after an outage of any length. Previously, clients gave up after 10 failed attempts (about 8 seconds) and never synced again until the page was reloaded or the process restarted. They now retry indefinitely with capped, jittered exponential backoff, and queued writes sync once the server is back. Global reads and waits still reject once the server has been unreachable for about 8 seconds, and they work again after the reconnect.
