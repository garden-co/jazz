---
"jazz-tools": patch
---

Keep reconnecting to the sync server after an outage of any length. Clients previously gave up after 10 failed attempts (about 8 seconds) and never synced again until reload or restart; they now retry indefinitely with capped, jittered exponential backoff and sync queued writes once the server is back.
