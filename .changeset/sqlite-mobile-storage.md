---
"jazz-rn": patch
---

Switch the mobile persistent storage engine to SQLite.

**WARNING:** Existing local data stored with the previous Fjall-based engine is not compatible with SQLite. On-device data will be lost on upgrade — users will need to re-sync from the server.
