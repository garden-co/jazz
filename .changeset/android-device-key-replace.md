---
"jazz-tools": patch
---

Native sessions on Android: restoring or creating a device account takes the lock a first launch creates `device.key` under, so a first launch in another process can't replace the key just kept. A process frozen while holding that lock fails the other's operation after 5 seconds, leaving the kept key, instead of stalling it and `close`. Other platforms are unchanged.
