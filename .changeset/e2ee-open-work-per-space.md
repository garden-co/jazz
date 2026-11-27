---
"jazz-tools": patch
---

Encryption: opening the app no longer acquires the keys of, and delivers for, every space the device holds before the app's first reads. Deliveries already stored are not treated as new, a space a read is waiting for is served first, and the once-per-session pass over every held space runs in the background, one space at a time and never ahead of a read. The first connection after the app opens is no longer treated as a reconnect.
