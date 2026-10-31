---
"jazz-tools": patch
---

A browser tab served through a SharedWorker reads each live result as of its own upstream read's settle, the way a direct client does, and hears each move of the upstream's position even when no rows changed.
