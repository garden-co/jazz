---
"cojson": patch
---

Added client-side load request throttling to improve the loading experience when loading alot data concurrently.

When a client requests more than 100 CoValues concurrently, load requests are now queued locally and sent as capacity becomes available.
