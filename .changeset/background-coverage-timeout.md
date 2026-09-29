---
"jazz-tools": patch
---

A local read's background refresh can no longer break the server connection when it fails (for example on a query-coverage timeout). Pending global waits on that client no longer fail with the refresh's error, and the client still reconnects after a later network drop instead of staying offline.
