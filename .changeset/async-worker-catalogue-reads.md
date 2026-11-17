---
"jazz-tools": patch
---

Await worker-backed catalogue refreshes and deployment startup acknowledgements without blocking a Tokio executor thread. Account-registry SQL continues to run on its dedicated owner thread.
