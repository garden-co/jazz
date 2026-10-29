---
"jazz-tools": patch
---

Make LocalOnly transaction reads return from local data without waiting for the
server. Keep the snapshot fixed after that read, including when a later read
requests remote coverage.
