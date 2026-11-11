---
"jazz-tools": patch
---

Add a shared core mutation-admission fence so hosts can stop application writes
while draining committed uploads, authority receipts and storage cleanup.
