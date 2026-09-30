---
"jazz-tools": patch
---

Keep read-your-writes for Global reads during large-value uploads: a Global read now waits until the local writes it depends on (writes to the tables it reads, including a streamed row itself) have gone out to the server, and fails with an explicit error if one of those uploads fails. Reads of other tables are not delayed, and the wait no longer counts against the read's coverage timeout.
