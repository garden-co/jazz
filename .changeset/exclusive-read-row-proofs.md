---
"jazz-tools": patch
---

Exclusive transactions no longer act on stale data. Reads inside an exclusive transaction now fetch the server's rows for their snapshot first, so a backend sees rows it had never received (for example someone else's redemption of a single-use invite). A row the server already deleted, such as a revoked invite, can no longer be committed on: the server now checks every row the transaction read, including on retries after `exclusive_conflict`.
