---
"jazz-tools": patch
---

Exclusive transactions no longer act on stale data. Reads inside an exclusive transaction now fetch the server's rows for their snapshot first, so a backend sees rows it had never received (for example someone else's redemption of a single-use invite). A row the server already deleted, such as a revoked invite, can no longer be committed on: the server now checks every row the transaction read, including on retries after `exclusive_conflict`.

While the server is unreachable, exclusive reads answer from local data instead of waiting, but such a transaction can no longer commit: its commit fails because its reads could not be checked against the server. Retry it in a new exclusive transaction once the server is reachable again. Apps that have never synced with a server are unaffected.
