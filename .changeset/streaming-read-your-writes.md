---
"jazz-tools": patch
---

Keep read-your-writes for Global reads after a streaming insert: a query no longer reaches the server ahead of writes that were held back while a streamed value uploaded.
