---
"jazz-tools": patch
---

Add `rivet()` to host the Jazz core in a Rivet actor. Rows live in the actor’s SQLite database. Set the required `blobs` option to `"sqlite"`, `s3()`, or `vercelBlob()` to choose where large values live.
