---
"jazz-tools": patch
---

BREAKING CHANGE: Reads, inserts, updates, and deletes now require explicit policies. When no permissions are defined or a table has no policies, the server rejects writes and returns no data for reads.

This changes existing behavior: define policies for every table and operation your app needs before upgrading. Optimistic local writes remain possible, but the server will rejects writes without a matching permission.
