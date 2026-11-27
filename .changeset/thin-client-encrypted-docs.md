---
"jazz-tools": patch
---

Docs: how an end-to-end encrypted app behaves in the browser before the local-first client has loaded (operations on encrypted columns wait up to 30 seconds and start the load; writes made meanwhile are kept in memory and run in order; a transaction callback may run twice), and that a device protects only the history of other accounts it has seen. The `client` and `encryption` options describe this, and the `localFirst` option no longer says that only the local-first client has `db.transaction`.
