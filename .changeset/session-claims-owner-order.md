---
"jazz-tools": patch
---

Session-scoped writes on a backend (`forSession()`, `forRequest()`, and `createPolicyTestApp`) no longer abort the process with "reentered a suspended operation" when they overlap an earlier write that is still in flight, such as two concurrent upserts of the same row. The session's claims now wait behind the earlier write and take effect in the order the writes were made.

The read-permission check that session upserts and large-value range reads run now also waits for the database instead of taking it synchronously. It no longer panics while another operation is using the database, and it no longer busy-waits on cold storage.
