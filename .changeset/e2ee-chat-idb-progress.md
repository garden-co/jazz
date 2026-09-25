---
"jazz-tools": patch
---

Prevent persistent browser queries from hanging behind a parked IndexedDB write. Reads and subsequent mutations can complete an already-started write without waiting for its original query owner to resume. Preserve the original write result, reconcile started commits after caller cancellation, and leave cancelled unstarted writes inert. Storage format and public APIs are unchanged.

Add an automatically initialised encrypted chat example with image uploads, offline browser-restart coverage, and end-to-end encryption documentation. The example distinguishes local visibility, server availability and the original write's acceptance receipt.
