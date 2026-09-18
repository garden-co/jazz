---
"jazz-tools": patch
---

Drain transaction preparation and outstanding reads before committing, retain deferred failures for later waits, and prevent ordinary mutations from overtaking preparation. Preserve synchronous row results and reject deferred admission after rollback, shutdown, discard, or account changes.
