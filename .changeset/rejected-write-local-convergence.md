---
---

Wait for the originating client's exact corrected local row after a rejected write, rather than treating the pre-existing optimistic row as convergence. An observer's server-visible marker does not settle a separate client's delivery stream. The expected owner/title, LocalFirst read, and existing query deadline are unchanged; missing rollback still fails the bounded wait.
