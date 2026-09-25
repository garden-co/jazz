---
"jazz-tools": patch
---

A permissioned Global one-shot read of a bounded page (a limit of 1 to 1,000 rows, no relations or array subqueries) is now answered directly by the server when it supports this, instead of the client first building a local query graph. On an anonymized SaaS fixture, 39 relayed page reads took about 0.5 s instead of 1.7 s. Older servers and relays, reads with pending local writes, and other query shapes keep the existing path.
