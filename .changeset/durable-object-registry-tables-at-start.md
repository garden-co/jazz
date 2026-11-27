---
"jazz-tools": patch
---

Compatibility: a Durable Object core now creates the account registry's tables when it starts, for every app, plain apps included. Before, they were created on the first use of the accounts route. These are new tables in the object's SQL; no existing table or stored encoding changes.
