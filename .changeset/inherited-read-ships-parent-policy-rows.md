---
"jazz-tools": patch
---

Reads of rows that inherit read access from a parent (`allowedTo.read("...")`) now also bring along the rows the parent's own read policy checks, such as a `policy.shares.exists` membership. A freshly started backend, or a client that had never read the parent's table, used to return no rows for such a read until it had read those tables directly.
