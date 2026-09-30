---
"jazz-tools": patch
---

An exclusive transaction that reads with `include()` or a join no longer times out with `not_observed` (or hangs in the browser) when the related table has a read policy that uses the session, such as a `$createdBy` owner check or access inherited from another table.
