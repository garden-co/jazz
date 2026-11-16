---
"jazz-tools": patch
---

The engine now checks, when a query is prepared, that JSON and bytes columns aren't used with `gt`, `gte`, `lt` or `lte` or in `orderBy`, and JSON columns not with `contains`, for every client (TS, thin client, Swift/Kotlin/Go). TS no longer checks this itself; the error comes from the engine with code `query`, and `contains` on a bytes column, which the engine supports, now works from TS. Ordering an include by a JSON or bytes column is now refused, as it was at the root; FFI and thin-client queries that did any of these are now refused instead of comparing JSON text or bytes.
