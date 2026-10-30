---
"jazz-tools": patch
---

A backend's `forRequest`/`forSession` read now returns rows the user can read through an `exists` policy on another table without that table having been read first: replicas receive the rows a correlated `exists` read policy rests on, as they already did for `allowedTo.read` parents. A reconnecting client no longer receives such rows again when it already holds them.
