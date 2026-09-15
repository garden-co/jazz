---
"jazz-tools": patch
---

Allow persistent clients that missed a published migration while offline to reopen and fetch the new schema catalogue without an application-managed old-schema bootstrap. Preserve cached rows and pending edits, and keep unpublished schemas unavailable.
