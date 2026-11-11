---
"jazz-tools": patch
---

A `schema.ts` and `permissions.ts` that import each other no longer fail `jazz-tools validate` and `migrations create` with a bare "Cannot convert undefined or null to object". `definePermissions` says the import is circular and how to break it, and a schema that fails to load names the file being loaded.
