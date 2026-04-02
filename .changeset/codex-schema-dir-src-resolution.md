---
"jazz-tools": patch
---

CLI schema resolution now accepts apps that keep `schema.ts` and `permissions.ts` in `src/` as well as the app root.

The legacy `--schema-dir ./schema` shim is no longer supported. Point CLI commands at the app root instead, where Jazz will resolve `schema.ts` from either the root or `src/`.
