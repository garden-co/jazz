---
"jazz-tools": patch
"jazz-napi": patch
---

Add and harden JSON column support:

- Reuse shared JSON serialization in runtime mutation conversion.
- Narrow TypeScript JSON schema typing and schema-derived JSON output types.
- Enforce that `schema` metadata is only accepted on JSON columns in N-API schema conversion.
- Deduplicate JSON/text/enum decode paths between column and array decoding.
