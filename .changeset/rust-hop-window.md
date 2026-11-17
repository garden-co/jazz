---
"jazz-tools": patch
---

**Rust: `order_by`, `limit` and `offset` before a `hop_to` apply to the rows it reaches**, as in TypeScript, where a query has one order and window applied to the rows it ends on. `jazz-app-support` used to refuse them with `Error::Unsupported`.
