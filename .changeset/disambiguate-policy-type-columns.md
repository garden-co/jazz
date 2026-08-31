---
"jazz-tools": major
---

**Breaking change:** permission consumers no longer interpret unbranded `PolicyExpr` objects as raw
policy IR. Plain objects—including objects with policy-shaped keys such as `type`—are now row
predicates. Wrap manually-authored or stored policy IR with `raw(...)`, and change reusable helpers
that can return any DSL condition to the new opaque `PermissionExpressionInput` type.
`PermissionExpression` remains available for helpers that specifically return branded raw policy
IR. DSL atoms, compounds, existence checks, session predicates, and resolved row-condition objects
all compose through `PermissionExpressionInput`, while plain row-predicate objects remain
structurally typed. `isCreator`, `allowedTo.*`, `allOf`, and `anyOf` continue to work with schemas
whose columns use policy-shaped names.
