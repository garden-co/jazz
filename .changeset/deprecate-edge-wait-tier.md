---
"jazz-tools": patch
---

Mark `wait({ tier: "edge" })` as deprecated in the TypeScript types instead of rejecting it at compile time. It still waits for `"global"`, and editors now point to `"global"` as the replacement.
