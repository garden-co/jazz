---
"jazz-tools": patch
---

Reduce migration workflow churn for schema changes that do not require row transforms.

`jazz-tools migrations create` and `jazz-tools migrations push` now treat default-only and column-order-only schema hash changes as compatible transitions that do not need a reviewed migration file, while still requiring reviewed migrations for incompatible changes like nullability or reference updates. The CLI also now accepts reviewed migration modules that load through CommonJS-style nested `default` exports.
