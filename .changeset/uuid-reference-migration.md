---
"jazz-tools": patch
---

Generate executable identity migrations when existing UUID and UUID-array columns gain explicit relations, preserving stored values and including referenced tables in migration witnesses. Continue rejecting reference removal, retargeting, and unsupported simultaneous column changes.
