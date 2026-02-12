---
"jazz-tools": patch
---

Introduced runtime validation for schema-based CoValues. All mutations now accept a `validation` option of `strict` or `loose`. `setDefaultValidationMode()` can also be used to enable or disable validation across the entire app. Currently, the default validation mode is `warn`: updates and inserts of invalid data will still be allowed, but a console warning will be issued. The usage of `setDefaultValidationMode("strict")` is encouraged, as it will be the default mode in the future.
