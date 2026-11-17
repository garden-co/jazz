---
"jazz-tools": patch
---

jazz-ffi reports a trusted backend client's `for_user` refused for a permissions bundle the main backend doesn't serve as its own error kind, `bundleMismatch` (contract 2, with `own` and `served`; `refused` before), as TS's `code: "bundle_mismatch"`.
