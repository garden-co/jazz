---
"jazz-tools": patch
---

Deliver backend mutation errors to `db.onMutationError`, including rejections settled during startup replay before a listener was attached, and give wait rejections readable `reason` text instead of Rust debug output.
