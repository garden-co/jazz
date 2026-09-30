---
"jazz-tools": patch
---

Deliver backend mutation errors to `db.onMutationError`, including rejections settled during startup replay before a listener was attached (bounded, with a dropped-count report), release listeners of dropped or shut-down backend `Db`s, and give wait rejections the same readable `reason` text as mutation-error events instead of Rust debug output.
