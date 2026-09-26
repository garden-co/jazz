---
"jazz-napi": patch
"jazz-tools": patch
---

Keep driving a Node client tick that yields between evaluation turns instead of dropping it. A fresh subscription whose first result was a few MB (for example 100 rows of 60 KB) previously never delivered any rows and reported no error.
