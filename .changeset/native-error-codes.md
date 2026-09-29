---
"jazz-tools": patch
---

Errors thrown from the native Jazz runtime now carry a stable `code` (for example `not_observed`); in the browser they are now `Error` objects instead of bare strings. Messages are unchanged.
