---
"jazz-tools": patch
---

`acceptLocalFirstAuth` is recognised when another copy of `jazz-tools/backend` made it (Next dev evaluates the backend per route, and `createJazz` shares one Jazz per store across copies), so guests no longer get 500s once a second route loads. An `authenticate` answer that is neither `null`, `acceptLocalFirstAuth(request)` nor `{ userId: string }` now fails with an error that says so, instead of a 422 "missing field `subject`" from the core.
