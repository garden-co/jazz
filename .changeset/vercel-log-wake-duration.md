---
"jazz-tools": patch
---

`vercel()`: a backend now logs one line each time it wakes its core, saying how long the wake took and what it did, e.g. `jazz: vercel(): core jazz-prj-production woke in 8412 ms (resumed)`. A core that was already answering logs nothing. Slow wakes can now be read from a deployment's function logs.
