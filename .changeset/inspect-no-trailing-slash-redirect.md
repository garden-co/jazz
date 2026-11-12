---
"jazz-tools": patch
---

The Inspector page now answers at `<basePath>/inspect` as well as `<basePath>/inspect/`, instead of redirecting to the slash. Next.js redirects `/jazz/inspect/` to `/jazz/inspect` by default, so the two redirects looped forever and the Inspector was unreachable in a default Next.js app.
