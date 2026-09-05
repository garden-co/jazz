---
"jazz-tools": patch
---

Keep managed development backend credentials in the server process instead of returned Next.js configuration. Applications using `jazz-tools/dev/next` should read `BACKEND_SECRET` from the server environment.
