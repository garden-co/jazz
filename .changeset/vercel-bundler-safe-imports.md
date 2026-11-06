---
"jazz-tools": patch
---

Backend fixes under Next.js bundlers:

- An app on `sqlite()` no longer needs `@vercel/sandbox` installed. Before, `next dev` and `next build` with Turbopack failed every `/jazz` request with "Can't resolve '@vercel/sandbox'".
- `vercel()` finds its own version in a deployed function built with webpack (Next 15). Before, every request failed with "Cannot find module 'jazz-tools/package.json'".
