---
"jazz-tools": patch
---

`vercel()` now explains why `@vercel/sandbox` 4.1.0 to 5.0.0 fail to load in a plain Node Vercel Function (one Vercel doesn't bundle): they `require()` an ES module through `@vercel/drives`, which Vercel's Node runtime doesn't allow. The error says to install `@vercel/sandbox@4.0.0`; Next.js apps bundle the package and aren't affected.
