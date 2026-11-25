---
"jazz-napi": patch
"jazz-tools": patch
---

A Vercel Function using `jazz-tools/backend` fits Vercel's 250 MB limit again: `jazz-napi` no longer bundles every platform's binary; npm installs only yours. If it's missing (optional dependencies omitted, or `node_modules` created on another platform), the error names the `@garden-co/jazz-napi-<platform>` package to install.
