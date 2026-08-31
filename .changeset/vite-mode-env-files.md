---
"jazz-tools": patch
---

Make `jazzPlugin` and `jazzSvelteKit` load Vite environment files for the active mode in standard precedence order, including `.env.local`, `.env.<mode>` and `.env.<mode>.local`. Production builds no longer inherit development-only values.
