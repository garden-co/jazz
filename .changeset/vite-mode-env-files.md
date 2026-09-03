---
"jazz-tools": patch
---

Make `jazzPlugin` and `jazzSvelteKit` load Vite environment files from Vite's configured `envDir` for the active serve mode, including `.env.local`, `.env.<mode>` and `.env.<mode>.local`. `envFile: false` disables this loading. Builds continue to skip the hooks; production serve mode is covered.
