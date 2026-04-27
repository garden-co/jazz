---
"jazz-tools": patch
---

Fix the SvelteKit dev plugin so the first-ever cold start no longer needs a manual restart. The plugin now triggers a Vite restart immediately after allocating a fresh app ID, so SvelteKit's `$env/*` capture re-reads the now-populated `.env` on the second pass. Plugin order in `vite.config.ts` is no longer load-bearing.
