---
"jazz-tools": patch
---

Add optional `navigation` prop to `JazzSvelteProvider` that automatically waits for pending CoValue syncs before SvelteKit navigations, preventing stale data on SSR pages.
