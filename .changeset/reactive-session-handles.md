---
"jazz-tools": patch
---

`getSession()` (Svelte) and `useSession()` (Vue) now return reactive handles that track auth changes without destroying the provider.

- **Svelte**: `getSession()` returns `{ current: Session | null }` — read `.current` in templates, `$derived`, or `$effect` and it updates automatically on login/logout.
- **Vue**: `useSession()` returns `ComputedRef<Session | null>` — bind `.value` in templates or computed properties and it stays in sync via `triggerRef`.
