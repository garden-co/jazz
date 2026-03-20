---
"jazz-tools": patch
---

Align the Vue and Svelte bindings more closely with React: Vue `useAll` now accepts `QueryOptions` and re-exports `DurabilityTier`/`QueryOptions`, while Svelte query subscriptions now use the shared subscription orchestrator, surface async subscription errors, and export `createExtensionJazzClient` and `attachDevTools` for extension tooling.
