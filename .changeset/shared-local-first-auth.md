---
"jazz-tools": patch
---

Local-first auth hooks for React, Vue, Svelte and Solid now share one controller per secret store. Consumers of the same store share a single secret load, and a consumer that mounts later no longer reloads the secret. `login` and `signOut` reload every consumer even when saving or clearing the secret fails. A synchronous secret-store throw is reported as an auth error. Vue and Svelte warn once per distinct load failure.
