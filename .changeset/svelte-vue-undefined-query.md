---
"jazz-tools": patch
---

Allow `QuerySubscription` (Svelte) and `useAll` (Vue) to accept `undefined` queries, matching the React `useAll` behaviour. When `undefined` is passed, the subscription returns `undefined` without subscribing.
