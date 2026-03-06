---
"jazz-tools": patch
---

`QuerySubscription` in the Svelte bindings now accepts an options object as its second argument (e.g. `{ tier: 'edge' }`), matching the React `useAll` API. The previous bare-string form is removed.
