---
"jazz-tools": patch
---

Svelte `QuerySubscription` keeps its options bag reactive (pass a getter, e.g. `() => ({ tier })`, and the subscription re-runs when its reactive reads change), and you can combine reactive options with a one-shot SSR `snapshot` in the same getter (`() => ({ tier, snapshot })`). The `snapshot` always seeds the store once at construction and is never reactive: if it changes after the first render it is ignored, and a development warning is logged so the silent staleness is visible.
