---
"jazz-tools": patch
---

Browser tabs that sync through the shared worker now all see its upstream link the same way, including tabs opened or reloaded later. The worker keeps its link state, its current upstream failure and any refused sign-in as one value, and sends it to each tab when the tab attaches and again whenever it changes. So when a recreated server store refuses the worker's link with `AuthorityChangedError`, every tab's `global` waits and `db.waitForSync()` fail with it, instead of waiting forever in tabs that attached after the refusal. Their writes stay pending and upload if the app recovers (for example after `db.deleteClientStorage()`). A tab also no longer keeps a stale auth error once the worker reconnects, so an offline sign-out is again told apart correctly from a refused one.
