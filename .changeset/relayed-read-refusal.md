---
"jazz-tools": patch
---

A remote query in a browser tab (or any foreground over a worker or native relay) no longer hangs when its relay refuses the server, for example after the server's store was recreated (`authority_changed`), when the relay cannot mirror the server's catalogue, or when the relay's store holds writes made before it ever synced. The tab's `tier: "remote"` subscriptions now fail with the relay's refusal, and a subscription the core rejects keeps the core error `code` in JS: a changed authority fails with an `AuthorityChangedError` (detect it with `isAuthorityChangedError`), for subscriptions held directly as well as relayed ones (Node and browser; React Native subscriptions get it through the stopped link, as before). Local-first queries are not failed: they keep reading the worker's store, as when there is no server. A `tier: "remote"` one-shot read on a refused link now fails with the refusal at once instead of waiting out its coverage budget.
