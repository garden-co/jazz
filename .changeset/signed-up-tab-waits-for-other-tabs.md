---
"jazz-tools": patch
---

A local-first guest who signs up in one tab while another tab is open no longer has the tab they signed up in stop syncing. That tab's new client was refused by the shared worker, which the other tab still held as the guest. It ended `offline` (`stopped`), read nothing, and its writes never uploaded. Now it shows `connecting` and joins once the other tab follows the sign-up, or goes away: the worker now notices a tab whose page closed, crashed or was discarded without closing its client, and lets go of what it held. Queries, waits and writes made in the meantime go on once it joins; writes stay in that tab's memory until then, so a reload before it joins still loses them.

If the other tab doesn't let go within 30 seconds (it is frozen in the background, or runs a build that doesn't follow sign-ups), the tab reports `{ state: "offline", reason: "held-by-another-tab", retrying: true }` and fails what waits for the store with a `StoreHeldByAnotherTabError` (check with `isStoreHeldByAnotherTabError`), so the app can ask the user to close or reload the other tab. It keeps trying in the background and joins as soon as it can. Any other configuration a worker refuses, such as an older deploy's schema in another tab, still fails visibly.
