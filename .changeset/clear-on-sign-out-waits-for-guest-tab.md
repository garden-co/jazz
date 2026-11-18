---
"jazz-tools": patch
---

`clearOnSignOut` in the browser no longer wipes a store another tab still has open. Tabs of one account share its local storage: a sign-out now closes this tab's client and wipes only once no other tab has the account open, so a tab that signs out of Jazz while another tab is still signed in (offline, say) leaves that tab its data and unsynced writes, and the last tab to sign out wipes. A tab following a sign-out from another tab no longer races that tab's wipe and ends in an error. The wipe's warning now counts every unsynced write it removes, whichever tab made it.

Also, `db.deleteClientStorage()` no longer fails with "incompatible persistent browser configuration" when the `Db` hasn't read or written yet, or when another tab holds the store under another sign-in (a guest who signed up in this tab): it waits for that tab to follow or close, and fails with a `StoreHeldByAnotherTabError` after 30 seconds.
