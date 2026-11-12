---
"jazz-tools": patch
---

With `clearOnSignOut` in the browser, a sign-out's wipe no longer fails with "incompatible persistent browser configuration" when another tab has the store open. A `Db` that hadn't read or written yet (a sign-out right after a sign-up) can now delete the store another tab opened with the app's schema. When that other tab is still the guest the user signed up from, the wipe waits for it to follow the sign-out (or close), then wipes. If it holds the store too long, the sign-out (and `db.deleteClientStorage()`) fails with a `StoreHeldByAnotherTabError` and leaves the session signed in, so the app can ask the user to close the other tab and sign out again.
