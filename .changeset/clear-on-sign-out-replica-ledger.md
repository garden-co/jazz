---
"jazz-tools": patch
---

`clearOnSignOut` in the browser: one explicit lifecycle for each account's local store (#5250). The behaviour you rely on is unchanged: the last tab out wipes, a tab still signed in never loses data, and the warning counts every tab's unsynced writes. Three things change:

- **No background retry.** A store left behind by a tab that closed while signed in is wiped when another open page becomes visible or its session changes, or on the next page load, once the backend confirms twice that the user is signed out. Before, a signed-out page retried every 15 seconds.
- **Guest data is no longer left behind for good.** If a guest's tab closes without signing out, its data stays while the guest is still the device's account, and is wiped once someone signs it out in another tab.
- **A wipe a crash interrupts is finished** before that account's data opens again, so a half-deleted store is never read.

A wipe no longer opens a client for the signed-out user: `jazz-tools/local-first` deletes the store through its worker.
