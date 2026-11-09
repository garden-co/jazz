---
"jazz-tools": patch
---

With `clearOnSignOut`, a user signed out while no page of theirs was open (a server-side logout, or a switch to another user before a reload) is now wiped from IndexedDB on the next load. Before, the wipe reported success and left their local-first replica in place, because `deleteClientStorage()` on a browser Db that had not yet read or written did nothing.
