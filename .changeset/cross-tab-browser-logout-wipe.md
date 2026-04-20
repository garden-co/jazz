---
"jazz-tools": patch
---

Add coordinated browser logout and storage wipe support so follower tabs can trigger an OPFS reset through the elected leader, stale fallback namespaces are removed, and `db.logout({ wipeData: true })` clears browser state before the next session starts.
