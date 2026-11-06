---
"jazz-tools": patch
---

When another user signs in on a local-first browser without signing out first, Jazz now switches to them. Before, the previous user's client was refused by the server and couldn't finish syncing, so the session was left stuck on an error in every tab.
