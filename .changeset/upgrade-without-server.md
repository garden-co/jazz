---
"jazz-tools": patch
---

A browser that opens with the server unreachable (a first visit offline, an outage) now still upgrades from the thin client to local-first. Before, the upgrade read the app's live queries from the server first, failed, and left the app on the thin client for the rest of the page's life: a guest's writes stayed in memory, didn't show in its queries, and were lost on reload. The upgrade now waits for those reads only while the server answers.
