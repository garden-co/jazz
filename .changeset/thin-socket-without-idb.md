---
"jazz-tools": patch
---

A page now opens its socket without first looking its endpoint up in IndexedDB. The memory of backends that serve the HTTP stream instead of sockets now keeps a copy in the page's Web Storage, which the page reads at once. A thin client's socket opens 10–20 ms sooner on every load. Workers still use IndexedDB.
