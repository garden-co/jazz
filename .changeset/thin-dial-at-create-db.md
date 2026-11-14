---
"jazz-tools": patch
---

A thin client now opens its connection when its `Db` is created, not at the app's first query. The socket, its sign-in and its hello need no schema, so they run while the app renders. The first query's runtime then takes over that connection. A connection nobody takes within 30 s closes, and so does one whose Db shuts down or goes offline first. Local-first clients don't change.
