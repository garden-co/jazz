---
"jazz-tools": patch
---

A local-first tab in Chrome's back/forward cache no longer holds the store. When the user navigates away, the tab lets go of its two Web Locks (its liveness lock and its store's) and detaches from the shared worker. When the user goes back, it takes them again and reconnects, keeping its in-memory state and any writes the worker hadn't seen. Before, Chrome cached the tab with its locks held, so the worker still counted it as open: a tab under another sign-in waited for it until it reported the store held, and the cached tab was dropped from the cache whenever another tab wrote. If another tab wiped the store while the tab was cached (it signed out as the last tab out), the tab reloads when the user goes back. A tab with an operation still waiting on the worker keeps its connection. Firefox still doesn't cache local-first tabs: it caches no page connected to a SharedWorker that holds a lock.
