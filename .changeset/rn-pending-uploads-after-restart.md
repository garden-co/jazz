---
"jazz-tools": patch
---

React Native: `pendingUploads` (and the upload status) now count writes the relay store recovered after an app restart. Previously a new foreground answered from its own, initially empty, store only and reported 0 pending uploads while unconfirmed writes were still waiting to sync.
