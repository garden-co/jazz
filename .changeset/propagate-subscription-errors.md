---
"jazz-tools": patch
---

Deliver established native subscription failures through the `Db.subscribeAll` callback object's `onError` listener instead of throwing them from an asynchronous callback. Errors retained by retired or unsubscribed native streams are ignored, so only the active subscription generation can reject an orchestrated query entry.
