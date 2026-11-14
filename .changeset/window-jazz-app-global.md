---
"jazz-tools": patch
---

A page that sets its own `window.__jazz` no longer breaks Jazz's auth flows. Before, the browser session refused it (`window.__jazz is reserved for Jazz`), so `linkDeviceAccount()` and the upgrade to local-first failed on such a page. Jazz now leaves the app's global as it is, warns once on the console, and works without its page handle (`window.__jazz.status()`, `waitForSync()`, `clearStorage()`); rename your own global to get the handle back.
