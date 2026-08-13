---
"jazz-napi": patch
---

Release native JavaScript callbacks when `NapiRuntime.close()` is called so Node.js processes can exit without waiting for garbage collection.
