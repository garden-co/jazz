---
"jazz-tools": patch
---

React Native users signed in through the app's backend recover after their bearer expires: reconnecting a native worker whose credential the sync server refused first asks the app for its current bearer and hands it to the worker, instead of retrying with the expired one. Asking is bounded, so a bearer that never answers can't hold later reconnects. Thanks to Xelson for this fix.
