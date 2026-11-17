---
"jazz-rn": patch
---

React Native now gives up a link the server has gone silent on (Wi-Fi off, a suspended server, a half-open connection) after 45 s and reconnects, instead of showing a live link with nothing arriving. Thanks to Xelson for this fix.
