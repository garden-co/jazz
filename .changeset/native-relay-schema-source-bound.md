---
"jazz-rn": patch
---

React Native: a schema source (which carries every migration the app ever shipped) is bounded on its own at 32 MiB instead of by the 1 MiB credential budget, so a long-lived app no longer outgrows it and fails to open its foreground.
