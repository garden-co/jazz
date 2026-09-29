---
"jazz-tools": patch
---

Errors thrown from the native Jazz runtime now carry a stable `code` (for example `not_observed`); in the browser they are now `Error` objects instead of bare strings. Messages are unchanged. React Native: the native relay ABI moves to 2, so a JavaScript-only (OTA) update to this release needs a new native build; a mismatched build fails at startup with "new native development/release build required".
