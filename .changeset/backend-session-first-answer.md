---
"jazz-tools": patch
---

A browser session that follows the app's backend (`createJazzClient`, and `JazzProvider` with `serverUrl`) no longer reports `signed-out` before the backend has answered who is signed in. With no account on the device, it now waits for that first answer, so a returning user goes from loading straight to ready instead of briefly seeing the app's `signedOut` screen on every page load.
