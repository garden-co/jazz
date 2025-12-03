---
"jazz-tools": patch
---

Improve error logging in subscriptions and add stacktraces on errors coming from React hooks.

Added jazzConfig.setCustomErrorReporter API to intercept subscription errors and send them to an error tracker.
