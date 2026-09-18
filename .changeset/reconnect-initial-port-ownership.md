---
---

Let the reconnect fixture's initial Core server bind an ephemeral port, then reuse its actual bound port for the replacement Core. This removes the initial bind-after-release reservation race without changing production behavior or reconnect assertions.
