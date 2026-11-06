---
"jazz-tools": patch
---

A relay (a browser worker or a native relay) now refuses a session of another identity with the error reason `identity_mismatch`. The browser worker admits each tab as the author the tab connected with, and refuses one whose author is not its own. Before, it relayed such a tab's writes under its own identity, so the server attributed them to the worker's user. Tabs signed in as the worker's own user are unaffected.
