---
"jazz-tools": patch
---

A relay (a browser worker or a native relay) now refuses a session of another identity with the error reason `identity_mismatch`: the browser worker admits each tab as the author the tab connected with, and refuses one whose author is not its own. A replica signed in as a user also refuses a local write made as another session. Before, both uploaded those writes under the relay's identity, so the server attributed them to the relay's user. Tabs signed in as the worker's own user are unaffected.
