---
"jazz-tools": patch
---

**Rust (`jazz-app-support`): a link the app's backend refuses for good now fails with `Error::Refused`.** That covers a user who already has another account. It used to be `Error::Server`, which hosts retry as a network failure. A bearer that names no one is still `Error::SignedOut`.
