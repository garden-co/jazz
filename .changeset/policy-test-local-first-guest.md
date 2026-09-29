---
"jazz-tools": patch
---

`createPolicyTestApp().as()` can now act as a local-first guest: pass `authMode: "local-first"` without an `issuer` and the test app acts with a real self-signed local-first identity and its founding account, admitted the same way `forRequest()` admits local-first clients. `testApp.accountFor(session)` returns that account. A missing author issuer or subject now fails with a clear error instead of a `TypeError`.
