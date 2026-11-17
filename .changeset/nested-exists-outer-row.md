---
"jazz-tools": patch
---

Deploying permissions, publishing a bundle and `jazz-tools validate` now refuse an `exists` nested inside another `exists` that refers to the checked row's columns, with an explicit "not supported in policies yet" error that names the policy path and points to relations (`hopTo`) instead. The engine didn't agree on which row such a reference names. The TypeScript permissions DSL can't produce this shape, so no app built with it is affected. A store that already holds such a policy still opens and serves it as before.
