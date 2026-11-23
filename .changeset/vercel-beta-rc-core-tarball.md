---
"jazz-tools": patch
---

`vercel()` on a beta release candidate (`2.0.0-beta.N-rc.M`), or on a candidate numbered `N.K-rc.M`, downloads its core from that candidate's GitHub release. It used to look on npm, where release candidates aren't published.
