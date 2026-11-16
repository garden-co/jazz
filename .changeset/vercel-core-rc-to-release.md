---
"jazz-tools": patch
---

`vercel()`: a core that ran a release candidate now moves to the final release. Core versions compared release candidates wrongly: `2.0.0-alpha.64` didn't count as newer than `2.0.0-alpha.64-rc.1`, and `alpha.100-rc.1` didn't count as newer than `alpha.99-rc.1`. So a Drive that once ran an rc kept running it after the release was deployed. The backend and the core's start script now agree that a `-rc.N` sorts before the release it is a candidate for.
