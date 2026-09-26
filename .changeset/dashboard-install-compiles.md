---
"jazz-tools": patch
---

Opening many subscriptions at once, such as a whole dashboard, no longer recompiles queries whose compiled programs were evicted before their installers ran, and the engine classifies each query node's aggregate dependencies once instead of walking its ancestors every time. Results are unchanged.
