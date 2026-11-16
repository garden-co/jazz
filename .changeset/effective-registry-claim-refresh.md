---
"jazz-tools": patch
---

Live queries now refresh when the executing user's registered claims change, even if the prepared query carries claims for a different user. Matching explicit claims, including an explicitly empty scope, remain fixed.
