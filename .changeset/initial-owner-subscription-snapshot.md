---
"jazz-tools": patch
---

Keep browser and React Native subscriptions loading until their persistent worker or relay has supplied the first complete local result, avoiding a premature empty snapshot when matching rows are already stored. Genuinely empty results still finish loading without waiting for server sync, and initialized subscriptions do not return to loading on disconnect. Preserve joined-row identities when resetting subscription inputs.
