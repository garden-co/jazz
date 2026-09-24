---
"jazz-napi": patch
---

Remove `'Edge'` from the `SubscriptionDeltaEvent.tier` type. The runtime has not produced an Edge tier since the server edge was removed, so subscription events now type their tier as `'None' | 'Local' | 'Global'`.
