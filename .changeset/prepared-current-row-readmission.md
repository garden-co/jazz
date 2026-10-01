---
"jazz-tools": patch
---

Preserve the deletion marker when preparing claim-routed CurrentRows reads. Existing Edge subscriptions now readmit restored authorized rows without reconnecting or exposing successors while access remains revoked.
