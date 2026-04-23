---
---

Remove stale `jazz_principal_id` JWT claims from the `react-betterauth` and
`react-hybrid` starters. Jazz ignores any JWT claim other than `sub` for
session user-id resolution; the claim was misleading new adopters into
thinking it was required.
