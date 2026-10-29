---
"jazz-tools": patch
---

`reachable_via(...).seeded_by(...)` now takes a session reference, such as `session.user.account` or `session.claims["sub"]`, instead of a raw claim name. A string such as `"sub"` used to build a policy that the schema load rejected; it now throws when the policy is defined.
