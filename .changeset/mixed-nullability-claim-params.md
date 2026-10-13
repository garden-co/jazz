---
"jazz-tools": patch
---

Fix authorised reads when one account claim is compared with both nullable ownership and required membership UUID columns. Finalise parameter types before checking predicates so anchored exact-base aliases consistently require the strongest UUID carrier in every comparison order and after canonical revalidation. Nullable-only groups retain nullable bindings, contradictory null checks reject consistently, and membership, containment and checked literal inference retain their operator-specific semantics.
