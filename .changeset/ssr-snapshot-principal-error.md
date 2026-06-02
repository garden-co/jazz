---
"jazz-tools": patch
---

SSR snapshot hydration now treats a cross-principal snapshot as an error rather than a warning. A snapshot scoped to one principal arriving in a session for a different principal throws instead of silently discarding, so it can never seed one user's rows into another user's session. A snapshot with no live principal yet (the pre-session seed) still defers to the live client, and public (`null` principal) snapshots still seed into any session.
