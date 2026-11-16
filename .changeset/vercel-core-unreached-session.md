---
"jazz-tools": patch
---

`vercel()`: a backend whose connections stop reaching its core recovers on its own. When the core's Sandbox answers `/health` but connections to the core keep ending without opening (hung or refused before they reach it), the instance now stops that Sandbox session after 20 s, and the next request wakes a fresh one. Before, it reconnected to the same session until the session ended, and its failed attempts kept that session awake.
