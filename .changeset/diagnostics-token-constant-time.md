---
"jazz-tools": patch
---

jazz-tools server: the diagnostics listener's bearer token is now compared in constant time, like the admin and backend secrets, so response timing doesn't reveal how much of a guessed token matched.
