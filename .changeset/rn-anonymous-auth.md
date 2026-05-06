---
"jazz-tools": patch
"jazz-rn": patch
---

Add React Native anonymous auth support. `jazz-tools` now mints anonymous JWTs through the React Native runtime module when no auth credentials are provided, and `jazz-rn` exposes the matching native `mintAnonymousToken` binding.
