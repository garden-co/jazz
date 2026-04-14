---
"jazz-tools": patch
---

Remove demo auth, anonymous auth, and synthetic users. The only valid auth modes are now local-first (Ed25519 JWT) and external (JWKS JWT). Add Expo support for local-first auth secret generation via expo-crypto.
