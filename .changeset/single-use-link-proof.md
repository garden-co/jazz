---
"jazz-tools": patch
"@garden-co/local-first-auth": patch
---

`linkWithBackend()` no longer sends the guest's sync token. The client asks the backend for a challenge that names the signed-in user and expires after two minutes, and signs it with its local-first key. The core links on each proof once. A leaked, unexpired guest token can no longer link the guest's account to someone else. local-first auth gains `signChallenge` and `verifyChallenge`.
