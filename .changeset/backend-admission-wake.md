---
"jazz-tools": patch
---

Node backends (`createJazzSession` with `initial: { backendSecret }`) now wait for a hibernated Jazz Cloud app to wake instead of failing their first sign-in with `Backend admission failed (503)` or a timeout. Admission retries gateway and not-ready answers (`502`, `503`, `504`), network failures and stalled attempts with jittered backoff, honouring `Retry-After`, for up to three minutes (the cloud gateway's own hold); any other answer, such as `401` for a wrong secret, still fails at once, as does a malformed server URL or secret.
