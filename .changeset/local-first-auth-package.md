---
"@garden-co/local-first-auth": patch
---

New package: local-first auth without Jazz. A device-held secret derives a stable identity and signs short-lived tokens that any backend verifies with `verifyRequest`. Formats are byte-identical to Jazz's existing local-first auth, so existing secrets, recovery phrases and passkey backups keep their identity.
