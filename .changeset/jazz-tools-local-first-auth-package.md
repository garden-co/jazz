---
"jazz-tools": patch
---

Local-first tokens are now minted and verified in TypeScript by a self-contained local-first auth module inside jazz-tools instead of by the browser and Node native runtimes (React Native still mints natively, with identical output). They are byte-identical to the tokens minted before, so existing local-first identities are unaffected. `ttlSeconds` for local-first tokens and identity proofs must now be a positive integer; 0 or a fraction throws instead of being coerced. `jazz-tools/passphrase` and `jazz-tools/passkey-backup` now come from that module.
