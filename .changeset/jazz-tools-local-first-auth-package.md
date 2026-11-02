---
"jazz-tools": patch
---

Local-first tokens are now minted and verified by `@garden-co/local-first-auth` in TypeScript instead of by the browser and Node native runtimes (React Native still mints natively, with identical output). They are byte-identical to the tokens minted before, so existing local-first identities are unaffected. `ttlSeconds` for local-first tokens and identity proofs must now be a positive integer; 0 or a fraction throws instead of being coerced. `jazz-tools/passphrase` and `jazz-tools/passkey-backup` now re-export the package's recovery phrase and passkey backup.
