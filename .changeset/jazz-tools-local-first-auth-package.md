---
"jazz-tools": patch
---

Local-first tokens are now minted and verified by `@garden-co/local-first-auth` in TypeScript, on every host, instead of by the native runtimes. They are byte-identical to the tokens minted before, so existing local-first identities are unaffected. `jazz-tools/passphrase` and `jazz-tools/passkey-backup` now re-export the package's recovery phrase and passkey backup.
