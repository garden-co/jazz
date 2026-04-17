---
"jazz-tools": minor
---

Add BIP39 recovery phrase for local-first identity, exposed at the new `jazz-tools/passphrase` subpath. `RecoveryPhrase.fromSecret` / `RecoveryPhrase.toSecret` encode and decode the 32-byte local-first auth secret as a 24-word English mnemonic, with structured `RecoveryPhraseError` codes and forgiving whitespace/case normalization. Also fixes a latent cache bug in `BrowserAuthSecretStore` and `ExpoAuthSecretStore` where `saveSecret` did not invalidate `cachedPromise`, so a restore after `getOrCreateSecret` would silently keep the pre-restore secret.
