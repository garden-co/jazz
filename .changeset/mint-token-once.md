---
"jazz-tools": patch
---

A page load no longer signs its local-first token two or three times in plain JS. The account manager mints a root's token at most once and reuses it for the restore, the account it founds and every connection. It keeps the token next to the root in the account selection store, so a reload within the first half of the token's hour signs nothing. Ed25519 signing in JS (the thin client, and the browser's account manager in local-first too) also uses a 4-bit base-point window instead of noble's 8-bit one, whose 4096-point table cost ~40 ms on the first signature of every load. The token is unchanged: the same claims, issuer, audience and 1 h lifetime.
