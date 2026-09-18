---
"jazz-tools": patch
---

Add opt-in space-scoped equality search over encrypted values, authenticating and
comparing logical candidates before pagination. Materialize nested encrypted
results and track live row/key dependencies with explicit incomplete/error
states. Unsupported encrypted operators remain rejected; current membership and
authority checks apply independently of candidate tokens and cached plaintext.
