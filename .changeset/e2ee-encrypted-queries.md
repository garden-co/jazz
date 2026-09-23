---
"jazz-tools": patch
---

Add opt-in space-scoped equality search over encrypted values, authenticating and
comparing logical candidates before pagination. Materialize nested encrypted
results and track live row/key dependencies with explicit incomplete/error
states. Unsupported encrypted operators remain rejected; current membership and
authority checks apply independently of candidate tokens and cached plaintext.

Verified key access schedules coalesced background device delivery without
waiting for another device's envelope. Explicit maintenance and authority
validation keep their existing failure semantics; background work never replays
the key-use callback.

Indexed whole-enum matches now work in subscriptions as well as ordinary reads;
partial enum matches remain unsupported. Encrypted equality queries accept the
same UUID scope spellings as ordinary queries, including uppercase and compact
forms, while tracking key changes and revoked access under the canonical scope.
