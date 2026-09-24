---
"jazz-tools": patch
---

Support explicit encrypted schema migration and stable catalogue identities
through renames. Initialize a legacy plaintext scope on its first encrypted
insert only after authoritative absence checks; accept root, recipients and
data atomically. Standalone retries retain the original logical values and row
identity, while explicit mergeable transactions refuse implicit initialization.
Hidden or inaccessible existing spaces never become a reason to mint new keys.

Resolve managed administration table additions against the full published
schemas, not partial migration witnesses. Adding another encrypted table
preserves existing managed identities and historical ciphertext. Explicit
attempts to recreate an existing table fail before catalogue publication.

For local qualification, preserve canonical temporary-directory environment
variables through Turbo's strict test boundary. Keep invalid-authority and
signed-token checks portable across supported Node versions and host loopback
interfaces without relaxing production URL policy or signature verification.
