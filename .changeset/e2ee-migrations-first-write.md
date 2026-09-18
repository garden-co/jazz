---
"jazz-tools": patch
---

Support explicit encrypted schema migration and stable catalogue identities
through renames. Initialize a legacy plaintext scope on its first encrypted
insert only after authoritative absence checks; accept root, recipients and
data atomically. Standalone retries retain the original logical values and row
identity, while explicit mergeable transactions refuse implicit initialization.
Hidden or inaccessible existing spaces never become a reason to mint new keys.
