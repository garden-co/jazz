---
"jazz-tools": patch
---

Generate encryption-space membership tables from encrypted declarations, so
`fromEncryptionSpace()` no longer requires spreading
`encryptionSpacePolicySchema(scope)` into the application schema. Scope discovery
is shared by application compilation and migration generation. Membership
maintenance remains enabled by the selected permissions or the legacy explicit
schema declaration; ordinary explicit-permission apps retain their first-write
behaviour. Existing deployed schemas without the membership tables require a
schema migration before using the generated schema, and existing shared spaces
need their membership records populated before adopting membership-derived data
permissions.
