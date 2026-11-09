---
"jazz-tools": patch
---

Resolve reserved table-identity policy values from the authority-confirmed stored
names for the governing schema. Caller claims cannot supply or override these
identities. Queries refuse missing or unconfirmed metadata, including null checks;
a renamed schema view retains its governing table identity.
