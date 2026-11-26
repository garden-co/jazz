---
"jazz-tools": patch
---

An app that declares encryption can now deploy to the Durable Object host. Its deploy reads the whole deploy body, keeps each schema version's encryption declarations in the same `e2ee_declarations` host table as the native server (in the same transaction as the catalogue change), and installs the account registry's founders view over the object's own SQL, so every account rule applies as on the native server. Apps without encryption deploy as before.
