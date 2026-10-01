---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
---

Expose opt-in coherent row-settlement metadata and portable accepted catalogue
identities for authenticated encryption histories. Exclusive wait handles can
request global acceptance without changing ordinary row encoding or conflating
local durability with authority. Add crypto-neutral preparation scopes and
accepted-only observation support; no encrypted lifecycle or table operations
are enabled by this layer.

Prepared transaction operations enforce the same schema restrictions as ordinary
transactions, preventing writes and typed reads through a different schema.

Catalogue coverage uses the current global tier. The legacy `edge` write-wait
alias resolves to global acceptance; omitted exclusive waits retain their
existing local-without-upstream behaviour.
