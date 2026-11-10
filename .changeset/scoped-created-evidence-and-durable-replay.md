---
"jazz-tools": patch
---

Keep browser SQLite storage bound to its exact physical-root name, owner and
replica identity through open, close, reset and reopen. Refuse an incompatible
or incomplete root without silently replacing its file or identity, and release
its owner before a successful reset permits immediate reopen.

Preserve the current SQLite pending-read proof, first validation and terminal
transaction fate. This port adds no permanent post-settlement proof, storage
epoch, Groove migration or synthetic admission receipt. Historical images remain
refusal evidence rather than current stores.

Use the separately owned, authenticated same-transaction created-witness core
for opted-in INSERT policies; ordinary reads and update/delete authority remain
unchanged. Browser ownership does not establish accepted E2EE membership or
permit new offline authority.
