---
"jazz-tools": patch
---

A live query in a browser tab (or any foreground over a worker or native relay) no longer hangs when its relay refuses the server, for example after the server's store was recreated (`authority_changed`) or when the relay cannot mirror the server's catalogue. The relay now ends the tab's live queries with the refusal and its error code, whatever their tier, including queries opened after the refusal; once the relay reconnects to a server it can follow, those queries settle again. A `tier: "remote"` one-shot read on a refused link now fails with the refusal at once instead of waiting out its coverage budget.
