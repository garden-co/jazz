---
"jazz-wasm": patch
"jazz-tools": patch
---

Order WASM initialization absence checks and sealing after the same transaction owner's queued begin and staging. Preserve the original exclusive snapshot and staging errors instead of racing initialization against an unexecuted begin.

Keep raw Memory initialization Promises progressing through ready predecessors without requiring a tick scheduler. Cold work retains its existing wake route, Browser keeps its scheduler, and initialization progress does not imply authority acceptance or change seal ownership.
