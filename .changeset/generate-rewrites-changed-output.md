---
"jazz-tools": patch
---

`jazz-tools generate` (without `--check`) now also rewrites bindings that this version of Jazz generates differently from the committed ones, so a generator fix reaches an app whose `jazz.app.json` didn't change; you no longer need to delete `jazz.generated.ts` first. `jazz.generated.ts` records a digest of the generator's output on its second line (`// Bindings format 1. Output <hex>. …`), so a module a formatter reformatted is still kept as it is. `generate --check` is unchanged: it checks the bindings' source and format, so upgrading Jazz doesn't fail CI by itself. Other languages' bindings are rewritten when their text differs.
