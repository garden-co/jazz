---
"jazz-tools": minor
---

Append variants to an encrypted enum as part of a migration that creates a new schema version. If appending variants is your only change, `jazz-tools migrations create` and `jazz-tools generate` refuse it. Add a change that creates a new version, such as adding an index, then run `jazz-tools migrations create` again.

Jazz keeps the original sealed type so existing values remain readable. When you use `jazz-tools print-ts` to turn an app file into TypeScript, the printer writes all current variants into `schema.ts` and preserves the original sealed type in an accompanying `.encryption.json` file.
