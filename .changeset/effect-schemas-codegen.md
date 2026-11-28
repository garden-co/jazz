---
"jazz-tools": minor
---

`npx jazz-tools generate --effect-schema` writes `jazz.effect.generated.ts`: Effect v4 Schemas for each table's row and insert, with the names and exact types of `jazz.generated.ts`. Once the file is committed, `generate` keeps it current and `generate --check` checks it.
