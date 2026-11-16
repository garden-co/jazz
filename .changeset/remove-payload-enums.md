---
"jazz-tools": minor
---

Removed enum-with-payload columns, which no store could hold yet ([#5758](https://github.com/garden-co/jazz/issues/5758)). Enums are lists of case names (`{ "enum": ["a", "b"] }`); a `jazz.app.json` that declares cases with fields is now refused by the schema.
