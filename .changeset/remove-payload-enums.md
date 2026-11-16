---
"jazz-tools": minor
---

Removed enum-with-payload columns, which no store could hold yet ([#5758](https://github.com/garden-co/jazz/issues/5758)). Enums are lists of case names (`{ "enum": ["a", "b"] }`); a `jazz.app.json` whose current tables declare cases with fields is refused, naming the column. A schema version from alpha.64 or earlier that declared one still reads in a store's history and in an app's `history` and migrations, so such stores open and such apps can migrate away from it.
