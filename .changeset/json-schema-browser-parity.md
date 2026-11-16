---
"jazz-tools": patch
---

**`jazz-tools validate` and deployments refuse JSON column schemas that use a JSON Schema feature the browser can't check yet.** These are `$dynamicRef`/`$dynamicAnchor`, draft 6/7 `contentMediaType`, the `idn-email`, `idn-hostname`, `iri` and `iri-reference` formats in drafts 4–7, and a few regex constructs in `pattern`/`patternProperties` (possessive quantifiers, `(?i)` anywhere but at the start of a pattern, the `x` flag, Unicode class names spelled differently from JS such as `\p{greek}`). Such schemas were already refused by browser clients; now every runtime treats them the same, and the error names the column, the feature and where it sits in the schema. Stores whose schema already uses one still open.
