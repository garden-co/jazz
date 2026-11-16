---
"jazz-tools": patch
---

BREAKING CHANGE: Deployments refuse JSON column schemas that use a JSON Schema feature the browser can't check yet. These are `$dynamicRef`/`$dynamicAnchor`, draft 6/7 `contentMediaType`, the `idn-email`, `idn-hostname`, `iri` and `iri-reference` formats in drafts 4–7, and a few regex constructs in `pattern`/`patternProperties`: possessive quantifiers, `(?i)` anywhere but at the start of a pattern, the `x` flag, and Unicode class names spelled differently from JS (such as `\p{greek}`). Browser clients already refused such schemas; now every runtime treats them the same. The error names the column, the feature and where it sits in the schema.

The check runs on every deploy's target schema, so an app whose deployed schema already uses one of these features can't redeploy at all (not even a permissions-only change) until it replaces the feature. This only affects apps without browser clients, since browsers could never open such a schema. Stores whose schema already uses one still open and sync.
