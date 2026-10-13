---
"jazz-tools": patch
"jazz-wasm": patch
---

The browser WASM is about 12% smaller (6.3 MB instead of 7.3 MB gzipped): JSON column schemas are now checked by a small JavaScript validator instead of a Rust one compiled into the WASM. A few rarely used JSON Schema features (`$dynamicRef`, draft-07 `contentMediaType`, and the `idn-email`, `idn-hostname`, `iri` and `iri-reference` formats) are reported as not supported in the browser yet, as are a few regular expression constructs the browser cannot match the same way (inline flags other than a leading `(?i)`, the `x` flag, possessive quantifiers, and a counted repetition directly after another quantifier). Code that loads `jazz-wasm` directly instead of through `jazz-tools` must call `setJsonSchemaValidator` before opening a database whose JSON columns declare a schema.
