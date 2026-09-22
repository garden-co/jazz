---
"jazz-tools": patch
---

Align runtime and TypeScript relation names using a compact bounded naming convention, removing pluralize-esm without introducing code generation. Explicit Ids/\_ids references support common irregular, invariant, and suffix-based plurals; unsuffixed names remain unchanged. This changes API aliases for names outside the convention: review includes, reverse relations, and permission hops when upgrading. Stored columns, references, schema hashes, and data are unchanged.
