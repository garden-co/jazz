---
"jazz-tools": patch
---

Remove the pluralize-esm dependency. The intermediate bounded automatic naming convention is superseded in this release by explicit table-local relationships: declare forward navigation with `s.rel` and reverse navigation with `s.reverse`; no automatic reverse names or pluralization remain. When changing navigation names, update includes, reverse declarations, relation filters, and permission hops together. Preserve stored column definitions and every column-to-target mapping when converting existing references; an equivalent conversion preserves schema identity and stored data. See the explicit-relationships migration guide in this changelog.
