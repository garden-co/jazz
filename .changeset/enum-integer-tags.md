---
"jazz-tools": patch
---

Store enum columns, and arrays of them, as small integers that each store
assigns locally. Reads, writes, filters and sync still use variant names.
Sorting by an enum column now follows the schema's declaration order instead
of alphabetical order, and adding or reordering variants makes a new schema
version.

Schema version ids now count enum variants, so every schema with an enum
column gets a new id. Peers on earlier builds name those schemas by their
old ids: in a mixed fleet using enums, clients and servers stop syncing
(writes are refused as made under an undeployed schema version) until
everyone upgrades.
