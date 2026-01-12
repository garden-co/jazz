---
"cojson": patch
"jazz-tools": patch
---

Added `deleteCoValues` function to permanently delete CoValues and their nested references.

- CoValues are marked with a tombstone, making them inaccessible to all users
- Supports deleting nested CoValues via resolve queries
- Requires admin permissions on the CoValue's group
- Introduces new `deleted` loading state for deleted CoValues
- Groups and Accounts are skipped during deletion

See documentation: https://jazz.tools/docs/core-concepts/covalues/deleting
