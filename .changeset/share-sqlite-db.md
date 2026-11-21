---
"jazz-tools": patch
---

Allow Jazz to share SQLite databases with unrelated application tables. Discover owned tables from the schema catalogue, reject object-name collisions, and continue refusing unsupported Jazz and legacy Groove storage formats without changing them. Initialize Jazz tables, indexes, triggers and the format stamp transactionally, so a rejected open leaves no new Jazz objects behind. Storage and wire encodings are unchanged.
