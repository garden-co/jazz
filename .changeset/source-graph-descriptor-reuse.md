---
"jazz-tools": patch
---

Inserts, queries and subscription refreshes no longer rebuild a table's physical schema and record descriptor each time they build a source graph, which makes seeding and writing to large stores faster. Results are unchanged.
