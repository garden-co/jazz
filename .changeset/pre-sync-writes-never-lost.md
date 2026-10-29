---
"jazz-tools": patch
---

A write made through a schema view of a version the server has not admitted yet is no longer lost: it uploads under that version once the server admits it, instead of being refused and rolled back. A store that wrote before it became a replica now keeps those rows and refuses its upstream with an explicit error, rather than losing them on its first connect.
