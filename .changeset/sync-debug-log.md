---
"jazz-tools": patch
---

`logLevel: "debug"` now logs sync traffic: each message between a tab, its worker and the server (its kind, the read it is about, what it carries and its size), and each subscription delivery with whether and how it settled. Before, debug and trace logging printed nothing about sync.
