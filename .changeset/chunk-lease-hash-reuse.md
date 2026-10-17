---
"jazz-tools": patch
---

Speed up reading large values (files, long text and JSON) by no longer re-hashing chunk bytes that the chunk provider has already verified.
