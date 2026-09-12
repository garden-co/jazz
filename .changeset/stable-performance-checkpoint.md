---
"jazz-tools": patch
---

Reduce cold-load and large-transaction overhead by avoiding repeated ancestry scans, record conversions, and unused query payloads. Preserve complete subscription results when recursive evaluation or data hydration resumes across runtime turns.

Check subscription completeness against its typed schema view, including views registered on an initially empty owner.

Keep provisional enum registries coherent when adopting the authority catalogue, preserving offline enum values and preventing provenance failures on subsequent reopen.
