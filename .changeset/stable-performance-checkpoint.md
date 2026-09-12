---
"jazz-tools": patch
---

Reduce cold-load and large-transaction overhead by avoiding repeated ancestry scans, record conversions, and unused query payloads. Preserve complete subscription results when recursive evaluation or data hydration resumes across runtime turns.
