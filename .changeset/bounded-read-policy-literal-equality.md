---
"jazz-tools": minor
---

Allow ordinary SELECT policies to compare long text or byte literals by verified
content within a bounded root-query operation. Preserve the reader's governing
policy, snapshot and indexed dependencies; missing content or exceeded limits
refuse the whole read, including negative predicates and paginated queries.
