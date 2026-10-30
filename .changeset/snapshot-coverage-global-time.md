---
"jazz-tools": patch
---

Transactional reads and commit validation decide whether a row version is visible from its transaction's global time and fate alone, instead of decoding the whole stored transaction for each past edit. Point reads and commits on frequently edited rows are faster. Results are unchanged.
