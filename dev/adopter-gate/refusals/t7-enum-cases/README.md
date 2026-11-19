t7-enum-cases: an app with a payload enum (`s.enum({ bug: {...}, feature: {...} })`).
Gate expectation on .65: `npx jazz-tools upgrade` REFUSES with a clear error naming `tickets.kind` (payload enums are removed in .65), and writes no partial files.
Baseline on .64: validate --check passes; smoke only covers a row without a kind (writes with a kind fail, jazz#5621).
