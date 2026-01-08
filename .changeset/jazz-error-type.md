---
"jazz-tools": patch
---

Added `getJazzErrorType` helper function to identify the type of Jazz error from an Error object thrown by suspense hooks. This enables error boundaries to display appropriate UI based on whether the error is "unauthorized", "unavailable", or "unknown".

