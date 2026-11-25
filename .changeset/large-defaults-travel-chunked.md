---
"jazz-tools": patch
---

A large schema default (a `string` or `bytes` default above the inline threshold) no longer travels whole inside every commit that creates a row. The client sends the default's chunks ahead of the commit, as it does for a large value the app writes, and the commit names it by reference. Upserts that create a row now push the chunks their filled-in cells need.
