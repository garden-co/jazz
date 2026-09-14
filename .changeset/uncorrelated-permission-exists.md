---
"jazz-tools": patch
---

Support permission policies with `EXISTS` checks that do not reference the outer row, such as checking whether the current user has an admin grant. Empty grant sets deny access, multiple matching grants do not duplicate results, and removing the last grant revokes existing subscriptions.

[PR #2880](https://github.com/garden-co/jazz/pull/2880).
