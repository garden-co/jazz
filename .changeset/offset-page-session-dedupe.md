---
"jazz-tools": patch
---

Paging with `offset` no longer re-downloads every row above the page. When a new subscription opens, the server leaves out rows the client already keeps current through another of its live subscriptions (such as the previous page, whose rows the next page's read covers), so each next page ships only its own rows.
