---
"jazz-tools": patch
---

`clearOnSignOut` now also wipes a user's local data when the page never saw them sign out: a sign-out on your server, or another user signing in, followed by a reload. Before, the replica stayed in the browser's storage. On the next page load, once your backend names someone else or no one, Jazz wipes it. The user who is still signed in keeps theirs.
