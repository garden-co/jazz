---
"jazz-tools": patch
---

With `clearOnSignOut` and the local-first client (`client`), Jazz now also wipes a backend user's local replica when the page never saw them sign out. That covers a sign-out on your server, or another user signing in, followed by a reload. Before, the replica stayed in the browser's storage. On the next page load, once your backend names someone else or no one (asked twice, a couple of seconds apart), Jazz wipes it, unless another tab still has it open. This covers replicas opened from this version on.
