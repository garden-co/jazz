---
"jazz-tools": patch
---

Fix the server spending CPU exponential in the policy depth to sync a table whose read policy consults its own table, such as "members of a workspace read its members". In BandBook, the fourth member to join a workspace took about a minute of server CPU; it now takes milliseconds.
