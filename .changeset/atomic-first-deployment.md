---
"jazz-tools": patch
---

Initialize a server database and publish its first schema deployment in one transaction on the real store. A rejected first deployment leaves existing tables unchanged and can be retried with a corrected schema; validation no longer opens a scratch SQLite database.
