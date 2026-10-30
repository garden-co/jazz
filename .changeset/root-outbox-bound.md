---
"jazz-tools": patch
---

A root server no longer keeps a copy of every client write in its upload queue for the life of the process. Queued uploads and downstream fates also drain in linear time after a long offline period.
