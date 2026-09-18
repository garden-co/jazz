---
"jazz-tools": patch
---

Declare encrypted table columns through the public schema builder and compose
managed lifecycle schemas and policies automatically. Ordinary synchronous
mutation handles own encryption preparation, atomic new-scope recipients and
authoritative acceptance; ordinary reads decrypt selected logical values.
Plaintext operations remain independent of key readiness. Unsupported encrypted
query/subscription and migration paths fail closed until their owning layers.
