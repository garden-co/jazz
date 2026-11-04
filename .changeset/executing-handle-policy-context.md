---
"jazz-tools": patch
---

Keep read and write permissions tied to the executing handle. Registered schema views retain their table and column names while following current owner policies; authenticated sessions retain their selected permission bundle through deferred reads, subscriptions and transactions.
