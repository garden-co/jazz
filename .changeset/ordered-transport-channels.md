---
"jazz-tools": patch
---

Multiplex bounded, prioritized channels over each connection so large uploads and deliveries do not block independent small queries. Compression now retains history per ordered stream, while explicit message dependencies preserve catalogue, session and transaction ordering across streams.

Run immutable chunk requests and responses independently of suspended database operations, fixing large-value reads that could wait indefinitely (#3164). Retained partial messages have enforced deadlines and bounded memory; a stalled connection cannot prevent other connections from progressing.

Preserve transaction settlement when an older query delivery is replaced while waiting for missing row bodies. Discarded deliveries retain transaction identity without publishing obsolete rows, and later partial deliveries cannot regress an already-settled transaction.

This requires the wire v3 client/server update together. It does not change storage encoding.
