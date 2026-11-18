---
"jazz-tools": patch
---

Live subscriptions no longer share a result across claim sources. A query that follows an identity's claims registry and the same query carrying fixed request claims for that identity could share one live result while their claims matched, so a later registry change left one of them wrong (a revoked row kept, or a fixed-claims row dropped). A result recomputed after a claims change is also now found by what it currently reads, so a new subscriber can't join a result that no longer matches its own claims.
