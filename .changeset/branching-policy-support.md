---
"jazz-tools": patch
---

Fix the server spending CPU exponential in the policy depth when a read policy has several `exists` arms that lead back to the same tables. Each row read walked the whole tree of arms, about 11,000 policy checks per row for a policy with three arms on two tables. Each table and depth is now followed once, so the work per row grows with the rows its read rests on, not with the number of arms to the power of the depth.
