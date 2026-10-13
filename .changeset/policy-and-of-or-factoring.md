---
"jazz-tools": patch
---

Compile policies that are an AND of ORs without expanding every combination: a policy like `(a OR b) AND (c OR d) AND …` is now checked as one union per OR instead of one branch per combination, so compile time grows with the number of ORs rather than doubling with each one.
