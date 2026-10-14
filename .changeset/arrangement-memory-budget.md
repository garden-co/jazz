---
"jazz-tools": patch
---

The Jazz server now bounds the memory its query indexes over stored tables keep, from its container memory limit, and lowers that bound when its resident memory passes 80% of the limit: queries then read more rows from storage instead of holding them, and the bound grows back once memory frees up.
