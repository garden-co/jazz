---
"jazz-tools": patch
---

Schedule React Native `batchedTick` callbacks on a macrotask so repeated native tick requests do not starve timers, React rendering, or runner timeouts.
