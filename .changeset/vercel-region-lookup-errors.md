---
"jazz-tools": patch
---

`vercel()` no longer reads a failed lookup of its core as a missing core. Only a "not found" answer from Vercel now makes it create a core; any other failure (a 503, a network error) is retried until the wake gives up, and then reported. Before, one failed lookup of an existing core with a different `region` configured asked Vercel for a Drive in that region, before the region check could refuse it. An existing Drive's region is now also checked before `vercel()` asks for one.
