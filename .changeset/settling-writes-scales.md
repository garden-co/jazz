---
"jazz-tools": patch
---

Settling many pending writes no longer slows down with the number of writes still pending. Before, every write the server settled replayed every other pending write, so a backend inserting 20,000 rows took minutes to settle them, and a read issued right after the inserts failed with "Timed out waiting for query coverage". A settled write now replays only the pending writes that share its rows, so settling 5,000 inserts takes about 2 s instead of 160 s, and the inserts themselves run several times faster. The speedup applies to writes on different rows: writes that all touch one shared row (a counter every write bumps, say) still replay together. A pending write through a branch view now also shows the base branch's new version as soon as it arrives.
