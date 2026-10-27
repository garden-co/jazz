---
"jazz-tools": patch
---

The server now logs slow queries. Every 10 seconds it writes one warning per query shape that took 50 ms or more, with how often it ran, how long it held the database, how many rows it scanned, SQLite's query plan, and which index would likely make it fast. Set `JAZZ_SLOW_QUERY_MS` to change the threshold, or to `0` to turn the log off.
