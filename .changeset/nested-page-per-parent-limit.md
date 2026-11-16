---
"jazz-tools": patch
---

Fix a 2.0.0-alpha.62 slowdown in nested includes with a limit or offset, such as a room list with each room's newest message. Each parent's page is now read from the relation's index and stops at the page's end, instead of reading every child of every parent and keeping the first few. BandChat's room list over 100 rooms opens in about 4 ms again, down from about 600 ms.
