---
"jazz-tools": patch
---

Re-apply stored Rejected batch settlements on runtime startup so that a crash between persisting a rejection and deleting its visible row no longer causes the lingering row to flash into queries on reload before being retracted.
