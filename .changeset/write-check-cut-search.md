---
"jazz-tools": patch
---

Permission checks on writes now look up the rows their policies refer to through indexes instead of scanning the whole table. Before, each check read every row of each table its policy refers to, other apps' and tenants' rows included, so a write cost server CPU in proportion to everything stored. In BigLabel with 15,897 labels, one edit took 575 ms of server CPU.
