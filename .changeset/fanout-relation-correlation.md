---
"jazz-tools": patch
---

Live queries with relations (includes) re-run far less often. A write to a table that a subscription reads only through optional relations now re-runs it only when the written row is in its result at that level, or the row's old or new link names a parent the result holds. On a 100-member chat server, a read-mark write re-runs the 5 chat lists that show the message instead of all 100. Self-relations (`messages.where({ chatId }).include({ replyToMessage })`) keep their `chatId` key, so a message in one chat no longer re-runs every open thread. Shapes the filter cannot decide (a read policy that reads the table, a required relation, an offset window, a link that is not a reference) still re-run in full.

Fixed: a relation whose table's read policy reads another table (for example messages readable to chat members) now refreshes when that table changes, such as when a member joins.
