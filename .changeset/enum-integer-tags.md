---
"jazz-tools": patch
---

Store enum columns, and arrays of them, as integer tags that the authority
assigns per column in the order variants were added. Reads, writes and
filters still use variant names. Sorting by an enum column now follows the
order its variants were added instead of alphabetical order. A variant the
server has not seen yet sorts last on the client that wrote it, until the
server's tag arrives.
